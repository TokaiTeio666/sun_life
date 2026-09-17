package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.messaging.support.MessageBuilder;

import javax.annotation.Resource;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * 秒杀下单流程（RocketMQ 事务消息 + 异步下单版）：
 * 1. 用户请求秒杀接口 /voucher-order/seckill/{id}
 * 2. 发送"半消息"到 RocketMQ（topic: seckill-order，消费者不可见）
 * 3. SeckillOrderTransactionListener 回调执行 Lua（Redis 内原子完成：校验库存 → 防重复 → 扣库存 → 记录用户）
 * 4. Lua 成功 → 提交半消息 → SeckillOrderConsumer 异步创建订单落库；Lua 失败 → 回滚半消息
 * 5. 前端通过 /voucher-order/status/{orderId} 轮询订单是否创建成功
 *
 * 相比原实现的关键升级：
 * - 事务消息：把"扣库存(Lua)"和"发消息"绑定为同一个原子决策，消除"库存扣了但消息没发出去"的一致性缺口
 * - 可靠性：RocketMQ 消息落盘持久化，消费失败自动重试（默认16次），耗尽进死信队列
 * - 扩展：延迟消息（订单30分钟未支付自动关单，topic: seckill-order-timeout）
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    @Resource
    private RocketMQTemplate rocketMQTemplate;

    /** 秒杀订单消息 topic（需与 SeckillOrderConsumer 注解中的 topic 保持一致） */
    public static final String SECKILL_ORDER_TOPIC = "seckill-order";

    /** 订单超时检查消息 topic（延迟消息，需与 OrderTimeoutConsumer 注解中的 topic 保持一致） */
    public static final String ORDER_TIMEOUT_TOPIC = "seckill-order-timeout";

    /** 订单超时未支付自动关单的延迟等级（读取 application.yaml 的 hmdp.order.timeout-delay-level） */
    @Value("${hmdp.order.timeout-delay-level:16}")
    private int timeoutDelayLevel;

    @Override
    public Result seckillVoucher(Long voucherId) {
        //1. 获取用户
        Long userId = UserHolder.getUser().getId();
        //2. 生成全局唯一订单id（Redis 自增 + 时间戳拼接，保证多实例下不重复）
        long orderId = redisIdWorker.nextId("order");
        //3. 组装订单信息（既是消息体，也作为本地事务的参数透传给监听器）
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
        voucherOrder.setStatus(1); // 1：未支付
        //4. 发送事务消息（核心改动）：
        //   ① 先发"半消息"到 Broker（暂存，消费者不可见）
        //   ② Broker 确认后回调 SeckillOrderTransactionListener.executeLocalTransaction 执行 Lua
        //      （校验库存 + 防重复 + 扣库存 + 记录用户，全部在 Redis 内原子完成）
        //   ③ Lua 成功 → COMMIT 半消息 → 消费者创建订单；Lua 失败 → ROLLBACK 半消息被丢弃
        //   这样"扣库存"和"发消息"绑定为同一个原子决策，彻底消除"扣了库存但消息没发出去"的缺口
        TransactionSendResult sendResult = rocketMQTemplate.sendMessageInTransaction(
                SECKILL_ORDER_TOPIC,
                MessageBuilder.withPayload(voucherOrder).build(),
                voucherOrder
        );
        //5. 根据本地事务结果给用户即时反馈（sendMessageInTransaction 会同步等本地事务执行完）
        if (sendResult.getLocalTransactionState() == LocalTransactionState.ROLLBACK_MESSAGE) {
            // 回滚原因区分：Lua 返回 1=库存不足、2=重复下单，用 Redis 轻量查询反推具体原因
            String stock = stringRedisTemplate.opsForValue().get(RedisConstants.SECKILL_STOCK_KEY + voucherId);
            if (stock != null && Long.parseLong(stock) <= 0) {
                return Result.fail("库存不足");
            }
            return Result.fail("不能重复下单");
        }
        if (sendResult.getLocalTransactionState() == LocalTransactionState.UNKNOW) {
            // 本地事务执行异常（如 Redis 抖动），半消息待 Broker 回查后裁定，用户侧先提示重试
            log.error("秒杀事务状态未知，orderId={}", orderId);
            return Result.fail("系统繁忙，请稍后重试");
        }
        //6. 返回订单id，前端轮询 /voucher-order/status/{orderId} 确认异步下单结果
        return Result.ok(orderId);
    }

    /**
     * 创建订单（由 RocketMQ 消费者 SeckillOrderConsumer 异步调用）
     *
     * 两个关键点：
     * 1. Redisson 锁：防止同一用户重复下单（RocketMQ 可能重复投递消息，At Least Once 语义）
     * 2. 幂等判断：订单已存在直接跳过，保证重复消息不会产生重复订单
     *
     * 注意：这里抛出的任何异常都会被 RocketMQ 视为消费失败并自动重试（默认16次），
     * 因此"库存不足/重复下单"等业务上不该重试的情况直接 return（消费成功，不重试），
     * 而"数据库异常"等临时故障则抛出异常触发重试。
     */
    @Override
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //1. 加 Redisson 锁，防止同一用户并发重复下单
        RLock lock = redissonClient.getLock("lock:order:" + voucherOrder.getUserId());
        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.error("不允许重复下单, userId={}", voucherOrder.getUserId());
            return;
        }
        try {
            //2. 幂等处理：消息可能被重复投递，订单已存在则跳过
            if (getById(voucherOrder.getId()) != null) {
                log.info("订单已存在，跳过重复处理，orderId={}", voucherOrder.getId());
                return;
            }
            //3. 扣减库存（带 stock > 0 条件，防止超卖）
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", voucherOrder.getVoucherId())
                    .gt("stock", 0)
                    .update();
            if (!success) {
                log.error("库存不足！voucherId={}", voucherOrder.getVoucherId());
                return;
            }
            //4. 创建订单
            save(voucherOrder);
            //5. 发送延迟消息：timeoutDelayLevel 后检查订单是否已支付，未支付则自动关单
            //   syncSend(目的地, 消息体, 发送超时ms, 延迟等级)；等级16 = 30分钟
            //   学习点：延迟消息先在 Broker 的 SCHEDULE_TOPIC_XXXX 暂存，到时间后才投递到业务主题
            rocketMQTemplate.syncSend(ORDER_TIMEOUT_TOPIC,
                    MessageBuilder.withPayload(voucherOrder.getId()).build(), 3000, timeoutDelayLevel);
            log.info("订单创建成功，已发送超时检查延迟消息，orderId={}, delayLevel={}", voucherOrder.getId(), timeoutDelayLevel);
        } catch (Exception e) {
            log.error("创建订单异常，触发 RocketMQ 重试，orderId={}", voucherOrder.getId(), e);
            // 抛出异常：事务回滚 + RocketMQ 判定消费失败自动重试
            throw e;
        } finally {
            //释放锁
            lock.unlock();
        }
    }

    /**
     * 超时关单（由延迟消息消费者 OrderTimeoutConsumer 调用）
     *
     * 学习点：
     * 1. 条件更新 WHERE id=? AND status=1：只有"未支付"才允许关单，
     *    如果期间用户已支付（status=2），更新影响 0 行，天然避免"支付成功又被关单"的竞态；
     * 2. 关单后回补库存：DB（tb_seckill_voucher.stock）和 Redis（Lua 校验用的库存 + 用户集合）都要回补，
     *    否则用户永远无法再次购买该券；
     * 3. 注意：关单消息本身是延迟消息，可能因为时间误差与支付几乎同时发生，条件更新就是兜底。
     */
    @Override
    @Transactional
    public void closeTimeoutOrder(VoucherOrder order) {
        //1. 条件更新：仅当订单仍为"未支付(1)"时置为"已取消(4)"
        boolean success = update()
                .setSql("status = 4")
                .eq("id", order.getId())
                .eq("status", 1)
                .update();
        if (!success) {
            log.info("订单状态已变化，跳过关单，orderId={}", order.getId());
            return;
        }
        //2. 回补 DB 库存
        seckillVoucherService.update()
                .setSql("stock = stock + 1")
                .eq("voucher_id", order.getVoucherId())
                .update();
        //3. 回补 Redis 库存 + 移除用户下单记录（保证该用户可再次购买）
        stringRedisTemplate.opsForValue().increment(RedisConstants.SECKILL_STOCK_KEY + order.getVoucherId());
        stringRedisTemplate.opsForSet().remove(RedisConstants.SECKILL_ORDER_KEY + order.getVoucherId(),
                order.getUserId().toString());
        log.info("订单已超时关闭，orderId={}, voucherId={}", order.getId(), order.getVoucherId());
    }

    /**
     * 支付订单（学习/测试用）
     * 条件更新：仅当订单为"未支付(1)"时置为"已支付(2)"，防止重复支付
     */
    @Override
    public Result payOrder(Long orderId) {
        boolean success = update()
                .setSql("status = 2")
                .eq("id", orderId)
                .eq("status", 1)
                .update();
        return success ? Result.ok() : Result.fail("订单不存在或状态不允许支付");
    }
}
