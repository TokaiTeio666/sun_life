package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mq.KafkaConstants;
import com.hmdp.mq.SeckillOrderMessage;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

/**
 * 秒杀下单（Kafka 异步下单版）。
 *
 * <p>流程：Lua 原子完成「校验库存 + 一人一单 + 扣减库存」→ 异步发送 Kafka 消息 →
 * 消费者手动提交位点并落库。发送失败时执行补偿 Lua 回滚库存与去重标记。</p>
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
    private KafkaTemplate<String, String> kafkaTemplate;

    /** 订单超时时长（分钟），超时扫描任务使用 */
    @Value("${hmdp.order.timeout-minutes:30}")
    private long timeoutMinutes;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    private static final DefaultRedisScript<Long> SECKILL_ROLLBACK_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);

        SECKILL_ROLLBACK_SCRIPT = new DefaultRedisScript<>();
        SECKILL_ROLLBACK_SCRIPT.setLocation(new ClassPathResource("seckill_rollback.lua"));
        SECKILL_ROLLBACK_SCRIPT.setResultType(Long.class);
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");

        //1. Lua 原子完成库存校验、一人一单与扣减库存
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );
        int r = result == null ? 1 : result.intValue();
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        //2. 异步发送订单消息；发送失败由回调回滚 Lua 扣减
        SeckillOrderMessage message = new SeckillOrderMessage(orderId, userId, voucherId);
        sendSeckillOrder(voucherId, userId, message);

        //3. 立即返回订单 id，前端轮询 /voucher-order/status/{orderId}
        return Result.ok(orderId);
    }

    /**
     * 以 userId 作为消息 key，保证同一用户的消息落到同一分区、顺序消费。
     * 发送失败时回滚 Redis 库存与去重标记，避免「扣了库存却没有订单」。
     */
    private void sendSeckillOrder(Long voucherId, Long userId, SeckillOrderMessage message) {
        final String payload = JSONUtil.toJsonStr(message);
        try {
            kafkaTemplate.send(KafkaConstants.SECKILL_ORDER_TOPIC, userId.toString(), payload)
                    .whenComplete((sendResult, ex) -> {
                        if (ex != null) {
                            rollbackStock(voucherId, userId, message.getId(), ex);
                        } else {
                            log.debug("秒杀订单消息发送成功 orderId={} partition={} offset={}",
                                    message.getId(),
                                    sendResult.getRecordMetadata().partition(),
                                    sendResult.getRecordMetadata().offset());
                        }
                    });
        } catch (Exception e) {
            rollbackStock(voucherId, userId, message.getId(), e);
        }
    }

    private void rollbackStock(Long voucherId, Long userId, Long orderId, Throwable ex) {
        log.error("秒杀订单消息发送失败，回滚库存 voucherId={} userId={} orderId={}",
                voucherId, userId, orderId, ex);
        try {
            stringRedisTemplate.execute(
                    SECKILL_ROLLBACK_SCRIPT,
                    Collections.emptyList(),
                    voucherId.toString(), userId.toString()
            );
        } catch (Exception rollbackEx) {
            // 回滚失败只能告警，需人工对账
            log.error("库存回滚失败，需人工核对 voucherId={} userId={} orderId={}",
                    voucherId, userId, orderId, rollbackEx);
        }
    }

    /**
     * 创建订单（Kafka 消费者调用）。Kafka 至少一次投递，这里按订单 id 做幂等。
     */
    @Override
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //0. 幂等：订单已存在直接跳过
        if (getById(voucherOrder.getId()) != null) {
            log.warn("订单已存在，跳过重复消费 orderId={}", voucherOrder.getId());
            return;
        }
        //1. 条件扣减 DB 库存，防止超卖
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0)
                .update();
        if (!success) {
            log.error("库存不足，跳过下单 voucherId={}", voucherOrder.getVoucherId());
            return;
        }
        //2. 创建订单
        save(voucherOrder);
    }

    @Override
    @Transactional
    public int closeTimeoutOrders() {
        LocalDateTime deadline = LocalDateTime.now().minusMinutes(timeoutMinutes);
        List<VoucherOrder> orders = query()
                .eq("status", 1)
                .lt("create_time", deadline)
                .list();
        int closed = 0;
        for (VoucherOrder order : orders) {
            // 条件更新：只有仍未支付才允许关单，避免与支付竞态
            boolean success = update()
                    .setSql("status = 4")
                    .eq("id", order.getId())
                    .eq("status", 1)
                    .update();
            if (!success) {
                continue;
            }
            seckillVoucherService.update()
                    .setSql("stock = stock + 1")
                    .eq("voucher_id", order.getVoucherId())
                    .update();
            stringRedisTemplate.opsForValue().increment(RedisConstants.SECKILL_STOCK_KEY + order.getVoucherId());
            stringRedisTemplate.opsForSet().remove(
                    RedisConstants.SECKILL_ORDER_KEY + order.getVoucherId(),
                    order.getUserId().toString());
            closed++;
        }
        return closed;
    }

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
