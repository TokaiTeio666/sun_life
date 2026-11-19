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
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 秒杀下单（Kafka 异步下单版）。
 *
 * <p>流程：Lua 原子完成「校验活动时间 + 校验库存 + 一人一单 + 扣减库存」→ 异步发送 Kafka 消息 →
 * 消费者批量落库后提交位点。发送失败时执行补偿 Lua 回滚库存与去重标记。</p>
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

    @Resource
    private TransactionTemplate transactionTemplate;

    /** 订单超时时长（分钟），超时扫描任务使用 */
    @Value("${hmdp.order.timeout-minutes:30}")
    private long timeoutMinutes;

    /** 超时关单每批处理条数，避免一次性加载全部数据、长事务长时间持有库存行锁 */
    private static final int CLOSE_BATCH_SIZE = 500;
    /** 单次任务最多处理的批数，防止积压过大时任务执行过久 */
    private static final int CLOSE_MAX_BATCHES = 100;

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

        //1. Lua 原子完成活动时间校验、库存校验、一人一单与扣减库存
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );
        int r = result == null ? 1 : result.intValue();
        if (r != 0) {
            return Result.fail(switch (r) {
                case 2 -> "不能重复下单";
                case 3 -> "秒杀尚未开始";
                case 4 -> "秒杀已经结束";
                default -> "库存不足";
            });
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
     * 创建单个订单（批量失败时的逐条兜底路径）。
     *
     * <p>Kafka 至少一次投递：同一条消息重复消费时订单 id 相同，INSERT 触发主键冲突抛
     * {@link DuplicateKeyException}，事务回滚（库存扣减一并撤销），调用方按“已处理”确认即可。
     * 因此不再需要先 SELECT 判重，也不再需要消费端分布式锁（userId 作为消息 key 已保证同一用户串行，
     * 一人一单由 Lua 原子保证）。</p>
     *
     * @return true 创建成功；false DB 库存不足（与 Redis 不一致，需对账）
     */
    @Override
    @Transactional
    public boolean createVoucherOrder(VoucherOrder voucherOrder) {
        //1. 条件扣减 DB 库存，防止超卖
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0)
                .update();
        if (!success) {
            log.error("DB 库存不足，跳过下单 voucherId={} orderId={}", voucherOrder.getVoucherId(), voucherOrder.getId());
            return false;
        }
        //2. 创建订单（主键冲突即重复消费，异常抛出后整体回滚）
        save(voucherOrder);
        return true;
    }

    /**
     * 批量创建订单（Kafka 批量消费快速路径）。
     *
     * <p>同一批消息按 voucherId 聚合，每张券只执行一次 {@code stock = stock - n}，
     * 把热点库存行的加锁次数从 N 次降到 1 次；订单用 saveBatch 批量插入
     * （JDBC URL 需开启 rewriteBatchedStatements）。任一步失败整批回滚，由调用方降级为逐条处理。</p>
     *
     * @return 本批实际新建的订单
     */
    @Override
    @Transactional
    public List<VoucherOrder> createVoucherOrdersBatch(List<VoucherOrder> orders) {
        //0. 批内去重 + 过滤已落库的订单（重复投递）
        Map<Long, VoucherOrder> byId = new LinkedHashMap<>();
        for (VoucherOrder o : orders) {
            byId.putIfAbsent(o.getId(), o);
        }
        if (byId.isEmpty()) {
            return Collections.emptyList();
        }
        query().select("id").in("id", byId.keySet()).list()
                .forEach(existing -> byId.remove(existing.getId()));
        List<VoucherOrder> toCreate = new ArrayList<>(byId.values());
        if (toCreate.isEmpty()) {
            return toCreate;
        }
        //1. 按券聚合扣减库存：一张券一条 UPDATE
        Map<Long, Long> countByVoucher = toCreate.stream()
                .collect(Collectors.groupingBy(VoucherOrder::getVoucherId, Collectors.counting()));
        for (Map.Entry<Long, Long> e : countByVoucher.entrySet()) {
            boolean ok = seckillVoucherService.update()
                    .setSql("stock = stock - " + e.getValue())
                    .eq("voucher_id", e.getKey())
                    .ge("stock", e.getValue())
                    .update();
            if (!ok) {
                throw new IllegalStateException("DB 库存不足以批量扣减 voucherId=" + e.getKey() + " n=" + e.getValue());
            }
        }
        //2. 批量插入订单
        saveBatch(toCreate);
        return toCreate;
    }

    /** 订单创建成功后写入状态短缓存，前端轮询直接命中 Redis */
    @Override
    public void cacheOrderStatus(Collection<VoucherOrder> orders) {
        if (orders.isEmpty()) {
            return;
        }
        stringRedisTemplate.executePipelined(new SessionCallback<Object>() {
            @Override
            @SuppressWarnings({"unchecked", "rawtypes"})
            public Object execute(RedisOperations operations) {
                for (VoucherOrder o : orders) {
                    operations.opsForValue().set(RedisConstants.ORDER_STATUS_KEY + o.getId(),
                            String.valueOf(o.getStatus()),
                            RedisConstants.ORDER_STATUS_TTL_MINUTES, TimeUnit.MINUTES);
                }
                return null;
            }
        });
    }

    @Override
    public Integer queryOrderStatus(Long orderId) {
        String cached = stringRedisTemplate.opsForValue().get(RedisConstants.ORDER_STATUS_KEY + orderId);
        if (cached != null) {
            return Integer.valueOf(cached);
        }
        VoucherOrder order = query().select("id", "status").eq("id", orderId).one();
        // 0 = 订单尚未创建（消息排队中），不缓存，避免把“处理中”状态缓存住
        return order == null ? 0 : order.getStatus();
    }

    /**
     * 超时关单：分批扫描（走 idx_status_ctime 索引），每批一个独立短事务，
     * 库存回补按券聚合成一条 UPDATE；Redis 回补在事务提交后用 pipeline 一次完成。
     */
    @Override
    public int closeTimeoutOrders() {
        LocalDateTime deadline = LocalDateTime.now().minusMinutes(timeoutMinutes);
        int total = 0;
        for (int batch = 0; batch < CLOSE_MAX_BATCHES; batch++) {
            List<VoucherOrder> candidates = query()
                    .select("id", "user_id", "voucher_id")
                    .eq("status", 1)
                    .lt("create_time", deadline)
                    .orderByAsc("create_time")
                    .last("LIMIT " + CLOSE_BATCH_SIZE)
                    .list();
            if (candidates.isEmpty()) {
                break;
            }
            List<VoucherOrder> closed = transactionTemplate.execute(status -> closeBatch(candidates));
            if (closed != null && !closed.isEmpty()) {
                restoreRedisStock(closed);
                total += closed.size();
            }
            if (candidates.size() < CLOSE_BATCH_SIZE) {
                break;
            }
        }
        return total;
    }

    private List<VoucherOrder> closeBatch(List<VoucherOrder> candidates) {
        List<VoucherOrder> closed = new ArrayList<>();
        for (VoucherOrder order : candidates) {
            // 条件更新：只有仍未支付才允许关单，避免与支付竞态
            boolean success = update()
                    .setSql("status = 4")
                    .eq("id", order.getId())
                    .eq("status", 1)
                    .update();
            if (success) {
                closed.add(order);
            }
        }
        Map<Long, Long> countByVoucher = closed.stream()
                .collect(Collectors.groupingBy(VoucherOrder::getVoucherId, Collectors.counting()));
        countByVoucher.forEach((voucherId, n) -> seckillVoucherService.update()
                .setSql("stock = stock + " + n)
                .eq("voucher_id", voucherId)
                .update());
        return closed;
    }

    private void restoreRedisStock(List<VoucherOrder> closed) {
        stringRedisTemplate.executePipelined(new SessionCallback<Object>() {
            @Override
            @SuppressWarnings({"unchecked", "rawtypes"})
            public Object execute(RedisOperations operations) {
                for (VoucherOrder order : closed) {
                    operations.opsForValue().increment(RedisConstants.SECKILL_STOCK_KEY + order.getVoucherId());
                    operations.opsForSet().remove(RedisConstants.SECKILL_ORDER_KEY + order.getVoucherId(),
                            order.getUserId().toString());
                    operations.delete(RedisConstants.ORDER_STATUS_KEY + order.getId());
                }
                return null;
            }
        });
    }

    @Override
    public Result payOrder(Long orderId) {
        boolean success = update()
                .setSql("status = 2")
                .eq("id", orderId)
                .eq("status", 1)
                .update();
        if (success) {
            stringRedisTemplate.delete(RedisConstants.ORDER_STATUS_KEY + orderId);
        }
        return success ? Result.ok() : Result.fail("订单不存在或状态不允许支付");
    }
}
