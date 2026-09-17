package com.hmdp.mq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.utils.RedisConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collections;

/**
 * 秒杀订单事务消息监听器（RocketMQ 事务消息）
 *
 * 解决的问题：
 *   原来"Lua 扣库存"和"Java 发消息"是两步，不是原子的——Lua 成功后 JVM 崩溃或发送失败，
 *   用户库存被扣了但订单消息没发出去，且 Lua 的 sismember 挡住了重试，订单永久丢失。
 *
 * 事务消息机制（RocketMQ 的招牌能力）：
 *   1. 生产者先发送"半消息"（Half Message）到 Broker —— 暂存于 RMQ_SYS_TRANS_HALF_TOPIC，消费者不可见
 *   2. Broker 确认收到后，回调 executeLocalTransaction 执行"本地事务"（本项目 = Redis Lua：校验+扣库存+记录用户）
 *   3. 本地事务成功 → 返回 COMMIT：半消息转为普通消息，消费者可见并创建订单
 *   4. 本地事务失败 → 返回 ROLLBACK：半消息被丢弃
 *   5. 若生产者在这期间崩溃、没来得及回报 → Broker 定时回调 checkLocalTransaction 回查，
 *      根据业务真实状态（用户是否已在 Redis 下单集合中）决定提交还是回滚
 *
 * 这样"扣库存"与"发消息"被绑定成同一个原子决策，消除一致性缺口。
 */
@Slf4j
@Component
@RocketMQTransactionListener
public class SeckillOrderTransactionListener implements RocketMQLocalTransactionListener {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private ObjectMapper objectMapper;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    /**
     * 本地事务执行：运行 Lua 脚本（校验库存 + 防重复 + 扣库存 + 记录用户）
     *
     * @param msg 半消息（payload 为序列化后的订单 JSON，byte[]）
     * @param arg sendMessageInTransaction 时原样透传的参数对象（与消息体同一个订单对象，直接用，无需反序列化）
     */
    @Override
    public RocketMQLocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        VoucherOrder order = (VoucherOrder) arg;
        try {
            Long result = stringRedisTemplate.execute(
                    SECKILL_SCRIPT,
                    Collections.emptyList(),
                    order.getVoucherId().toString(), order.getUserId().toString(), order.getId().toString()
            );
            if (result == 0) {
                // 本地事务成功：库存已扣、用户已记录 → 提交半消息，让消费者创建订单
                log.info("【事务消息】本地事务成功（Lua=0），提交半消息，orderId={}", order.getId());
                return RocketMQLocalTransactionState.COMMIT;
            }
            // Lua 返回 1=库存不足 / 2=重复下单 → 回滚半消息，消息被丢弃
            log.info("【事务消息】本地事务失败（Lua={}），回滚半消息，orderId={}", result, order.getId());
            return RocketMQLocalTransactionState.ROLLBACK;
        } catch (Exception e) {
            // 本地事务执行异常（如 Redis 抖动）→ 返回 UNKNOWN，Broker 稍后会回查
            log.error("【事务消息】执行本地事务异常，返回UNKNOWN等待回查，orderId={}", order.getId(), e);
            return RocketMQLocalTransactionState.UNKNOWN;
        }
    }

    /**
     * 事务回查：生产者可能在"本地事务执行完、结果未上报"之间崩溃
     * Broker 会定时回调此方法（默认 60s 一次，最多 15 次），根据业务真实状态做最终裁定
     *
     * 判据：Redis 的 seckill:order:{voucherId} 集合中是否有该用户
     *   - 有 → Lua 确实执行成功了（但结果没回报）→ 提交，消费者创建订单
     *   - 无 → Lua 没执行或已回滚 → 回滚半消息
     *
     * 注意：此方法拿不到 arg（arg 只活在生产者内存里，可能已随崩溃丢失），
     * 只能从消息 payload（byte[] = 订单 JSON）反序列化出订单信息。
     */
    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(Message msg) {
        try {
            byte[] payload = (byte[]) msg.getPayload();
            VoucherOrder order = objectMapper.readValue(payload, VoucherOrder.class);
            Boolean ordered = stringRedisTemplate.opsForSet().isMember(
                    RedisConstants.SECKILL_ORDER_KEY + order.getVoucherId(),
                    order.getUserId().toString()
            );
            if (Boolean.TRUE.equals(ordered)) {
                log.info("【事务消息】回查：用户已下单，确认提交，orderId={}", order.getId());
                return RocketMQLocalTransactionState.COMMIT;
            }
            log.info("【事务消息】回查：用户未下单，确认回滚，orderId={}", order.getId());
            return RocketMQLocalTransactionState.ROLLBACK;
        } catch (Exception e) {
            log.error("【事务消息】回查异常，返回UNKNOWN", e);
            return RocketMQLocalTransactionState.UNKNOWN;
        }
    }
}
