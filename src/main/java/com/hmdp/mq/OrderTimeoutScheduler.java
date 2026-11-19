package com.hmdp.mq;

import com.hmdp.service.IVoucherOrderService;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 订单超时关单：Kafka 没有 RocketMQ 的延迟消息，改用定时任务扫描未支付订单。
 *
 * <p>多实例部署时每个实例都会触发调度，这里用 Redisson 锁保证同一时刻只有一个实例执行
 * （tryLock 不等待，拿不到直接跳过；看门狗自动续期，实例宕机后锁自动释放）。</p>
 */
@Slf4j
@Component
public class OrderTimeoutScheduler {

    private static final String JOB_LOCK_KEY = "lock:job:order-timeout";

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private RedissonClient redissonClient;

    @Scheduled(fixedDelayString = "${hmdp.order.timeout-scan-ms:60000}")
    public void closeTimeoutOrders() {
        RLock lock = redissonClient.getLock(JOB_LOCK_KEY);
        if (!lock.tryLock()) {
            log.debug("超时关单任务已由其他实例执行，本实例跳过");
            return;
        }
        try {
            int closed = voucherOrderService.closeTimeoutOrders();
            if (closed > 0) {
                log.info("超时关单完成，关闭 {} 笔未支付订单", closed);
            }
        } catch (Exception e) {
            log.error("超时关单任务执行失败", e);
        } finally {
            lock.unlock();
        }
    }
}
