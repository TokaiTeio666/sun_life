package com.hmdp.mq;

import com.hmdp.service.IVoucherOrderService;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 订单超时关单：Kafka 没有 RocketMQ 的延迟消息，改用本地定时任务扫描未支付订单。
 *
 * <p>单机场景够用；多实例部署时可加分布式锁或改为时间轮/延迟 Topic。</p>
 */
@Slf4j
@Component
public class OrderTimeoutScheduler {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Scheduled(fixedDelayString = "${hmdp.order.timeout-scan-ms:60000}")
    public void closeTimeoutOrders() {
        try {
            int closed = voucherOrderService.closeTimeoutOrders();
            if (closed > 0) {
                log.info("超时关单完成，关闭 {} 笔未支付订单", closed);
            }
        } catch (Exception e) {
            log.error("超时关单任务执行失败", e);
        }
    }
}
