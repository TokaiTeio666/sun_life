package com.hmdp.mq;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;

/**
 * 订单超时检查消费者（延迟消息）
 *
 * 生产者（createVoucherOrder）在订单创建后发送一条延迟消息（默认等级16 = 30分钟），
 * 到时间后 RocketMQ 才把消息投递到 seckill-order-timeout 主题，本消费者收到后：
 *   1. 查询订单
 *   2. 若订单仍为"未支付(1)" → 调用 closeTimeoutOrder 关单 + 回补库存
 *   3. 若已支付/已取消 → 什么都不做
 *
 * 学习点：
 * - 延迟消息不是"定时任务"，而是 Broker 先把消息写入 SCHEDULE_TOPIC_XXXX（定时主题），
 *   到点后再投递到真实主题，所以支持分布式的延迟调度，无需自己维护定时器；
 * - 延迟等级只有固定的 18 档（1s~2h），不是任意秒数——这是 RocketMQ 4.x 延迟消息的局限，
 *   5.x 的定时消息（syncSendDeliverTimeMills）支持精确到毫秒的任意时间。
 */
@Slf4j
@Component
@RocketMQMessageListener(topic = "seckill-order-timeout", consumerGroup = "order-timeout-consumer")
public class OrderTimeoutConsumer implements RocketMQListener<Long> {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Override
    public void onMessage(Long orderId) {
        log.info("收到订单超时检查消息，orderId={}", orderId);
        VoucherOrder order = voucherOrderService.getById(orderId);
        if (order == null) {
            log.warn("订单不存在，跳过（可能事务回滚后未真正创建），orderId={}", orderId);
            return;
        }
        if (order.getStatus() != 1) {
            log.info("订单状态={}，非未支付，跳过自动关单，orderId={}", order.getStatus(), orderId);
            return;
        }
        voucherOrderService.closeTimeoutOrder(order);
    }
}
