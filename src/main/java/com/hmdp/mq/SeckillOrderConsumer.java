package com.hmdp.mq;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;

/**
 * 秒杀订单消费者
 *
 * 监听 RocketMQ 的 seckill-order 主题，异步创建订单，实现"秒杀接口快速响应 + 订单异步落库"。
 *
 * 学习要点：
 * 1. 消费者组（consumerGroup）：多个相同 group 的消费者实例共同分担消息（负载均衡），
 *    单实例内默认并发消费（默认线程数由 consumeThreadNumber 控制，默认20）；
 * 2. 消费语义：RocketMQ 是 At Least Once（至少一次），onMessage 抛异常会自动重试（默认16次），
 *    重试耗尽进入死信队列 %DLQ%seckill-order-consumer，因此消费端必须做幂等（见 createVoucherOrder）；
 * 3. 如果想保证同一用户订单按顺序处理，可把 consumeMode 改为 ORDERLY（默认 CONCURRENTLY），
 *    代价是吞吐下降，秒杀场景通常不需要。
 *
 * @author hm
 * @since 2026-09-15
 */
@Slf4j
@Component
@RocketMQMessageListener(topic = "seckill-order", consumerGroup = "seckill-order-consumer")
public class SeckillOrderConsumer implements RocketMQListener<VoucherOrder> {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Override
    public void onMessage(VoucherOrder voucherOrder) {
        log.info("收到秒杀订单消息：orderId={}, userId={}, voucherId={}",
                voucherOrder.getId(), voucherOrder.getUserId(), voucherOrder.getVoucherId());
        voucherOrderService.createVoucherOrder(voucherOrder);
    }
}
