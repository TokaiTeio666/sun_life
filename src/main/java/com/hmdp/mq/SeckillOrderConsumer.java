package com.hmdp.mq;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * 秒杀订单 Kafka 消费者。
 *
 * <p>手动提交位点：订单创建成功后才 ack；抛异常时由 DefaultErrorHandler 重试，
 * 重试耗尽后进入 {@code seckill.orders.DLT} 死信主题。</p>
 */
@Slf4j
@Component
public class SeckillOrderConsumer {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private RedissonClient redissonClient;

    @KafkaListener(
            topics = KafkaConstants.SECKILL_ORDER_TOPIC,
            groupId = KafkaConstants.SECKILL_ORDER_GROUP,
            concurrency = "${hmdp.kafka.consumer.concurrency:3}"
    )
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        SeckillOrderMessage message = JSONUtil.toBean(record.value(), SeckillOrderMessage.class);
        log.debug("收到秒杀订单消息 partition={} offset={} message={}",
                record.partition(), record.offset(), message);

        //1. 同一用户加分布式锁，防止并发重复下单
        RLock lock = redissonClient.getLock("lock:order:" + message.getUserId());
        boolean isLock = lock.tryLock();
        if (!isLock) {
            // 重复消息直接确认，避免无意义重试
            log.warn("不允许重复下单 userId={}", message.getUserId());
            ack.acknowledge();
            return;
        }
        try {
            VoucherOrder voucherOrder = new VoucherOrder()
                    .setId(message.getId())
                    .setUserId(message.getUserId())
                    .setVoucherId(message.getVoucherId())
                    .setStatus(1);
            //2. 创建订单（内部做幂等判断 + DB 库存条件扣减）
            voucherOrderService.createVoucherOrder(voucherOrder);
            //3. 成功后才提交位点
            ack.acknowledge();
        } finally {
            lock.unlock();
        }
    }
}
