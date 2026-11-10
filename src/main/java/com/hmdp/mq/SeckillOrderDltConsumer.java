package com.hmdp.mq;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * 死信主题消费者：这里不重试业务，只记录告警信息，便于人工补偿。
 */
@Slf4j
@Component
public class SeckillOrderDltConsumer {

    @KafkaListener(
            topics = KafkaConstants.SECKILL_ORDER_DLT_TOPIC,
            groupId = KafkaConstants.SECKILL_ORDER_DLT_GROUP
    )
    public void onDeadLetter(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.error("秒杀订单进入死信 topic={} partition={} offset={} key={} value={}",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
        ack.acknowledge();
    }
}
