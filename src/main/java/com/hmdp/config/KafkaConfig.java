package com.hmdp.config;

import com.hmdp.mq.KafkaConstants;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import org.apache.kafka.clients.admin.NewTopic;

/**
 * Kafka 配置：主题创建 + 消费失败重试 + 死信主题。
 */
@Configuration
public class KafkaConfig {

    /** 秒杀订单主题：3 分区，消费者并发度与之对齐 */
    @Bean
    public NewTopic seckillOrderTopic() {
        return TopicBuilder.name(KafkaConstants.SECKILL_ORDER_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    /** 死信主题：重试耗尽的订单消息落这里，便于人工补偿与告警 */
    @Bean
    public NewTopic seckillOrderDltTopic() {
        return TopicBuilder.name(KafkaConstants.SECKILL_ORDER_DLT_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }

    /**
     * 消费失败固定间隔重试 3 次；仍失败则投递到 DLT，避免阻塞后续消息。
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> new TopicPartition(KafkaConstants.SECKILL_ORDER_DLT_TOPIC, -1));
        return new DefaultErrorHandler(recoverer, new FixedBackOff(500L, 3L));
    }
}
