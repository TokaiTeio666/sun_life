package com.hmdp.config;

import com.hmdp.mq.KafkaConstants;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Kafka 配置：主题创建 + 批量消费容器 + 消费失败重试 + 死信主题。
 */
@Configuration
public class KafkaConfig {

    /**
     * 秒杀订单主题分区数。分区数决定了整个消费组的最大并行度：
     * 多实例部署时「实例数 × concurrency」不应超过分区数，否则多出的消费者空闲。
     * 已存在的主题会被 KafkaAdmin 自动扩容到该值（分区只能增不能减）。
     */
    @Value("${hmdp.kafka.seckill-partitions:12}")
    private int seckillPartitions;

    @Bean
    public NewTopic seckillOrderTopic() {
        return TopicBuilder.name(KafkaConstants.SECKILL_ORDER_TOPIC)
                .partitions(seckillPartitions)
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
     * 批量监听器抛 BatchListenerFailedException 时，只重试/死信失败的那一条。
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> new TopicPartition(KafkaConstants.SECKILL_ORDER_DLT_TOPIC, -1));
        return new DefaultErrorHandler(recoverer, new FixedBackOff(500L, 3L));
    }

    /**
     * 秒杀订单专用批量监听容器：一次 poll（max-poll-records）整批交给监听器，
     * 监听器正常返回后由容器提交整批位点（AckMode.BATCH）。
     * 其他监听器（如 DLT）继续使用 Spring Boot 默认的单条 + 手动 ack 容器。
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> seckillBatchListenerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory,
            DefaultErrorHandler kafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        factory.setBatchListener(true);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        factory.setCommonErrorHandler(kafkaErrorHandler);
        return factory;
    }
}
