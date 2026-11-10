package com.hmdp.mq;

/**
 * Kafka 常量
 */
public final class KafkaConstants {

    /** 秒杀订单消息主题 */
    public static final String SECKILL_ORDER_TOPIC = "seckill.orders";

    /** 重试耗尽后的死信主题（Dead Letter Topic） */
    public static final String SECKILL_ORDER_DLT_TOPIC = "seckill.orders.DLT";

    /** 秒杀订单消费者组 */
    public static final String SECKILL_ORDER_GROUP = "seckill-order-group";

    /** 死信主题消费者组（仅用于告警/排查） */
    public static final String SECKILL_ORDER_DLT_GROUP = "seckill-order-dlt-group";

    private KafkaConstants() {
    }
}
