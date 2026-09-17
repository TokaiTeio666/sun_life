package com.hmdp.mq;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

/**
 * 死信队列演示消费者（实验用，验证后可删除）
 *
 * 故意每次都抛异常，让消息走一遍完整链路：
 *   消费失败 → 延迟重试（10s → 30s → 1m）→ 第 4 次仍失败 → 进入死信队列 %DLQ%demo-dlq-consumer
 *
 * maxReconsumeTimes=3：把默认 16 次重试调小，1~2 分钟内就能看到消息进 DLQ。
 * 生产环境不要这样设——默认 16 次是为了给临时故障（如数据库抖动）留足恢复时间。
 *
 * 演示步骤：
 *   1. 调用 GET /demo/dlq/send 发一条消息
 *   2. 观察应用日志：onMessage 每次收到都抛异常
 *   3. mqadmin topicList 看 %RETRY%demo-dlq-consumer 出现（重试主题）
 *   4. 等 1~2 分钟，mqadmin topicList 看到 %DLQ%demo-dlq-consumer（死信主题）
 *   5. mqadmin consumerProgress -g demo-dlq-consumer 查看消费进度
 */
@Slf4j
@Component
@RocketMQMessageListener(topic = "demo-dlq", consumerGroup = "demo-dlq-consumer", maxReconsumeTimes = 3)
public class DemoDlqConsumer implements RocketMQListener<String> {

    @Override
    public void onMessage(String message) {
        log.info("【DLQ演示】收到消息：{}", message);
        // 故意抛异常：模拟消费失败（比如数据库不可用、消息数据异常等真实场景）
        throw new RuntimeException("演示：故意抛异常，观察消息重试与进入死信队列");
    }
}
