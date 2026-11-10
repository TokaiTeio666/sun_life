package com.hmdp.controller;

import com.hmdp.dto.Result;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.annotation.Resource;
import java.time.LocalDateTime;

/**
 * 死信队列演示入口（实验用，验证后可删除）
 *
 * GET /demo/dlq/send
 * 往 demo-dlq 主题发一条消息，DemoDlqConsumer 会故意消费失败，
 * 消息经历重试后进入死信队列 %DLQ%demo-dlq-consumer。
 */
@Slf4j
@RestController
@RequestMapping("/demo/dlq")
public class DemoDlqController {

    @Resource
    private RocketMQTemplate rocketMQTemplate;

    @GetMapping("/send")
    public Result sendDlqDemoMessage() {
        String message = "dlq-demo-" + LocalDateTime.now();
        rocketMQTemplate.convertAndSend("demo-dlq", message);
        log.info("【DLQ演示】已发送消息：{}", message);
        return Result.ok(message);
    }
}
