package com.hmdp.mq;

import cn.hutool.json.JSONUtil;
import com.hmdp.service.FeedService;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 笔记发布消息的消费者：把笔记分发到粉丝的 Feed（普通作者分批推送，大 V 只标记、由粉丝读取时拉取）。
 *
 * <p>分发是幂等的（ZADD 同一个 member、同一个 score），所以失败后整条消息重试即可；
 * 重试耗尽由 {@code feedErrorHandler} 记录日志和指标后跳过，不阻塞同分区后面的消息。</p>
 *
 * <p>不受 {@code FEED_ASYNC_PUSH} 开关影响、始终运行：开关关掉后，之前已经发出的消息仍要消费完。</p>
 */
@Slf4j
@Component
public class BlogPublishedConsumer {

    @Resource
    private FeedService feedService;

    @KafkaListener(
            topics = KafkaConstants.BLOG_PUBLISHED_TOPIC,
            groupId = KafkaConstants.FEED_FANOUT_GROUP,
            containerFactory = "feedListenerFactory",
            concurrency = "${hmdp.feed.consumer-concurrency:2}"
    )
    public void onMessage(ConsumerRecord<String, String> record) {
        BlogPublishedMessage message = JSONUtil.toBean(record.value(), BlogPublishedMessage.class);
        if (message.getBlogId() == null || message.getUserId() == null || message.getPublishedAt() == null) {
            throw new IllegalArgumentException("非法笔记发布消息: " + record.value());
        }
        feedService.fanOut(message);
    }
}
