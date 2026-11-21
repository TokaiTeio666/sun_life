package com.hmdp.config;

import com.hmdp.utils.LocalCacheBroadcaster;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

/**
 * 本地缓存失效广播的订阅端：一条独立的 Redis 订阅连接，收到消息后交给 {@link LocalCacheBroadcaster} 分发。
 */
@Configuration
public class LocalCacheConfig {

    @Bean
    public RedisMessageListenerContainer localCacheEvictListenerContainer(RedisConnectionFactory connectionFactory,
                                                                          LocalCacheBroadcaster broadcaster) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(broadcaster, new ChannelTopic(LocalCacheBroadcaster.CHANNEL));
        return container;
    }
}
