package com.hmdp.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * 本地缓存失效广播：一个副本删除了本地缓存里的某个 key，通过 Redis Pub/Sub 通知其他副本也删除。
 *
 * <p>消息格式 {@code 缓存名|key}。各本地缓存用 {@link #register} 登记自己的删除动作，
 * 数据变更方先删本机缓存，再调 {@link #broadcast} 通知其他副本（自己也会收到，重复删除无害）。</p>
 *
 * <p>Pub/Sub 不保证送达（订阅连接断开期间的消息会丢），所以它只是「尽快失效」，
 * 兜底靠本地缓存自己的短 TTL：最坏情况下其他副本在 TTL 到期后才看到变更。</p>
 */
@Slf4j
@Component
public class LocalCacheBroadcaster implements MessageListener {

    public static final String CHANNEL = "hmdp:local-cache:evict";

    private final StringRedisTemplate stringRedisTemplate;
    private final Map<String, Consumer<String>> evictors = new ConcurrentHashMap<>();

    public LocalCacheBroadcaster(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /** 登记一个本地缓存：收到广播时用 evictor 删除对应的 key。缓存名不能包含 '|'。 */
    public void register(String cacheName, Consumer<String> evictor) {
        evictors.put(cacheName, evictor);
    }

    /** 通知所有副本删除本地缓存里的 key。发送失败只记录日志，由 TTL 兜底。 */
    public void broadcast(String cacheName, String key) {
        try {
            stringRedisTemplate.convertAndSend(CHANNEL, cacheName + "|" + key);
        } catch (Exception e) {
            log.warn("本地缓存失效广播发送失败，其他副本将在 TTL 到期后失效 cache={}", cacheName, e);
        }
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        String body = new String(message.getBody(), StandardCharsets.UTF_8);
        int sep = body.indexOf('|');
        if (sep <= 0) {
            return;
        }
        Consumer<String> evictor = evictors.get(body.substring(0, sep));
        if (evictor != null) {
            evictor.accept(body.substring(sep + 1));
        }
    }
}
