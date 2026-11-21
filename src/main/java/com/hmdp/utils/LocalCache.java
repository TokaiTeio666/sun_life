package com.hmdp.utils;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;

/**
 * 多级缓存的第一级：副本内的 Caffeine 短缓存，挡在 Redis 前面。
 *
 * <p>key 直接用对应的 Redis key（如 {@code cache:shop:1}），value 是已经反序列化好的对象。
 * 热点数据命中本地缓存时一条 Redis 命令都不发，也省掉 JSON 反序列化。</p>
 *
 * <p>一致性：本地缓存 TTL 很短（默认 10s），数据变更方调 {@link #evict} 删本机并通过
 * {@link LocalCacheBroadcaster} 通知其他副本；广播丢失时其他副本最多再读到一个 TTL 的旧值。</p>
 *
 * <p><b>缓存的对象在请求之间共享，调用方不能修改</b>；需要按请求改字段的（如笔记的 isLike）自己先复制。</p>
 *
 * <p>开关：{@code hmdp.cache.local.enabled}（环境变量 {@code LOCAL_CACHE_ENABLED}）。</p>
 */
@Slf4j
@Component
public class LocalCache {

    private static final String CACHE_NAME = "l1";

    /** null 表示关闭 */
    private final Cache<String, Object> cache;
    private final LocalCacheBroadcaster broadcaster;

    public LocalCache(@Value("${hmdp.cache.local.enabled:true}") boolean enabled,
                      @Value("${hmdp.cache.local.ttl-seconds:10}") long ttlSeconds,
                      @Value("${hmdp.cache.local.max-size:10000}") long maxSize,
                      LocalCacheBroadcaster broadcaster,
                      MeterRegistry registry) {
        this.broadcaster = broadcaster;
        if (!enabled || ttlSeconds <= 0 || maxSize <= 0) {
            this.cache = null;
            log.info("本地缓存（多级缓存第一级）已关闭，读请求直接访问 Redis");
        } else {
            this.cache = Caffeine.newBuilder()
                    .expireAfterWrite(Duration.ofSeconds(ttlSeconds))
                    .maximumSize(maxSize)
                    .recordStats()
                    .build();
            CaffeineCacheMetrics.monitor(registry, cache, CACHE_NAME);
            log.info("本地缓存（多级缓存第一级）：TTL {}s，容量上限 {} 条", ttlSeconds, maxSize);
        }
        broadcaster.register(CACHE_NAME, this::evictLocal);
    }

    /** 未命中或缓存关闭返回 null。 */
    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        return cache == null ? null : (T) cache.getIfPresent(key);
    }

    public void put(String key, Object value) {
        if (cache != null && value != null) {
            cache.put(key, value);
        }
    }

    /** 数据变更后调用：删本机缓存并通知其他副本。 */
    public void evict(String key) {
        evictLocal(key);
        broadcaster.broadcast(CACHE_NAME, key);
    }

    /**
     * 同 {@link #evict}，但如果当前在事务里，等事务提交后再执行。
     * 事务提交前就删缓存的话，并发的读请求会把旧值重新读进缓存。
     */
    public void evictAfterCommit(String key, Runnable alsoRun) {
        Runnable action = () -> {
            if (alsoRun != null) {
                alsoRun.run();
            }
            evict(key);
        };
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    action.run();
                }
            });
        } else {
            action.run();
        }
    }

    private void evictLocal(String key) {
        if (cache != null) {
            cache.invalidate(key);
        }
    }
}
