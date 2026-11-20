package com.hmdp.utils;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.hmdp.dto.UserDTO;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * 登录态本地短缓存：token → 用户。
 *
 * <p>每个带 token 的请求原本都要一次 {@code HGETALL login:token:{token}}，是全站 Redis 命令数里最大的一项。
 * 这里在副本内缓存十几秒：同一个用户连续请求时只有第一次（以及每次过期后的第一次）访问 Redis。</p>
 *
 * <p>代价是登录态的变化最多延迟一个 TTL 才在本副本生效：</p>
 * <ul>
 *   <li>退出登录：本机立即删除，并通过 {@link LocalCacheBroadcaster} 通知其他副本；广播丢失时由 TTL 兜底；</li>
 *   <li>token 在 Redis 里自然过期：最多多活一个 TTL。</li>
 * </ul>
 *
 * <p>开关：{@code hmdp.login.local-cache-seconds}（环境变量 {@code LOGIN_LOCAL_CACHE_SECONDS}），0 = 关闭。</p>
 */
@Slf4j
@Component
public class LoginUserCache {

    private static final String CACHE_NAME = "loginUser";

    /** null 表示关闭 */
    private final Cache<String, UserDTO> cache;
    private final LocalCacheBroadcaster broadcaster;

    public LoginUserCache(@Value("${hmdp.login.local-cache-seconds:15}") long ttlSeconds,
                          @Value("${hmdp.login.local-cache-max-size:200000}") long maxSize,
                          LocalCacheBroadcaster broadcaster,
                          MeterRegistry registry) {
        this.broadcaster = broadcaster;
        if (ttlSeconds <= 0) {
            this.cache = null;
            log.info("登录态本地缓存已关闭（LOGIN_LOCAL_CACHE_SECONDS=0），每个带 token 的请求访问一次 Redis");
        } else {
            this.cache = Caffeine.newBuilder()
                    .expireAfterWrite(Duration.ofSeconds(ttlSeconds))
                    .maximumSize(maxSize)
                    .recordStats()
                    .build();
            // 输出 cache_gets_total{cache="loginUser",result="hit|miss"} 等指标
            CaffeineCacheMetrics.monitor(registry, cache, CACHE_NAME);
            log.info("登录态本地缓存：TTL {}s，容量上限 {}", ttlSeconds, maxSize);
        }
        // 关闭时也登记：其他副本可能开着，广播照发照收
        broadcaster.register(CACHE_NAME, this::evictLocal);
    }

    /** 命中返回用户；未命中或缓存关闭返回 null。返回的对象在请求之间共享，调用方不要修改。 */
    public UserDTO get(String token) {
        return cache == null ? null : cache.getIfPresent(token);
    }

    public void put(String token, UserDTO user) {
        if (cache != null) {
            cache.put(token, user);
        }
    }

    /** 退出登录：删除本机缓存并通知其他副本。 */
    public void evict(String token) {
        evictLocal(token);
        broadcaster.broadcast(CACHE_NAME, token);
    }

    private void evictLocal(String token) {
        if (cache != null) {
            cache.invalidate(token);
        }
    }
}
