package com.hmdp.utils;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * 秒杀售罄本地标记。
 *
 * <p>一张券卖完之后，后面的请求照样会去 Redis 执行一次 Lua（还要先生成订单号），结果只是得到「库存不足」。
 * 而单券的 Lua 是单 key 热点，加 Redis 节点也分摊不了。这里在副本内记下「这张券卖完了」，
 * 之后的请求直接返回，不访问 Redis。</p>
 *
 * <p>库存回补时要清除标记，否则补回来的库存卖不出去：</p>
 * <ul>
 *   <li>超时关单回补、消息发送失败回滚、新增 / 重置秒杀券：调 {@link #clear}，删本机标记并通过
 *       {@link LocalCacheBroadcaster} 通知其他副本；</li>
 *   <li>兜底：标记本身只保留几秒（{@code hmdp.seckill.soldout-ttl-seconds}），广播丢失或有人直接改 Redis 库存时，
 *       最多这么久之后会再去 Redis 确认一次。</li>
 * </ul>
 *
 * <p>开关：{@code hmdp.seckill.soldout-flag}（环境变量 {@code SECKILL_SOLDOUT_FLAG}）。</p>
 */
@Slf4j
@Component
public class SeckillSoldOutFlags {

    public static final String CACHE_NAME = "soldOut";

    /** null 表示关闭 */
    private final Cache<Long, Boolean> flags;
    private final LocalCacheBroadcaster broadcaster;

    public SeckillSoldOutFlags(@Value("${hmdp.seckill.soldout-flag:true}") boolean enabled,
                               @Value("${hmdp.seckill.soldout-ttl-seconds:5}") long ttlSeconds,
                               LocalCacheBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
        if (!enabled || ttlSeconds <= 0) {
            this.flags = null;
            log.info("秒杀售罄本地标记已关闭，售罄后的请求仍会执行 Lua");
        } else {
            this.flags = Caffeine.newBuilder()
                    .expireAfterWrite(Duration.ofSeconds(ttlSeconds))
                    .maximumSize(100_000)
                    .build();
        }
        broadcaster.register(CACHE_NAME, voucherId -> {
            try {
                clearLocal(Long.valueOf(voucherId));
            } catch (NumberFormatException ignored) {
                // 非法消息，忽略
            }
        });
    }

    public boolean isSoldOut(Long voucherId) {
        return flags != null && flags.getIfPresent(voucherId) != null;
    }

    /** Lua 返回库存不足后调用。 */
    public void mark(Long voucherId) {
        if (flags != null) {
            flags.put(voucherId, Boolean.TRUE);
        }
    }

    /** 库存回补后调用：删本机标记并通知其他副本。 */
    public void clear(Long voucherId) {
        clearLocal(voucherId);
        broadcaster.broadcast(CACHE_NAME, voucherId.toString());
    }

    private void clearLocal(Long voucherId) {
        if (flags != null) {
            flags.invalidate(voucherId);
        }
    }
}
