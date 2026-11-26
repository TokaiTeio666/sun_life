package com.hmdp.service;

import cn.hutool.json.JSONUtil;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.hmdp.entity.Follow;
import com.hmdp.metrics.BizMetrics;
import com.hmdp.mq.BlogPublishedMessage;
import com.hmdp.mq.KafkaConstants;
import com.hmdp.utils.LocalCacheBroadcaster;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hmdp.utils.RedisConstants.FEED_BIGV_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_MAX_SIZE;
import static com.hmdp.utils.RedisConstants.FEED_OUTBOX_KEY;
import static com.hmdp.utils.RedisConstants.FEED_OUTBOX_MAX_SIZE;
import static com.hmdp.utils.RedisConstants.FOLLOWS_KEY;

/**
 * 关注 Feed：发布时的分发与读取时的合并（推拉结合）。
 *
 * <p><b>发布</b>（{@link #publish}）：请求线程里只写作者自己的发件箱 {@code outbox:{作者}}（2 条 Redis 命令）
 * 并发一条 Kafka 消息，耗时与粉丝数无关。</p>
 *
 * <p><b>分发</b>（{@link #fanOut}，Kafka 消费者调用）：</p>
 * <ul>
 *   <li>普通作者：按 tb_follow 的 id 游标分页读粉丝，每页一个 pipeline 推送到粉丝收件箱 {@code feed:{粉丝}}；</li>
 *   <li>大 V（粉丝数超过阈值）：不推送，只把作者记入 {@code feed:bigv}。6 万粉丝的作者发一条笔记原本是 12 万条
 *       Redis 命令，现在是 0 条。</li>
 * </ul>
 *
 * <p><b>读取</b>（{@link #read}）：收件箱 + 所关注大 V 的发件箱，按时间合并、按笔记 id 去重。
 * 发件箱对所有作者都写（不只是大 V），所以作者的粉丝数越过阈值时不需要回填历史笔记：
 * 之前推送过的笔记在收件箱和发件箱里各有一份，score 相同，合并时去重。</p>
 *
 * <p>大 V 标记只增不减：粉丝数回落到阈值以下也不会自动摘除，否则摘除前只存在于发件箱的笔记会从粉丝的 Feed 里消失。</p>
 */
@Slf4j
@Service
public class FeedService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IFollowService followService;

    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    @Resource
    private BizMetrics bizMetrics;

    /** false = 回到改造前：发帖时在请求线程里同步推送给全部粉丝，不写发件箱、不区分大 V */
    @Value("${hmdp.feed.async-push:true}")
    private boolean asyncPush;

    /** 粉丝数超过该值的作者不推送，改为读取时拉取；小于等于 0 表示不区分大 V，全部推送 */
    @Value("${hmdp.feed.bigv-threshold:5000}")
    private int bigvThreshold;

    /** 推送时每页读多少个粉丝（每页一次 DB 查询 + 一个 Redis pipeline） */
    @Value("${hmdp.feed.push-batch-size:1000}")
    private int pushBatchSize;

    /** 「我关注的大 V」本地缓存秒数；<= 0 关闭 */
    @Value("${hmdp.feed.bigv-local-cache-seconds:5}")
    private long bigvLocalCacheSeconds;

    @Value("${hmdp.feed.bigv-local-cache-max-size:200000}")
    private long bigvLocalCacheMaxSize;

    @Resource
    private LocalCacheBroadcaster localCacheBroadcaster;

    /** 用户 id → 他关注的人里哪些是大 V。null 表示关闭。值是不可变集合，可以在请求之间共享 */
    private Cache<Long, Set<String>> followedBigVsCache;

    private static final String BIGV_CACHE_NAME = "feed-bigv";

    @PostConstruct
    void initFollowedBigVsCache() {
        if (bigvThreshold > 0 && bigvLocalCacheSeconds > 0 && bigvLocalCacheMaxSize > 0) {
            followedBigVsCache = Caffeine.newBuilder()
                    .expireAfterWrite(Duration.ofSeconds(bigvLocalCacheSeconds))
                    .maximumSize(bigvLocalCacheMaxSize)
                    .build();
        }
        // 关注 / 取关会改变「我关注的大 V」：通过广播让所有副本删掉这个用户的缓存
        localCacheBroadcaster.register(BIGV_CACHE_NAME, key -> {
            if (followedBigVsCache != null) {
                followedBigVsCache.invalidate(Long.valueOf(key));
            }
        });
    }

    /** 关注关系变化后调用（FollowServiceImpl）：删本机缓存并通知其他副本；广播丢失时由本地缓存的短 TTL 兜底。 */
    public void onFollowChanged(Long userId) {
        if (followedBigVsCache == null) {
            return;
        }
        followedBigVsCache.invalidate(userId);
        localCacheBroadcaster.broadcast(BIGV_CACHE_NAME, userId.toString());
    }

    /** 一页 Feed 及下一页的滚动参数 */
    public record FeedPage(List<Long> blogIds, long minTime, int offset) {
    }

    // ---------------------------------------------------------------- 发布

    /** 笔记落库后调用。 */
    public void publish(Long blogId, Long authorId) {
        long publishedAt = System.currentTimeMillis();
        if (!asyncPush) {
            pushToFollowers(authorId, blogId, publishedAt);
            return;
        }
        // 1. 写作者发件箱并截断到最近 FEED_OUTBOX_MAX_SIZE 条
        String outboxKey = FEED_OUTBOX_KEY + authorId;
        stringRedisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            StringRedisConnection conn = (StringRedisConnection) connection;
            conn.zAdd(outboxKey, publishedAt, blogId.toString());
            conn.zRemRange(outboxKey, 0, -(FEED_OUTBOX_MAX_SIZE + 1));
            return null;
        });
        // 2. 发消息，由消费者分发。发送失败只能记录：笔记已落库，大 V 的笔记仍能被拉取到，普通作者的粉丝收不到这一条
        //    （不丢消息需要本地消息表，属于批次 C 的 Outbox）
        String payload = JSONUtil.toJsonStr(new BlogPublishedMessage(blogId, authorId, publishedAt));
        try {
            kafkaTemplate.send(KafkaConstants.BLOG_PUBLISHED_TOPIC, authorId.toString(), payload)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            onPublishFailure(blogId, authorId, ex);
                        }
                    });
        } catch (Exception e) {
            onPublishFailure(blogId, authorId, e);
        }
    }

    private void onPublishFailure(Long blogId, Long authorId, Throwable ex) {
        bizMetrics.feedPublishFailure();
        log.error("笔记发布消息发送失败，该笔记不会推送给粉丝 blogId={} userId={}", blogId, authorId, ex);
    }

    // ---------------------------------------------------------------- 分发（消费者）

    public void fanOut(BlogPublishedMessage message) {
        Long authorId = message.getUserId();
        if (isBigV(authorId)) {
            bizMetrics.feedFanout(true, 0);
            return;
        }
        long pushed = pushToFollowers(authorId, message.getBlogId(), message.getPublishedAt());
        bizMetrics.feedFanout(false, pushed);
    }

    /** 已标记的大 V，或粉丝数刚越过阈值（此时顺便标记）。 */
    private boolean isBigV(Long authorId) {
        if (bigvThreshold <= 0) {
            return false;
        }
        if (Boolean.TRUE.equals(stringRedisTemplate.opsForSet().isMember(FEED_BIGV_KEY, authorId.toString()))) {
            return true;
        }
        // 不 COUNT 全部粉丝：只看第「阈值 + 1」个粉丝是否存在，最多扫描 阈值 + 1 条索引记录
        boolean exceeds = !followService.query()
                .select("id")
                .eq("follow_user_id", authorId)
                .last("LIMIT 1 OFFSET " + bigvThreshold)
                .list().isEmpty();
        if (exceeds) {
            stringRedisTemplate.opsForSet().add(FEED_BIGV_KEY, authorId.toString());
            log.info("作者 {} 的粉丝数超过 {}，标记为大 V：之后的笔记不再推送，由粉丝读取时拉取", authorId, bigvThreshold);
        }
        return exceeds;
    }

    /**
     * 把笔记推送到作者全部粉丝的收件箱，返回推送的粉丝数。
     * 按 tb_follow.id 游标分页（走 idx_follow_user，同一 follow_user_id 下按主键有序），不一次读出全部粉丝。
     */
    private long pushToFollowers(Long authorId, Long blogId, long publishedAt) {
        String member = blogId.toString();
        long cursor = 0;
        long pushed = 0;
        while (true) {
            List<Follow> page = followService.query()
                    .select("id", "user_id")
                    .eq("follow_user_id", authorId)
                    .gt("id", cursor)
                    .orderByAsc("id")
                    .last("LIMIT " + pushBatchSize)
                    .list();
            if (page.isEmpty()) {
                break;
            }
            stringRedisTemplate.executePipelined((RedisCallback<Object>) connection -> {
                StringRedisConnection conn = (StringRedisConnection) connection;
                for (Follow follow : page) {
                    String key = FEED_KEY + follow.getUserId();
                    conn.zAdd(key, publishedAt, member);
                    conn.zRemRange(key, 0, -(FEED_MAX_SIZE + 1));
                }
                return null;
            });
            pushed += page.size();
            cursor = page.get(page.size() - 1).getId();
            if (page.size() < pushBatchSize) {
                break;
            }
        }
        return pushed;
    }

    // ---------------------------------------------------------------- 读取

    /**
     * 滚动读取一页 Feed。
     *
     * @param max    上一页最后一条的时间（第一页传当前时间）
     * @param offset 上一页里时间等于 max 的条数（第一页传 0），即这次要跳过的条数
     * @return 没有更多数据时返回 null
     */
    public FeedPage read(Long userId, long max, int offset, int pageSize) {
        // 1. 数据源：自己的收件箱 + 所关注大 V 的发件箱
        List<String> keys = new ArrayList<>();
        keys.add(FEED_KEY + userId);
        for (String bigV : cachedFollowedBigVs(userId)) {
            keys.add(FEED_OUTBOX_KEY + bigV);
        }
        // 2. 每个数据源取 score <= max 的前 offset + pageSize 条（合并后的前 offset + pageSize 条一定在其中），一次往返
        int fetch = offset + pageSize;
        List<Object> results = stringRedisTemplate.executePipelined(new SessionCallback<Object>() {
            @Override
            @SuppressWarnings("unchecked")
            public <K, V> Object execute(RedisOperations<K, V> operations) throws DataAccessException {
                ZSetOperations<String, String> zset = ((RedisOperations<String, String>) operations).opsForZSet();
                for (String key : keys) {
                    zset.reverseRangeByScoreWithScores(key, 0, max, 0, fetch);
                }
                return null;
            }
        });
        // 3. 合并并按笔记 id 去重（同一条笔记在收件箱和发件箱里的 score 相同）
        Map<Long, Long> timeByBlog = new HashMap<>();
        for (Object result : results) {
            if (!(result instanceof Collection<?> tuples)) {
                continue;
            }
            for (Object o : tuples) {
                ZSetOperations.TypedTuple<?> tuple = (ZSetOperations.TypedTuple<?>) o;
                if (tuple.getValue() != null && tuple.getScore() != null) {
                    timeByBlog.put(Long.valueOf(tuple.getValue().toString()), tuple.getScore().longValue());
                }
            }
        }
        // 4. 时间倒序；时间相同按笔记 id 倒序，保证翻页时同一毫秒的几条顺序固定，offset 才有意义
        List<Map.Entry<Long, Long>> merged = new ArrayList<>(timeByBlog.entrySet());
        merged.sort(Comparator.<Map.Entry<Long, Long>>comparingLong(Map.Entry::getValue).reversed()
                .thenComparing(Map.Entry::getKey, Comparator.reverseOrder()));
        // 5. 跳过上一页已经返回的、时间等于 max 的那几条
        int sameAsMax = 0;
        while (sameAsMax < merged.size() && merged.get(sameAsMax).getValue() == max) {
            sameAsMax++;
        }
        int skip = Math.min(offset, sameAsMax);
        List<Map.Entry<Long, Long>> page = merged.subList(skip, Math.min(merged.size(), skip + pageSize));
        if (page.isEmpty()) {
            return null;
        }
        // 6. 下一页的滚动参数：minTime = 本页最后一条的时间，offset = 已返回的、时间等于 minTime 的条数
        long minTime = page.get(page.size() - 1).getValue();
        int nextOffset = 0;
        List<Long> ids = new ArrayList<>(page.size());
        for (Map.Entry<Long, Long> e : page) {
            ids.add(e.getKey());
            if (e.getValue() == minTime) {
                nextOffset++;
            }
        }
        if (minTime == max) {
            // 整页都与 max 同一毫秒：之前跳过的那几条也要算上
            nextOffset += skip;
        }
        return new FeedPage(ids, minTime, nextOffset);
    }

    /**
     * 先查本地缓存。不用 Cache.get(key, loader)：loader 在 ConcurrentHashMap.compute 的 synchronized 里执行，
     * 里面访问 Redis 会把虚拟线程钉在载体线程上；两个请求同时未命中时各查一次，结果相同，无害。
     * <p>已知竞态（接受）：查 Redis 与 put 之间如果发生关注 / 取关，失效广播先到，put 会把旧结果写回本地缓存，
     * 新关注的大 V 最多晚 {@code bigv-local-cache-seconds}（默认 5s）出现在 Feed 里。为这几秒加版本号不值得。</p>
     */
    private Set<String> cachedFollowedBigVs(Long userId) {
        if (followedBigVsCache == null) {
            return followedBigVs(userId);
        }
        Set<String> cached = followedBigVsCache.getIfPresent(userId);
        if (cached != null) {
            return cached;
        }
        Set<String> bigVs = Set.copyOf(followedBigVs(userId));
        followedBigVsCache.put(userId, bigVs);
        return bigVs;
    }

    /**
     * 当前用户关注的人里哪些是大 V：{@code follows:{userId}} 与 {@code feed:bigv} 求交集。
     * 关注集合不在 Redis 里（造数、被淘汰）时先从 DB 重建，与 FollowServiceImpl.isFollow 的做法一致。
     */
    private Set<String> followedBigVs(Long userId) {
        String followsKey = FOLLOWS_KEY + userId;
        List<Object> results = stringRedisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            StringRedisConnection conn = (StringRedisConnection) connection;
            conn.exists(followsKey);
            conn.sInter(followsKey, FEED_BIGV_KEY);
            return null;
        });
        if (Boolean.TRUE.equals(results.get(0))) {
            return toStringSet(results.get(1));
        }
        List<Follow> follows = followService.query().select("follow_user_id").eq("user_id", userId).list();
        if (follows.isEmpty()) {
            return Set.of();
        }
        stringRedisTemplate.opsForSet().add(followsKey, follows.stream()
                .map(f -> f.getFollowUserId().toString()).toArray(String[]::new));
        Set<String> bigVs = stringRedisTemplate.opsForSet().intersect(followsKey, FEED_BIGV_KEY);
        return bigVs == null ? Set.of() : bigVs;
    }

    private static Set<String> toStringSet(Object o) {
        if (!(o instanceof Collection<?> c) || c.isEmpty()) {
            return Set.of();
        }
        Set<String> set = new java.util.HashSet<>(c.size() * 2);
        for (Object item : c) {
            set.add(item.toString());
        }
        return set;
    }
}
