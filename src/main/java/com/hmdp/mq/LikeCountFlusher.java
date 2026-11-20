package com.hmdp.mq;

import com.hmdp.metrics.BizMetrics;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hmdp.utils.RedisConstants.BLOG_LIKE_DELTA_FLUSHING_KEY;
import static com.hmdp.utils.RedisConstants.BLOG_LIKE_DELTA_KEY;

/**
 * 点赞数定时回写：把 Redis 里累积的点赞数增量批量写回 {@code tb_blog.liked}。
 *
 * <p>点赞接口只在 Redis 里切换点赞状态并把 ±1 累加到 hash {@code blog:like:delta}（见 BlogServiceImpl），
 * 这里每隔几秒把整个 hash 取走，同一篇笔记的多次点赞合并成一条 UPDATE，一批在一个事务里批量执行。</p>
 *
 * <p><b>不丢增量</b>：先把 hash 改名为 {@code blog:like:delta:flushing}（改名是原子的，之后的点赞写进新的 hash），
 * 写库成功后才删除；写库失败时保留，下一轮先重试这一批。</p>
 *
 * <p><b>可能重复</b>：写库事务已提交、还没来得及删除 flushing 时进程被杀，下一轮会把这一批再加一遍（至少一次）。
 * 窗口只有一次 DEL 的时间；点赞数不是账，接受这个误差。</p>
 *
 * <p>多副本时用 Redisson 锁保证同一时刻只有一个副本回写。不受 {@code LIKE_ASYNC_FLUSH} 开关影响、始终运行：
 * 开关关掉之后，之前累积的增量仍要写回。</p>
 */
@Slf4j
@Component
public class LikeCountFlusher {

    private static final String JOB_LOCK_KEY = "lock:job:like-flush";
    private static final int BATCH_SIZE = 500;

    /** 有上一轮没写完的批次就继续用它；否则把当前增量 hash 改名成待回写批次。返回 0 表示没有要回写的数据 */
    private static final DefaultRedisScript<Long> TAKE_SCRIPT = new DefaultRedisScript<>(
            "if redis.call('exists', KEYS[2]) == 1 then return 1 end " +
            "if redis.call('exists', KEYS[1]) == 0 then return 0 end " +
            "redis.call('rename', KEYS[1], KEYS[2]) " +
            "return 1", Long.class);

    /** liked 是无符号列：先转成有符号再加，取消点赞多于点赞时最低减到 0，不会溢出报错 */
    private static final String UPDATE_SQL =
            "UPDATE tb_blog SET liked = GREATEST(CAST(liked AS SIGNED) + ?, 0) WHERE id = ?";

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Resource
    private TransactionTemplate transactionTemplate;

    @Resource
    private BizMetrics bizMetrics;

    @Scheduled(fixedDelayString = "${hmdp.like.flush-interval-ms:5000}")
    public void flush() {
        RLock lock = redissonClient.getLock(JOB_LOCK_KEY);
        if (!lock.tryLock()) {
            return;
        }
        try {
            Long taken = stringRedisTemplate.execute(TAKE_SCRIPT,
                    List.of(BLOG_LIKE_DELTA_KEY, BLOG_LIKE_DELTA_FLUSHING_KEY));
            if (taken == null || taken == 0L) {
                return;
            }
            Map<Object, Object> deltas = stringRedisTemplate.opsForHash().entries(BLOG_LIKE_DELTA_FLUSHING_KEY);
            List<Object[]> args = new ArrayList<>(deltas.size());
            for (Map.Entry<Object, Object> e : deltas.entrySet()) {
                long delta = Long.parseLong(e.getValue().toString());
                if (delta != 0) {
                    args.add(new Object[]{delta, Long.valueOf(e.getKey().toString())});
                }
            }
            if (!args.isEmpty()) {
                // 整批一个事务：要么全部写入、之后删除 flushing，要么全部回滚、下一轮重试，不会只写一半
                transactionTemplate.executeWithoutResult(status -> {
                    for (int from = 0; from < args.size(); from += BATCH_SIZE) {
                        jdbcTemplate.batchUpdate(UPDATE_SQL, args.subList(from, Math.min(args.size(), from + BATCH_SIZE)));
                    }
                });
            }
            stringRedisTemplate.delete(BLOG_LIKE_DELTA_FLUSHING_KEY);
            bizMetrics.likeFlushed(args.size());
            log.debug("点赞数回写完成，更新 {} 篇笔记", args.size());
        } catch (Exception e) {
            bizMetrics.likeFlushFailed();
            log.error("点赞数回写失败，增量保留在 {}，下一轮重试", BLOG_LIKE_DELTA_FLUSHING_KEY, e);
        } finally {
            lock.unlock();
        }
    }
}
