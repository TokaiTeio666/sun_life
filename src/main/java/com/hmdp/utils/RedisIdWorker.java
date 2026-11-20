package com.hmdp.utils;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 全局唯一 id 生成器（订单号）。
 *
 * <p>id 是 63 位正整数：高 31 位是相对 {@link #BEGIN_TIMESTAMP} 的秒数，低 32 位是序列部分。两种生成方式共用这个布局：</p>
 * <pre>
 *   Redis 模式（改造前）：低 32 位 = 当天的 Redis INCR 计数          → 每个 id 一次 Redis 往返
 *   本地模式（默认）    ：低 32 位 = 0 | 1 | 机器号 10 位 | 序列 20 位  → 不访问 Redis
 *                                   ↑   ↑
 *                             bit31=0  bit30=1（本地生成标记）
 * </pre>
 * <p>bit30 的标记让两种 id 不会相同（Redis 计数要一天超过 2^30 ≈ 10 亿才会碰到这一位），所以两种模式可以随时切换、
 * 滚动发布时可以混用；bit31 留给压测造数（seed_data.py 的 ORDER_SEED_FLAG）。高位仍然是秒数，id 的时间顺序和以前一样。</p>
 *
 * <p><b>机器号</b>：启动时在 Redis 里抢占一个 0–1023 的号（{@code idworker:lease:{n}}，60s 过期），每 20s 续约一次，
 * 正常停机时释放。续约失败或租约即将到期时（Redis 不可用、号被别人占了）本地模式自动停用，
 * 退回 Redis 模式，重新抢到号后恢复——宁可多一次往返，也不在不确定机器号是否唯一时发号。</p>
 *
 * <p><b>时钟回拨</b>：序列的时间只增不减（时钟回拨时继续用上一次的秒数）；一秒内 2^20 个序列用完时借用下一秒。
 * 续约时把用到的最大秒数记在 Redis（{@code idworker:floor:{n}}），下一个拿到同一机器号的实例从它之后开始，
 * 避免重启前后时钟回拨造成重复。</p>
 *
 * <p>开关：{@code hmdp.id.mode}（环境变量 {@code ORDER_ID_MODE}）= {@code local}（默认）/ {@code redis}。</p>
 */
@Slf4j
@Component
public class RedisIdWorker {
    /**
     * 开始时间戳
     */
    private static final long BEGIN_TIMESTAMP = 1788307200L;
    /**
     * 序列号的位数
     */
    private static final long COUNT_BITS = 32L;

    /** 本地生成标记：低 32 位的 bit30 */
    private static final long LOCAL_FLAG = 1L << 30;
    private static final int WORKER_BITS = 10;
    private static final int SEQ_BITS = 20;
    private static final int MAX_WORKERS = 1 << WORKER_BITS;
    private static final long MAX_SEQ = (1L << SEQ_BITS) - 1;

    private static final String LEASE_KEY = "idworker:lease:";
    private static final String FLOOR_KEY = "idworker:floor:";
    private static final long LEASE_TTL_MS = 60_000;
    /** 本地认定的有效期比 Redis 里的过期时间短这么多，保证租约在 Redis 过期之前本机就已停止发号 */
    private static final long LEASE_SAFETY_MS = 10_000;
    /** 续约间隔内可能发出、但还没记进 floor 的秒数上限（续约间隔 20s + 余量） */
    private static final long FLOOR_MARGIN_SECONDS = 30;

    /** 仍是自己持有时续约并记录已用到的最大秒数；返回 1 成功，0 表示租约已不属于自己 */
    private static final DefaultRedisScript<Long> RENEW_SCRIPT = new DefaultRedisScript<>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then " +
            "  redis.call('pexpire', KEYS[1], ARGV[2]) " +
            "  redis.call('set', KEYS[2], ARGV[3], 'EX', 86400) " +
            "  return 1 " +
            "end " +
            "return 0", Long.class);
    private static final DefaultRedisScript<Long> RELEASE_SCRIPT = new DefaultRedisScript<>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then " +
            "  redis.call('set', KEYS[2], ARGV[2], 'EX', 86400) " +
            "  return redis.call('del', KEYS[1]) " +
            "end " +
            "return 0", Long.class);

    private final StringRedisTemplate stringRedisTemplate;
    private final boolean localMode;
    /** 本实例的租约持有者标识 */
    private final String owner = UUID.randomUUID().toString();

    /** -1 表示当前没有机器号 */
    private volatile int workerId = -1;
    /** 本地认定的租约有效期（毫秒时间戳），过了就不能再用本地模式发号 */
    private volatile long leaseValidUntilMs;
    private long lastSecond;
    private long seq;

    public RedisIdWorker(StringRedisTemplate stringRedisTemplate,
                         @Value("${hmdp.id.mode:local}") String mode,
                         MeterRegistry registry) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.localMode = "local".equalsIgnoreCase(mode);
        Gauge.builder("hmdp.id.worker", this, w -> w.workerId)
                .description("本地发号的机器号；-1 表示没有机器号（Redis 模式，或租约丢失后的回退）")
                .register(registry);
    }

    public long nextId(String keyPrefix) {
        if (localMode) {
            long id = nextLocalId();
            if (id >= 0) {
                return id;
            }
        }
        return nextRedisId(keyPrefix);
    }

    // ---------------------------------------------------------------- Redis 模式（改造前的实现）

    private long nextRedisId(String keyPrefix) {
        //1. 生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowSecond - BEGIN_TIMESTAMP;

        //2. 生成序列号
        //2.1 获取当前日期，精确到天
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        //2.2 自增长
        long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);

        //3. 拼接并返回
        return timestamp << COUNT_BITS | count;
    }

    // ---------------------------------------------------------------- 本地模式

    /** 与 Redis 模式用同一种方式取秒数，两种 id 的高位才可比 */
    private static long currentSecond() {
        return LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
    }

    /** 返回 -1 表示当前不能本地发号（没有机器号或租约即将到期）。临界区内没有阻塞操作。 */
    private synchronized long nextLocalId() {
        int worker = workerId;
        if (worker < 0 || System.currentTimeMillis() >= leaseValidUntilMs) {
            return -1;
        }
        long now = currentSecond();
        if (now > lastSecond) {
            lastSecond = now;
            seq = 0;
        } else if (++seq > MAX_SEQ) {
            // 这一秒的序列用完（或时钟回拨后一直停在 lastSecond）：借用下一秒
            lastSecond++;
            seq = 0;
        }
        return (lastSecond - BEGIN_TIMESTAMP) << COUNT_BITS | LOCAL_FLAG | ((long) worker << SEQ_BITS) | seq;
    }

    private synchronized long lastSecondUsed() {
        return lastSecond;
    }

    private synchronized void raiseFloor(long second) {
        if (second > lastSecond) {
            lastSecond = second;
            seq = 0;
        }
    }

    @PostConstruct
    void init() {
        if (!localMode) {
            log.info("订单号生成：Redis 模式（每个 id 一次 INCR）");
            return;
        }
        try {
            acquire();
        } catch (Exception e) {
            log.warn("订单号机器号抢占失败，暂时退回 Redis 模式，稍后重试", e);
        }
    }

    /** 抢占一个空闲的机器号：从随机位置开始逐个尝试 SET NX。 */
    private void acquire() {
        int start = ThreadLocalRandom.current().nextInt(MAX_WORKERS);
        for (int i = 0; i < MAX_WORKERS; i++) {
            int candidate = (start + i) % MAX_WORKERS;
            long before = System.currentTimeMillis();
            Boolean ok = stringRedisTemplate.opsForValue()
                    .setIfAbsent(LEASE_KEY + candidate, owner, Duration.ofMillis(LEASE_TTL_MS));
            if (!Boolean.TRUE.equals(ok)) {
                continue;
            }
            // 上一个用这个号的实例可能用到过的最大秒数：从它之后开始，防止重启前后时钟回拨
            String floor = stringRedisTemplate.opsForValue().get(FLOOR_KEY + candidate);
            if (floor != null) {
                raiseFloor(Long.parseLong(floor) + FLOOR_MARGIN_SECONDS);
            }
            leaseValidUntilMs = before + LEASE_TTL_MS - LEASE_SAFETY_MS;
            workerId = candidate;
            log.info("订单号生成：本地模式，机器号 {}", candidate);
            return;
        }
        log.error("订单号机器号已全部被占用（{} 个），退回 Redis 模式", MAX_WORKERS);
    }

    @Scheduled(fixedDelay = 20_000, initialDelay = 20_000)
    public void renewLease() {
        if (!localMode) {
            return;
        }
        try {
            int worker = workerId;
            if (worker < 0) {
                acquire();
                return;
            }
            long before = System.currentTimeMillis();
            Long renewed = stringRedisTemplate.execute(RENEW_SCRIPT,
                    List.of(LEASE_KEY + worker, FLOOR_KEY + worker),
                    owner, String.valueOf(LEASE_TTL_MS), String.valueOf(lastSecondUsed()));
            if (renewed != null && renewed == 1L) {
                leaseValidUntilMs = before + LEASE_TTL_MS - LEASE_SAFETY_MS;
            } else {
                log.warn("订单号机器号 {} 的租约已丢失，退回 Redis 模式并重新抢占", worker);
                workerId = -1;
                acquire();
            }
        } catch (Exception e) {
            // Redis 不可用：不动 leaseValidUntilMs，到期后 nextLocalId 自动停用本地模式
            log.warn("订单号机器号续约失败，租约到期前恢复则不受影响", e);
        }
    }

    /**
     * 停机时释放机器号。监听 ContextClosedEvent 而不是用 @PreDestroy：Spring 在销毁 bean 之前会先停掉
     * Redis 连接工厂，那时已经发不出命令了。释放之后还在处理的请求走 Redis 模式。
     */
    @EventListener(ContextClosedEvent.class)
    public void release() {
        int worker = workerId;
        if (worker < 0) {
            return;
        }
        workerId = -1;
        try {
            stringRedisTemplate.execute(RELEASE_SCRIPT, List.of(LEASE_KEY + worker, FLOOR_KEY + worker),
                    owner, String.valueOf(lastSecondUsed()));
        } catch (Exception e) {
            log.warn("订单号机器号 {} 释放失败，60s 后自动过期", worker, e);
        }
    }
}
