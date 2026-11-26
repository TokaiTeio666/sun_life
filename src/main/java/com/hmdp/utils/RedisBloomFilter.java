package com.hmdp.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.connection.BitFieldSubCommands.BitFieldType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 基于 Redis BitMap 的布隆过滤器。
 *
 * <p>用于商铺详情查询前的存在性预判：不存在的 id 直接拒绝，避免请求穿透到数据库。
 * 位数组和哈希函数个数按预期插入量与误判率自动计算（无需引入 Guava，且多实例共享同一份位图）。</p>
 *
 * <p>布隆过滤器只保证「不存在的一定不存在」，删除操作无法安全支持；
 * 商铺通常只增不删，新增时调用 {@link #add(long)} 补充。</p>
 *
 * <p>一个 id 对应的 k 个位用一条 {@code BITFIELD} 命令读写（每个位是一个 u1 子命令），
 * 而不是 k 条 GETBIT / SETBIT：Redis 每个请求只执行 1 条命令。</p>
 *
 * <p><b>容量可配置</b>（{@code hmdp.bloom.expected-insertions} / {@code hmdp.bloom.fpp}）。位图长度 m 和哈希个数 k
 * 决定了每个 id 落在哪些位上，参数一变旧位图就不能再用，所以 key 里带上这两个值（{@code bloom:shop:{m}:{k}}）：
 * 改容量后新副本写新 key，滚动发布期间还在运行的旧副本继续读旧 key，互不影响。旧 key 由
 * {@link #retireOtherKeys()} 设 1 天过期。</p>
 *
 * <p><b>就绪标记</b>：新 key 在预热完成前是空的，这时如果照常判断，所有店铺都会被当成不存在。
 * 所以预热完成（{@link #setReady(boolean)}）之前 {@link #mightContain(long)} 一律放行，由空值缓存兜底。</p>
 */
@Slf4j
@Component
public class RedisBloomFilter {

    /** 改造前的固定 key（容量 1 万），仍可能被未升级的副本使用 */
    private static final String LEGACY_KEY = "bloom:shop";
    private static final String KEY_PREFIX = "bloom:shop:";
    /** 记录用过的位图 key，容量变化后据此找到旧 key（避免 SCAN 整个库） */
    private static final String REGISTRY_KEY = "bloom:shop:keys";
    private static final long RETIRED_KEY_TTL_HOURS = 24;
    /** Redis 字符串最大 512MB = 2^32 位 */
    private static final long MAX_BITS = 1L << 32;

    private final StringRedisTemplate redis;
    private final long expectedInsertions;
    private final long numBits;
    private final int numHashFunctions;
    private final String key;
    private final byte[] keyBytes;

    private volatile boolean ready;

    public RedisBloomFilter(StringRedisTemplate redis,
                            @Value("${hmdp.bloom.expected-insertions:1000000}") long expectedInsertions,
                            @Value("${hmdp.bloom.fpp:0.01}") double fpp) {
        if (expectedInsertions <= 0 || fpp <= 0 || fpp >= 1) {
            throw new IllegalArgumentException("hmdp.bloom.expected-insertions 必须为正数，hmdp.bloom.fpp 必须在 (0, 1) 之间");
        }
        this.redis = redis;
        this.expectedInsertions = expectedInsertions;
        this.numBits = optimalNumOfBits(expectedInsertions, fpp);
        if (numBits >= MAX_BITS) {
            throw new IllegalArgumentException("布隆过滤器位图需要 " + numBits + " 位，超过 Redis 单个字符串 512MB 的上限");
        }
        this.numHashFunctions = optimalNumOfHashFunctions(expectedInsertions, numBits);
        this.key = KEY_PREFIX + numBits + ":" + numHashFunctions;
        this.keyBytes = key.getBytes(StandardCharsets.UTF_8);
    }

    /** 预估容量与误判率对应的位数组长度：m = -n*ln(p) / (ln2)^2 */
    private static long optimalNumOfBits(long n, double p) {
        return Math.max(64, (long) (-n * Math.log(p) / (Math.log(2) * Math.log(2))));
    }

    /** 最优哈希函数个数：k = m/n * ln2 */
    private static int optimalNumOfHashFunctions(long n, long m) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    public void add(long value) {
        redis.opsForValue().bitField(key, setCommands(offsets(value)));
    }

    /** 批量写入：一批 id 一次 pipeline。调用方负责控制每批大小。 */
    public void addAll(Collection<Long> values) {
        if (values == null || values.isEmpty()) {
            return;
        }
        redis.executePipelined((RedisCallback<Object>) connection -> {
            for (Long value : values) {
                connection.stringCommands().bitField(keyBytes, setCommands(offsets(value)));
            }
            return null;
        });
    }

    /** 返回 false 表示该 id 一定不存在；返回 true 表示可能存在（存在误判，或过滤器尚未预热完成）。 */
    public boolean mightContain(long value) {
        if (!ready) {
            return true;
        }
        long[] offsets = offsets(value);
        BitFieldSubCommands commands = BitFieldSubCommands.create();
        for (long offset : offsets) {
            commands = commands.get(BitFieldType.unsigned(1)).valueAt(offset);
        }
        List<Long> bits = redis.opsForValue().bitField(key, commands);
        // 返回结果异常时宁可放行，避免布隆过滤器把存在的商铺挡掉
        if (bits == null || bits.size() != offsets.length) {
            return true;
        }
        for (Long bit : bits) {
            if (bit == null || bit != 1L) {
                return false;
            }
        }
        return true;
    }

    public boolean isReady() {
        return ready;
    }

    /** 预热完成后置为 true；位图丢失（被淘汰 / 清库）需要重建时置回 false。 */
    public void setReady(boolean ready) {
        this.ready = ready;
    }

    /** 位图 key 是否存在。写入过至少一个 id 后应当一直存在，不存在说明被淘汰、过期或清库。 */
    public boolean exists() {
        return Boolean.TRUE.equals(redis.hasKey(key));
    }

    /** 实际填充率：已置 1 的位数 ÷ 位图长度。按设计容量写满时约为 50%。 */
    public double fillRatio() {
        Long ones = redis.execute((RedisCallback<Long>) connection -> connection.stringCommands().bitCount(keyBytes));
        return ones == null ? 0 : (double) ones / numBits;
    }

    /** 按当前填充率估算的误判率：k 个位都恰好为 1 的概率。 */
    public double estimatedFpp(double fillRatio) {
        return Math.pow(fillRatio, numHashFunctions);
    }

    /**
     * 登记当前 key，并让其他参数的旧位图在 1 天后过期（滚动发布、回滚的时间窗口内旧副本还要用）。
     * 当前 key 如果之前被别的配置标记过期（改了容量又改回来），取消过期。
     */
    public void retireOtherKeys() {
        redis.opsForSet().add(REGISTRY_KEY, key);
        redis.persist(key);
        Set<String> others = new HashSet<>();
        Set<String> registered = redis.opsForSet().members(REGISTRY_KEY);
        if (registered != null) {
            others.addAll(registered);
        }
        others.add(LEGACY_KEY);
        others.remove(key);
        for (String other : others) {
            Long ttl = redis.getExpire(other);
            if (ttl == null || ttl == -2) {
                // 已经不存在：从登记表里移除
                redis.opsForSet().remove(REGISTRY_KEY, other);
            } else if (ttl == -1) {
                redis.expire(other, RETIRED_KEY_TTL_HOURS, TimeUnit.HOURS);
                log.info("布隆过滤器参数已变化，旧位图 {} 将在 {} 小时后过期", other, RETIRED_KEY_TTL_HOURS);
            }
        }
    }

    public String key() {
        return key;
    }

    public long expectedInsertions() {
        return expectedInsertions;
    }

    public long numBits() {
        return numBits;
    }

    public int numHashFunctions() {
        return numHashFunctions;
    }

    private static BitFieldSubCommands setCommands(long[] offsets) {
        BitFieldSubCommands commands = BitFieldSubCommands.create();
        for (long offset : offsets) {
            commands = commands.set(BitFieldType.unsigned(1)).valueAt(offset).to(1);
        }
        return commands;
    }

    /**
     * 双哈希：把 64 位 id 映射成两个独立哈希值，第 i 个位置为 (h1 + i*h2) mod m。
     */
    private long[] offsets(long value) {
        byte[] data = new byte[8];
        long v = value;
        for (int i = 7; i >= 0; i--) {
            data[i] = (byte) (v & 0xff);
            v >>>= 8;
        }
        long h1 = fnv1a(data, 0x9E3779B97F4A7C15L);
        long h2 = fnv1a(data, 0xC2B2AE3D27D4EB4FL);
        if (h2 == 0) {
            h2 = 0x165667B19E3779F9L;
        }
        long[] offsets = new long[numHashFunctions];
        for (int i = 0; i < numHashFunctions; i++) {
            offsets[i] = Math.floorMod(h1 + (long) i * h2, numBits);
        }
        return offsets;
    }

    private static long fnv1a(byte[] data, long seed) {
        long hash = 0xcbf29ce484222325L ^ seed;
        for (byte b : data) {
            hash ^= (b & 0xffL);
            hash *= 0x100000001b3L;
        }
        return hash;
    }
}
