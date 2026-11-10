package com.hmdp.utils;

import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;

/**
 * 基于 Redis BitMap 的布隆过滤器。
 *
 * <p>用于商铺详情查询前的存在性预判：不存在的 id 直接拒绝，避免请求穿透到 MySQL。
 * 位数组和哈希函数个数按预期插入量与误判率自动计算（无需引入 Guava，且多实例共享同一份位图）。</p>
 *
 * <p>布隆过滤器只保证「不存在的一定不存在」，删除操作无法安全支持；
 * 商铺通常只增不删，新增时调用 {@link #add(long)} 补充。</p>
 */
@Component
public class RedisBloomFilter {

    private static final String KEY = "bloom:shop";
    private static final int EXPECTED_INSERTIONS = 10_000;
    private static final double FALSE_POSITIVE_RATE = 0.01;

    private final StringRedisTemplate redis;
    private final long numBits;
    private final int numHashFunctions;
    private final byte[] keyBytes = KEY.getBytes(StandardCharsets.UTF_8);

    public RedisBloomFilter(StringRedisTemplate redis) {
        this.redis = redis;
        this.numBits = optimalNumOfBits(EXPECTED_INSERTIONS, FALSE_POSITIVE_RATE);
        this.numHashFunctions = optimalNumOfHashFunctions(EXPECTED_INSERTIONS, numBits);
    }

    /** 预估容量与误判率对应的位数组长度：m = -n*ln(p) / (ln2)^2 */
    private static long optimalNumOfBits(int n, double p) {
        return (long) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    /** 最优哈希函数个数：k = m/n * ln2 */
    private static int optimalNumOfHashFunctions(int n, long m) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    public void add(long value) {
        long[] offsets = offsets(value);
        redis.executePipelined((RedisCallback<Object>) connection -> {
            for (long offset : offsets) {
                connection.stringCommands().setBit(keyBytes, offset, true);
            }
            return null;
        });
    }

    /** 批量写入，启动预热时一次 pipeline 完成，避免逐条往返。 */
    public void addAll(Collection<Long> values) {
        if (values == null || values.isEmpty()) {
            return;
        }
        redis.executePipelined((RedisCallback<Object>) connection -> {
            for (Long value : values) {
                for (long offset : offsets(value)) {
                    connection.stringCommands().setBit(keyBytes, offset, true);
                }
            }
            return null;
        });
    }

    /** 返回 false 表示该 id 一定不存在；返回 true 表示可能存在（存在误判）。 */
    public boolean mightContain(long value) {
        long[] offsets = offsets(value);
        List<Object> bits = redis.executePipelined((RedisCallback<Object>) connection -> {
            for (long offset : offsets) {
                connection.stringCommands().getBit(keyBytes, offset);
            }
            return null;
        });
        // Redis 不可用等异常场景宁可放行，避免布隆过滤器成为单点故障
        if (bits == null || bits.size() != offsets.length) {
            return true;
        }
        for (Object bit : bits) {
            if (!isSet(bit)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isSet(Object bit) {
        if (bit instanceof Boolean b) {
            return b;
        }
        return bit instanceof Number n && n.longValue() == 1L;
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
