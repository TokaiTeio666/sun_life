package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    /** 请求线程上的回源查询都经过并发闸；后台的逻辑过期重建不经过（它自己有 4 线程的上限） */
    private final DbGate dbGate;

    public CacheClient(StringRedisTemplate stringRedisTemplate, DbGate dbGate) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.dbGate = dbGate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        // TTL 加 ±10% 随机扰动，避免同一批 key 同时过期造成缓存雪崩
        long ttlSeconds = jitteredSeconds(time, unit);
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), ttlSeconds, TimeUnit.SECONDS);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        long ttlSeconds = jitteredSeconds(time, unit);
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(ttlSeconds));
        //写入Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /** 把 TTL 换算成秒并叠加 ±10% 抖动（至少 ±1 秒）。 */
    private long jitteredSeconds(Long time, TimeUnit unit) {
        long seconds = Math.max(1, unit.toSeconds(time));
        long delta = Math.max(1, seconds / 10);
        return seconds - delta + ThreadLocalRandom.current().nextLong(delta * 2 + 1);
    }

    /**
     * 缓存击穿
     * @param keyPrefix
     * @param id
     * @param type
     * @param dbFallback
     * @param time
     * @param unit
     * @return
     * @param <R>
     * @param <ID>
     */
    public <R,ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback,Long time, TimeUnit unit) {
        String key = keyPrefix + id;

        // 1. 从 Redis 查询店铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);

        // 2. 判断是否存在
        if (StrUtil.isNotBlank(json)) {
            // 3. 存在，直接返回
            return JSONUtil.toBean(json, type);
        }

        // 判断数据库中是否存在
        if (json != null) {
            //返回一个错误信息
            return null;
        }

        // 4. 不存在，根据 id 查询数据库
        R r = dbGate.call(() -> dbFallback.apply(id));

        // 5. 判断数据库中是否存在
        if (r == null) {
            // 6. 数据库不存在，将空值写入 Redis
            stringRedisTemplate.opsForValue().set(
                    key,
                    "",
                    jitteredSeconds(RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES),
                    TimeUnit.SECONDS
            );

            return null;
        }

        // 7. 数据库存在，写入 Redis
        this.set(key, r, time, unit);

        // 8. 返回店铺
        return r;
    }

    /** 锁持有者标识：每次加锁生成唯一值，解锁时比对，避免误删其他线程/实例的锁 */
    private static final org.springframework.data.redis.core.script.DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static {
        UNLOCK_SCRIPT = new org.springframework.data.redis.core.script.DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new org.springframework.core.io.ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    private boolean tryLock(String key, String owner){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, owner, 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key, String owner){
        stringRedisTemplate.execute(UNLOCK_SCRIPT, java.util.Collections.singletonList(key), owner);
    }

    /**
     * 逻辑过期的异步重建线程池：有界队列 + 丢弃策略（重建任务丢了无妨，下个请求会再触发），
     * 守护线程不阻塞 JVM 退出。替代原来无界队列的 newFixedThreadPool。
     */
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = new java.util.concurrent.ThreadPoolExecutor(
            2, 4, 60, TimeUnit.SECONDS,
            new java.util.concurrent.ArrayBlockingQueue<>(200),
            r -> {
                Thread t = new Thread(r, "cache-rebuild");
                t.setDaemon(true);
                return t;
            },
            new java.util.concurrent.ThreadPoolExecutor.DiscardPolicy());

    /** 互斥锁等待重试次数与间隔：最多等待约 1s，超时直接回源，避免无限递归 */
    private static final int MUTEX_MAX_RETRIES = 20;
    private static final long MUTEX_RETRY_SLEEP_MS = 50;

    /**
     * 逻辑过期
     * @param keyPerfix
     * @param id
     * @param type
     * @param dbFallback
     * @param time
     * @param unit
     * @return
     * @param <R>
     * @param <ID>
     */
    public <R,ID> R queryWithLogicalExpire(String keyPerfix, ID id, Class<R> type, Function<ID,R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPerfix + id;
        //1. 从redis查询商城缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2. 判断是否存在
        if (StrUtil.isBlank(json)) {
            //3. 存在，直接返回
            return null;
        }

        //4. 命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json,RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();

        //5. 判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            //5.1 未过期，直接返回店铺信息
            return r;
        }
        //5.2 已过期，需要缓存重建
        //6.1 获取互斥锁
        String lockKey = "lock:" + keyPerfix + id;
        String owner = java.util.UUID.randomUUID().toString();
        boolean isLock = tryLock(lockKey, owner);

        //6.2 判断是否获取成功
        if (isLock) {
            //6.3 成功，开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    //重建缓存
                    R rl = dbFallback.apply(id);
                    //写入redis
                    this.setWithLogicalExpire(key, rl, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unLock(lockKey, owner);
                }
            });
        }

        //6.4 返回过期店铺信息
        return r;
    }

    /**
     * 互斥锁
     * @param keyPerfix
     * @param id
     * @param type
     * @param dbFallback
     * @param time
     * @param unit
     * @return
     * @param <R>
     * @param <ID>
     */
    public <R,ID> R queryWithMutex(String keyPerfix, ID id, Class<R> type, Function<ID,R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPerfix + id;
        String lockKey = "lock:" + keyPerfix + id;
        for (int attempt = 0; attempt <= MUTEX_MAX_RETRIES; attempt++) {
            //1. 查缓存：命中真实值直接返回；命中空值说明不存在
            String json = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(json)) {
                return JSONUtil.toBean(json, type);
            }
            if (json != null) {
                return null;
            }

            //2. 未命中：只有拿到锁的一个请求回源 DB，其余请求短暂等待后重查缓存
            String owner = java.util.UUID.randomUUID().toString();
            if (!tryLock(lockKey, owner)) {
                try {
                    Thread.sleep(MUTEX_RETRY_SLEEP_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                continue;
            }
            try {
                //3. 双重检查：等锁期间其他请求可能已经重建好缓存
                json = stringRedisTemplate.opsForValue().get(key);
                if (StrUtil.isNotBlank(json)) {
                    return JSONUtil.toBean(json, type);
                }
                //4. 回源 DB 并写缓存（空值也缓存，防穿透）；并发闸满时抛 SystemBusyException，finally 里照常释放锁
                R r = dbGate.call(() -> dbFallback.apply(id));
                if (r == null) {
                    stringRedisTemplate.opsForValue().set(key, "",
                            jitteredSeconds(RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES), TimeUnit.SECONDS);
                    return null;
                }
                this.set(key, r, time, unit);
                return r;
            } finally {
                //5. 只释放自己持有的锁
                unLock(lockKey, owner);
            }
        }
        //等锁超时（重建方异常或极慢）：直接回源，保证可用性
        log.warn("互斥锁等待超时，直接回源 key={}", key);
        return dbGate.call(() -> dbFallback.apply(id));
    }
}
