package com.hmdp;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class MutiLock {

    @Resource
    private RedissonClient redissonClient;

    @Resource
    private RedissonClient redissonClient2;

    @Resource
    private RedissonClient redissonClient3;

    private RLock lock;

    @BeforeEach
    void setUp() {
        // 三个锁分别来自三个独立的 Redis 实例
        RLock lock1 = redissonClient.getLock("order");
        RLock lock2 = redissonClient2.getLock("order");
        RLock lock3 = redissonClient3.getLock("order");

        // 创建联锁：必须同时在三个实例上加锁成功才算获取成功
        lock = redissonClient.getMultiLock(lock1, lock2, lock3);
    }

    /**
     * 测试1：联锁基本加锁与释放
     * 加锁成功后，可在 6379/6380/6381 三个实例上同时看到锁 key
     */
    @Test
    void testLockAndUnlock() throws InterruptedException {
        // 最多等待1秒，持有10秒后自动释放
        boolean isLock = lock.tryLock(1, 10, TimeUnit.SECONDS);
        assertTrue(isLock, "联锁获取失败");

        System.out.println("=== 联锁获取成功 ===");
        System.out.println("此时在 6379/6380/6381 上执行 keys order，三个实例都应存在锁");

        lock.unlock();
        System.out.println("=== 联锁已释放 ===");
    }

    /**
     * 测试2：联锁的互斥性
     * 主线程持有联锁时，子线程无法获取
     */
    @Test
    void testMutex() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        final boolean[] subGotLock = {false};

        // 主线程先获取联锁，持有30秒
        boolean mainLock = lock.tryLock(1, 30, TimeUnit.SECONDS);
        assertTrue(mainLock, "主线程获取联锁失败");

        // 子线程尝试获取，最多等待2秒，预期失败
        Thread sub = new Thread(() -> {
            try {
                subGotLock[0] = lock.tryLock(2, 5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                latch.countDown();
            }
        });
        sub.start();

        latch.await();
        assertFalse(subGotLock[0], "主线程持有联锁时，子线程不应获取成功");

        lock.unlock();
        System.out.println("互斥性测试通过：一个线程持有联锁时，其他线程无法获取");
    }

    /**
     * 测试3：联锁的可重入性
     * 同一线程可以多次获取同一个联锁
     */
    @Test
    void testReentrant() {
        boolean first = lock.tryLock();
        assertTrue(first, "第一次加锁失败");

        boolean second = lock.tryLock();
        assertTrue(second, "第二次重入加锁失败");

        // 加了两次锁，必须释放两次
        lock.unlock();
        lock.unlock();
        System.out.println("可重入性测试通过：同一线程可多次获取联锁");
    }
}
