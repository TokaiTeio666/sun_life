package com.hmdp.utils;

import com.hmdp.metrics.BizMetrics;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.Semaphore;
import java.util.function.Supplier;

/**
 * 回源 DB 的并发闸（过载保护）。
 *
 * <p>虚拟线程下 Tomcat 不再有 200 线程的上限，DB 变慢或缓存大面积失效时，所有请求都会堆到 Hikari
 * 连接池上排队，一起等到超时再失败。这里用信号量限制「正在查库 + 正在等连接」的请求总数，
 * 拿不到许可的请求立即失败（HTTP 503），不进入排队。</p>
 *
 * <p>许可数默认 = 连接池大小 × 2：一半在执行，另一半在等连接，排队的请求最多等一轮查询的时间。
 * 配置 {@code hmdp.db-gate.permits}（环境变量 {@code DB_GATE_PERMITS}）：
 * 负数 = 按连接池大小自动计算；0 = 关闭并发闸；正数 = 指定许可数。</p>
 *
 * <p>只用于请求线程上的回源查询。Kafka 消费者、定时任务不经过这里，它们的写库不能被丢弃。
 * 这是 Sentinel 限流熔断 / 线程池隔离的简化版，接入 Sentinel 后可以替换。</p>
 */
@Slf4j
@Component
public class DbGate {

    private static final SystemBusyException BUSY = new SystemBusyException("回源 DB 并发已达上限");

    /** null 表示并发闸关闭 */
    private final Semaphore semaphore;
    private final int permits;
    private final BizMetrics bizMetrics;

    public DbGate(@Value("${hmdp.db-gate.permits:-1}") int configuredPermits,
                  @Value("${spring.datasource.hikari.maximum-pool-size:10}") int poolSize,
                  BizMetrics bizMetrics,
                  MeterRegistry registry) {
        this.permits = configuredPermits < 0 ? poolSize * 2 : configuredPermits;
        this.semaphore = permits > 0 ? new Semaphore(permits) : null;
        this.bizMetrics = bizMetrics;
        Gauge.builder("hmdp.db.gate.active", this, DbGate::active)
                .description("并发闸内正在回源 DB 的请求数（含等待连接的）；闸关闭时恒为 0")
                .register(registry);
        Gauge.builder("hmdp.db.gate.permits", this, g -> g.permits)
                .description("并发闸许可数；0 表示关闭")
                .register(registry);
        if (semaphore == null) {
            log.warn("回源 DB 并发闸已关闭（DB_GATE_PERMITS=0），过载时请求会在连接池上排队");
        } else {
            log.info("回源 DB 并发闸：许可数 {}（连接池 {}）", permits, poolSize);
        }
    }

    /**
     * 在并发闸内执行一次回源查询。
     *
     * @throws SystemBusyException 并发已达上限，本次请求被拒绝
     */
    public <T> T call(Supplier<T> dbQuery) {
        if (semaphore == null) {
            return dbQuery.get();
        }
        if (!semaphore.tryAcquire()) {
            bizMetrics.overloadRejected(BizMetrics.OverloadReason.DB_GATE);
            throw BUSY;
        }
        try {
            return dbQuery.get();
        } finally {
            semaphore.release();
        }
    }

    private double active() {
        return semaphore == null ? 0 : permits - semaphore.availablePermits();
    }
}
