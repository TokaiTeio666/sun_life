package com.hmdp.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.Map;

/**
 * 业务自定义指标（阶段 3-1 监控体系）。
 *
 * <p>指标名统一以 {@code hmdp.} 开头，Prometheus 中点号转下划线、计数器加 {@code _total}，
 * 例如 {@code hmdp.seckill.requests} → {@code hmdp_seckill_requests_total}。
 * 标签取值都是有限枚举，不带 userId / orderId 等高基数字段。</p>
 *
 * <p>命名注意：不要以 {@code created} 结尾（OpenMetrics 保留后缀，会被丢弃），
 * 也不要给 DistributionSummary 设 baseUnit（会被追加到指标名里）。</p>
 */
@Component
public class BizMetrics {

    /** 秒杀 Lua 的返回结果，对应 seckill.lua 的 0–4 */
    public enum SeckillResult {
        SUCCESS("success"), SOLD_OUT("sold_out"), DUPLICATE("duplicate"),
        NOT_STARTED("not_started"), ENDED("ended");

        private final String tag;

        SeckillResult(String tag) {
            this.tag = tag;
        }

        public static SeckillResult ofLuaCode(int code) {
            return switch (code) {
                case 0 -> SUCCESS;
                case 2 -> DUPLICATE;
                case 3 -> NOT_STARTED;
                case 4 -> ENDED;
                default -> SOLD_OUT;
            };
        }
    }

    /** 过载保护拒绝请求的原因 */
    public enum OverloadReason {
        /** 回源 DB 并发闸没有许可，立即拒绝 */
        DB_GATE("db_gate"),
        /** 等 DB 连接超时（Hikari connection-timeout） */
        DB_CONN_TIMEOUT("db_conn_timeout");

        private final String tag;

        OverloadReason(String tag) {
            this.tag = tag;
        }
    }

    private final Map<OverloadReason, Counter> overloadRejected = new EnumMap<>(OverloadReason.class);
    private final Map<SeckillResult, Counter> seckillRequests = new EnumMap<>(SeckillResult.class);
    private final Counter sendFailureRollbackOk;
    private final Counter sendFailureRollbackFailed;
    private final Counter ordersCreatedBatch;
    private final Counter ordersCreatedSingle;
    private final Counter consumeDuplicates;
    private final Counter consumeBatchFallback;
    private final Counter dbStockShort;
    private final Counter deadLetters;
    private final Counter timeoutClosed;
    private final Counter feedFanoutPush;
    private final Counter feedFanoutPull;
    private final Counter feedPushedEntries;
    private final Counter feedPublishFailures;
    private final Counter feedFanoutFailures;
    private final DistributionSummary consumeBatchSize;
    private final DistributionSummary ordersPerStockUpdate;

    public BizMetrics(MeterRegistry registry) {
        for (SeckillResult r : SeckillResult.values()) {
            seckillRequests.put(r, Counter.builder("hmdp.seckill.requests")
                    .description("秒杀请求按 Lua 结果计数：成功 / 售罄 / 重复下单 / 未开始 / 已结束")
                    .tag("result", r.tag)
                    .register(registry));
        }
        for (OverloadReason r : OverloadReason.values()) {
            overloadRejected.put(r, Counter.builder("hmdp.overload.rejected")
                    .description("过载保护返回 503 的请求数（reason=db_gate 并发闸拒绝；db_conn_timeout 等 DB 连接超时）")
                    .tag("reason", r.tag)
                    .register(registry));
        }
        sendFailureRollbackOk = Counter.builder("hmdp.seckill.send.failures")
                .description("订单消息投递 Kafka 失败次数（rollback=ok 已回滚 Redis 库存；failed 需人工对账）")
                .tag("rollback", "ok").register(registry);
        sendFailureRollbackFailed = Counter.builder("hmdp.seckill.send.failures")
                .tag("rollback", "failed").register(registry);
        ordersCreatedBatch = Counter.builder("hmdp.seckill.orders.persisted")
                .description("消费端实际落库的秒杀订单数（path=batch 批量快速路径；single 逐条降级路径）")
                .tag("path", "batch").register(registry);
        ordersCreatedSingle = Counter.builder("hmdp.seckill.orders.persisted")
                .tag("path", "single").register(registry);
        consumeDuplicates = Counter.builder("hmdp.seckill.consume.duplicates")
                .description("重复投递被主键幂等拦截的订单消息数").register(registry);
        consumeBatchFallback = Counter.builder("hmdp.seckill.consume.batch.fallback")
                .description("批量落库失败、降级为逐条处理的批次数").register(registry);
        dbStockShort = Counter.builder("hmdp.seckill.db.stock.short")
                .description("DB 库存不足而跳过的订单数（Redis 与 DB 库存不一致，需对账）").register(registry);
        deadLetters = Counter.builder("hmdp.seckill.dead.letters")
                .description("进入死信主题的秒杀订单消息数").register(registry);
        timeoutClosed = Counter.builder("hmdp.order.timeout.closed")
                .description("超时未支付被关闭的订单数").register(registry);
        feedFanoutPush = Counter.builder("hmdp.feed.fanout")
                .description("笔记 Feed 分发次数（mode=push 推送到粉丝收件箱；pull 大 V 不推送，由粉丝读取时拉取）")
                .tag("mode", "push").register(registry);
        feedFanoutPull = Counter.builder("hmdp.feed.fanout")
                .tag("mode", "pull").register(registry);
        feedPushedEntries = Counter.builder("hmdp.feed.pushed.entries")
                .description("推送写入粉丝收件箱的条数（= 被推送的粉丝数之和）").register(registry);
        feedPublishFailures = Counter.builder("hmdp.feed.publish.failures")
                .description("笔记发布消息投递 Kafka 失败次数（笔记已落库，但不会推送给粉丝）").register(registry);
        feedFanoutFailures = Counter.builder("hmdp.feed.fanout.failures")
                .description("Feed 分发重试耗尽后被跳过的消息数").register(registry);
        consumeBatchSize = DistributionSummary.builder("hmdp.seckill.consume.batch.size")
                .description("每次 poll 交给批量监听器的消息条数")
                .serviceLevelObjectives(1, 5, 10, 25, 50, 100, 200)
                .register(registry);
        ordersPerStockUpdate = DistributionSummary.builder("hmdp.seckill.stock.update.orders")
                .description("每条库存 UPDATE 合并的订单数（批量落库聚合度；count 即热点行 UPDATE 次数）")
                .serviceLevelObjectives(1, 5, 10, 25, 50, 100, 200)
                .register(registry);
    }

    public void overloadRejected(OverloadReason reason) {
        overloadRejected.get(reason).increment();
    }

    public void feedFanout(boolean pull, long pushedEntries) {
        (pull ? feedFanoutPull : feedFanoutPush).increment();
        if (pushedEntries > 0) {
            feedPushedEntries.increment(pushedEntries);
        }
    }

    public void feedPublishFailure() {
        feedPublishFailures.increment();
    }

    public void feedFanoutFailed() {
        feedFanoutFailures.increment();
    }

    public void seckillRequest(int luaCode) {
        seckillRequests.get(SeckillResult.ofLuaCode(luaCode)).increment();
    }

    public void sendFailure(boolean rollbackOk) {
        (rollbackOk ? sendFailureRollbackOk : sendFailureRollbackFailed).increment();
    }

    public void consumeBatch(int size) {
        consumeBatchSize.record(size);
    }

    public void batchFallback() {
        consumeBatchFallback.increment();
    }

    /** 批量路径：每张券一条 UPDATE，ordersOfVoucher 为该券本批落库订单数 */
    public void batchOrdersCreated(int ordersOfVoucher) {
        ordersCreatedBatch.increment(ordersOfVoucher);
        ordersPerStockUpdate.record(ordersOfVoucher);
    }

    /** 逐条路径：一单一条 UPDATE */
    public void singleOrderCreated() {
        ordersCreatedSingle.increment();
        ordersPerStockUpdate.record(1);
    }

    /** 重复投递：批量路径为「本批消息数 − 实际新建数」，逐条路径为主键冲突 */
    public void duplicatesConsumed(int n) {
        if (n > 0) {
            consumeDuplicates.increment(n);
        }
    }

    public void dbStockShort() {
        dbStockShort.increment();
    }

    public void deadLetter() {
        deadLetters.increment();
    }

    public void timeoutClosed(int n) {
        if (n > 0) {
            timeoutClosed.increment(n);
        }
    }
}
