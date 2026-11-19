package com.hmdp.metrics;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.utils.RedisConstants;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * 秒杀库存对账采样：定时比较 Redis 库存与 DB 库存，导出为 Prometheus 指标。
 *
 * <ul>
 *   <li>{@code hmdp_seckill_stock{voucher, source="redis"|"db"}}：两侧当前库存；</li>
 *   <li>{@code hmdp_seckill_stock_gap{voucher}} = DB 库存 − Redis 库存。</li>
 * </ul>
 *
 * <p>Redis 先扣、DB 在 Kafka 消费后才扣，所以 gap 正常情况下 ≥ 0，等于「在途订单数」，
 * Kafka lag 清零后应收敛到 0。<b>gap &lt; 0</b> 说明 DB 扣得比 Redis 多（超卖风险）；
 * <b>lag 为 0 时 gap 仍 &gt; 0</b> 说明有订单丢失（如 Lua 已扣、消息未发出）。两者都要告警。</p>
 *
 * <p>只看最近一天内仍有效的秒杀券，且 Redis 中存在库存 key 的券。每个实例都会采样，
 * 指标值相同，看板与告警里用 {@code max by (voucher)} 去重。</p>
 */
@Slf4j
@Component
public class SeckillStockGapSampler {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private final MultiGauge stockGauge;
    private final MultiGauge gapGauge;

    public SeckillStockGapSampler(MeterRegistry registry) {
        this.stockGauge = MultiGauge.builder("hmdp.seckill.stock")
                .description("秒杀券当前库存（source=redis / db）")
                .register(registry);
        this.gapGauge = MultiGauge.builder("hmdp.seckill.stock.gap")
                .description("DB 库存 − Redis 库存：正常为在途订单数并收敛到 0；负数表示超卖风险")
                .register(registry);
    }

    @Scheduled(initialDelay = 10_000, fixedDelayString = "${hmdp.metrics.stock-gap-scan-ms:15000}")
    public void sample() {
        try {
            List<SeckillVoucher> vouchers = seckillVoucherService.list(new LambdaQueryWrapper<SeckillVoucher>()
                    .select(SeckillVoucher::getVoucherId, SeckillVoucher::getStock)
                    .gt(SeckillVoucher::getEndTime, LocalDateTime.now().minusDays(1)));
            if (vouchers.isEmpty()) {
                stockGauge.register(List.of(), true);
                gapGauge.register(List.of(), true);
                return;
            }
            List<String> keys = vouchers.stream()
                    .map(v -> RedisConstants.SECKILL_STOCK_KEY + v.getVoucherId())
                    .toList();
            List<String> redisStocks = stringRedisTemplate.opsForValue().multiGet(keys);

            List<MultiGauge.Row<?>> stockRows = new ArrayList<>();
            List<MultiGauge.Row<?>> gapRows = new ArrayList<>();
            for (int i = 0; i < vouchers.size(); i++) {
                String redisValue = redisStocks == null ? null : redisStocks.get(i);
                Integer dbStock = vouchers.get(i).getStock();
                if (redisValue == null || dbStock == null) {
                    continue;
                }
                long redisStock = Long.parseLong(redisValue);
                String voucher = String.valueOf(vouchers.get(i).getVoucherId());
                stockRows.add(MultiGauge.Row.of(Tags.of("voucher", voucher, "source", "redis"), redisStock));
                stockRows.add(MultiGauge.Row.of(Tags.of("voucher", voucher, "source", "db"), dbStock));
                gapRows.add(MultiGauge.Row.of(Tags.of("voucher", voucher), dbStock - redisStock));
            }
            // overwrite=true：已结束的券对应的时间序列会被移除
            stockGauge.register(stockRows, true);
            gapGauge.register(gapRows, true);
        } catch (Exception e) {
            log.warn("秒杀库存对账采样失败: {}", e.toString());
        }
    }
}
