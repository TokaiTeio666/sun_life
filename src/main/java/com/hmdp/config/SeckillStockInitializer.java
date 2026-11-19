package com.hmdp.config;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.utils.RedisConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;
import java.time.LocalDateTime;
import java.util.List;

/**
 * 秒杀库存预热器
 *
 * 应用启动时扫描"未过期"的秒杀券，若 Redis 中不存在对应库存 key，则按数据库库存初始化，
 * 保证 Docker 一键启动后秒杀接口立即可用（无需手动 redis-cli SET）。
 *
 * 幂等性：已有 key 不覆盖——重启不会重置已经扣减的库存，避免"重启即回满"。
 */
@Slf4j
@Component
public class SeckillStockInitializer implements CommandLineRunner {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public void run(String... args) {
        List<SeckillVoucher> vouchers = seckillVoucherService.list(
                new LambdaQueryWrapper<SeckillVoucher>()
                        .gt(SeckillVoucher::getEndTime, LocalDateTime.now())
        );
        int initCount = 0;
        for (SeckillVoucher v : vouchers) {
            String key = RedisConstants.SECKILL_STOCK_KEY + v.getVoucherId();
            // SETNX：多实例同时启动也只会有一个写入，且不会覆盖已扣减的库存
            if (Boolean.TRUE.equals(stringRedisTemplate.opsForValue().setIfAbsent(key, String.valueOf(v.getStock())))) {
                initCount++;
            }
            // 活动时间每次启动都同步（幂等覆盖），供 seckill.lua 校验
            cacheSeckillTime(stringRedisTemplate, v.getVoucherId(), v.getBeginTime(), v.getEndTime());
        }
        log.info("秒杀库存预热完成：有效秒杀券 {} 张，初始化 {} 张到 Redis", vouchers.size(), initCount);
    }

    /** 写入秒杀活动时间（epoch 毫秒） */
    public static void cacheSeckillTime(StringRedisTemplate template, Long voucherId,
                                        LocalDateTime begin, LocalDateTime end) {
        if (begin == null || end == null) {
            return;
        }
        java.time.ZoneId zone = java.time.ZoneId.systemDefault();
        template.opsForHash().putAll(RedisConstants.SECKILL_TIME_KEY + voucherId, java.util.Map.of(
                "begin", String.valueOf(begin.atZone(zone).toInstant().toEpochMilli()),
                "end", String.valueOf(end.atZone(zone).toInstant().toEpochMilli())));
    }
}
