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
            Boolean exists = stringRedisTemplate.hasKey(key);
            if (Boolean.FALSE.equals(exists)) {
                stringRedisTemplate.opsForValue().set(key, String.valueOf(v.getStock()));
                initCount++;
            }
        }
        log.info("秒杀库存预热完成：有效秒杀券 {} 张，初始化 {} 张到 Redis", vouchers.size(), initCount);
    }
}
