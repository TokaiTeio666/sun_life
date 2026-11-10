package com.hmdp.config;

import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import com.hmdp.utils.RedisBloomFilter;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 启动时把已有商铺 id 预热进布隆过滤器，避免上线初期全部请求穿透到数据库。
 */
@Slf4j
@Component
public class BloomFilterInitializer implements ApplicationRunner {

    @Resource
    private IShopService shopService;

    @Resource
    private RedisBloomFilter bloomFilter;

    @Override
    public void run(ApplicationArguments args) {
        try {
            List<Shop> shops = shopService.query().select("id").list();
            List<Long> ids = shops.stream().map(Shop::getId).toList();
            bloomFilter.addAll(ids);
            log.info("布隆过滤器预热完成，商铺数量={}", ids.size());
        } catch (Exception e) {
            // 预热失败不影响启动；后续 DB 命中的商铺会按需补写
            log.warn("布隆过滤器预热失败，将在缓存未命中时按需补写", e);
        }
    }
}
