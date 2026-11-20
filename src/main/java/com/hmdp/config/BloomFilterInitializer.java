package com.hmdp.config;

import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import com.hmdp.utils.RedisBloomFilter;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 把商铺 id 同步进布隆过滤器。
 *
 * <p><b>启动预热</b>：按 id 游标分页读库（每页 {@value #BATCH_SIZE} 个），每页一次 pipeline 写入。
 * 不一次读出全部 id、也不把全部命令塞进一个 pipeline：10 万家店时那样做会占用大量内存并长时间占住一条 Redis 连接。
 * 预热完成前过滤器处于未就绪状态，一律放行（见 {@link RedisBloomFilter}）。</p>
 *
 * <p><b>定时增量同步</b>：之后每隔一段时间从上次的游标继续读，把新增的商铺补进去。应用内新增商铺时已经会写过滤器，
 * 这一步覆盖的是不经过本副本的写入：滚动发布期间旧版本副本新增的商铺（它写的是旧 key）、直接写库的造数脚本。
 * 同时检查位图是否还在，被淘汰或清库后自动整体重建。</p>
 */
@Slf4j
@Component
public class BloomFilterInitializer implements ApplicationRunner {

    private static final int BATCH_SIZE = 1000;

    @Resource
    private IShopService shopService;

    @Resource
    private RedisBloomFilter bloomFilter;

    /** 已同步到的最大商铺 id */
    private long cursor;
    /** 本副本写入过滤器的商铺数（整体重建时清零） */
    private long synced;

    @Override
    public void run(ApplicationArguments args) {
        try {
            bloomFilter.retireOtherKeys();
        } catch (Exception e) {
            log.warn("布隆过滤器旧位图清理失败，不影响使用", e);
        }
        sync();
    }

    @Scheduled(initialDelayString = "${hmdp.bloom.sync-interval-ms:60000}",
            fixedDelayString = "${hmdp.bloom.sync-interval-ms:60000}")
    public void scheduledSync() {
        sync();
    }

    private synchronized void sync() {
        try {
            if (cursor > 0 && !bloomFilter.exists()) {
                log.warn("布隆过滤器位图 {} 不存在（被淘汰、过期或清库），开始整体重建", bloomFilter.key());
                bloomFilter.setReady(false);
                cursor = 0;
                synced = 0;
            }
            long added = 0;
            while (true) {
                List<Long> ids = shopService.query()
                        .select("id")
                        .gt("id", cursor)
                        .orderByAsc("id")
                        .last("LIMIT " + BATCH_SIZE)
                        .list()
                        .stream().map(Shop::getId).toList();
                if (ids.isEmpty()) {
                    break;
                }
                bloomFilter.addAll(ids);
                cursor = ids.get(ids.size() - 1);
                added += ids.size();
                if (ids.size() < BATCH_SIZE) {
                    break;
                }
            }
            synced += added;
            if (!bloomFilter.isReady()) {
                bloomFilter.setReady(true);
                report("预热完成");
            } else if (added > 0) {
                report("增量同步 " + added + " 家");
            }
        } catch (Exception e) {
            // 失败不影响启动：未就绪时过滤器一律放行，下一轮定时同步从游标处继续
            log.warn("布隆过滤器同步失败，下一轮重试（当前就绪={}）", bloomFilter.isReady(), e);
        }
    }

    private void report(String what) {
        double fill = bloomFilter.fillRatio();
        log.info("布隆过滤器{}：key={}，商铺 {} 家 / 设计容量 {}，哈希 {} 个，填充率 {}，估算误判率 {}",
                what, bloomFilter.key(), synced, bloomFilter.expectedInsertions(), bloomFilter.numHashFunctions(),
                String.format("%.2f%%", fill * 100), String.format("%.4f%%", bloomFilter.estimatedFpp(fill) * 100));
        if (synced > bloomFilter.expectedInsertions()) {
            log.warn("布隆过滤器已超过设计容量：商铺 {} 家 > {}，误判率会快速上升，请调大 hmdp.bloom.expected-insertions（BLOOM_EXPECTED_INSERTIONS）",
                    synced, bloomFilter.expectedInsertions());
        }
    }
}
