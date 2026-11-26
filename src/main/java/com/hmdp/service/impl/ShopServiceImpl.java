package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.DbGate;
import com.hmdp.utils.LocalCache;
import com.hmdp.utils.RedisBloomFilter;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    public CacheClient cacheClient;

    @Resource
    private RedisBloomFilter bloomFilter;

    @Resource
    private DbGate dbGate;

    @Resource
    private LocalCache localCache;

    /** 压测开关：false 时跳过布隆过滤器与缓存，直接查库，用于对比缓存前后 P99 */
    @Value("${hmdp.cache.enabled:true}")
    private boolean cacheEnabled;

    @Override
    public Result queryById(Long id) {
        if (!cacheEnabled) {
            // 直查 DB 同样经过并发闸：关缓存压测时这里就是全部的回源流量
            Shop fromDb = dbGate.call(() -> getById(id));
            return fromDb == null ? Result.fail("店铺不存在！") : Result.ok(fromDb);
        }

        // 第一级：本地短缓存。命中时不访问 Redis（只缓存存在的店铺；不存在的 id 由布隆过滤器拦，不占本地缓存）
        String localKey = CACHE_SHOP_KEY + id;
        Shop local = localCache.get(localKey);
        if (local != null) {
            return Result.ok(local);
        }

        // 布隆过滤器前置拦截：不存在的 id 不查 Redis、不打数据库
        if (!bloomFilter.mightContain(id)) {
            return Result.fail("店铺不存在！");
        }

        // 互斥锁防击穿（含空值缓存防穿透）：热点 key 过期时只有一个请求回源 DB
        Shop shop = cacheClient.queryWithMutex(CACHE_SHOP_KEY, id, Shop.class,
                shopId -> {
                    Shop fromDb = getById(shopId);
                    // 布隆过滤器不支持删除，新增商铺在 DB 命中后补写
                    if (fromDb != null) {
                        bloomFilter.add(shopId);
                    }
                    return fromDb;
                },
                CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //逻辑过期解决缓存击穿
        //Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        if (shop==null) {
            return Result.fail("店铺不存在！");
        }
        localCache.put(localKey, shop);
        //7. 返回
        return Result.ok(shop);
    }

    @Override
    public boolean save(Shop shop) {
        boolean saved = super.save(shop);
        if (saved && shop.getId() != null) {
            bloomFilter.add(shop.getId());
        }
        return saved;
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        if(shop.getId()==null){
            return Result.fail("店铺id不能为空");
        }
        //1. 更新数据库
        updateById(shop);
        //2. 删除缓存
        String key = CACHE_SHOP_KEY + shop.getId();
        stringRedisTemplate.delete(key);
        //3. 事务提交后再删一次 Redis，并删各副本的本地缓存：提交前删的那一次之后，
        //   并发读请求可能已经把旧值重新写回缓存
        localCache.evictAfterCommit(key, () -> stringRedisTemplate.delete(key));
        return Result.ok(shop);
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1.判断是否需要根据坐标查询
        if (x == null || y == null) {
            // 不需要坐标查询，按数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }

        // 2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 3.查询redis、按照距离排序、分页。结果：shopId、distance
        String key = SHOP_GEO_KEY + typeId;
        // GEORADIUS key x y 5000 m WITHDIST ASC COUNT end（兼容 Redis 5.x，Redis 6.2+ 的 GEOSEARCH 亦可用）
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .radius(
                        key,
                        new Circle(new Point(x, y), new Distance(5000)),
                        RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs()
                                .includeDistance()
                                .limit(end)
                                .sortAscending()
                );
        // 4.解析出id
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            // 没有下一页了，结束
            return Result.ok(Collections.emptyList());
        }
        // 4.1.截取 from ~ end的部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            // 4.2.获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            // 4.3.获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });
        // 5.根据id查询Shop
        // 按 GEO 返回的距离顺序（ids 的顺序）排列；在内存里排，不依赖 MySQL 的 ORDER BY FIELD
        Map<Long, Shop> shopById = new HashMap<>(ids.size());
        for (Shop shop : listByIds(ids)) {
            shopById.put(shop.getId(), shop);
        }
        List<Shop> shops = new ArrayList<>(ids.size());
        for (Long id : ids) {
            Shop shop = shopById.get(id);
            if (shop != null) {
                shop.setDistance(distanceMap.get(id.toString()).getValue());
                shops.add(shop);
            }
        }
        // 6.返回
        return Result.ok(shops);
    }

}
