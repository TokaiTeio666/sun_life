package com.hmdp.service.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;
import java.lang.annotation.Target;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList() {
        String key = "shopTypeList";
        //1. 从redis查询商城缓存
        String shopTypejson = stringRedisTemplate.opsForValue().get(key);
        //2. 判断是否存在
        if (StrUtil.isNotBlank(shopTypejson)) {
            //3. 存在，直接返回
            List<ShopType> shopTypeList = JSONUtil.toList(shopTypejson, ShopType.class);
            return Result.ok(shopTypeList);
        }
        //4. 不存在，根据id查询数据库
        List<ShopType> shopTypeList = list(); // 核心：无参调用list()查全表

        //5. 不存在，返回错误
        if (CollUtil.isEmpty(shopTypeList)) {
            return Result.fail("暂无商铺类型数据!");
        }

        //6. 存在，写入redis
        stringRedisTemplate.opsForValue().set(
                key,
                JSONUtil.toJsonStr(shopTypeList),
                12,
                TimeUnit.HOURS
                );

        //7. 返回
        return Result.ok(shopTypeList);
    }
}
