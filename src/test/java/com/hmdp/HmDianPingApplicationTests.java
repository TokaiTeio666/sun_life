package com.hmdp;

import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.service.impl.ShopTypeServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;

import javax.annotation.Resource;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private ShopServiceImpl shopServiceImpl;

    @Test
    void testSaveShop() {
        shopServiceImpl.saveShop2Redis(1L,10L);
    }

}
