package com.hmdp.utils;

public class RedisConstants {
    public static final String LOGIN_CODE_KEY = "login:code:";
    public static final Long LOGIN_CODE_TTL = 2L;
    public static final String LOGIN_USER_KEY = "login:token:";
    public static final Long LOGIN_USER_TTL = 36000L;

    public static final Long CACHE_NULL_TTL = 2L;

    public static final Long CACHE_SHOP_TTL = 30L;
    public static final String CACHE_SHOP_KEY = "cache:shop:";

    public static final String LOCK_SHOP_KEY = "lock:shop:";
    public static final Long LOCK_SHOP_TTL = 10L;

    public static final String SECKILL_STOCK_KEY = "seckill:stock:";
    public static final String SECKILL_ORDER_KEY = "seckill:order:";
    /** 点赞关系位图：blogId 维度的 userId 位偏移，O(1) 判断是否点赞 */
    public static final String BLOG_LIKED_BIT_KEY = "blog:liked:bit:";
    /** 点赞时间有序集合，仅保留前 N 个用于「点赞用户 TopN」展示 */
    public static final String BLOG_LIKED_TOP_KEY = "blog:liked:top:";
    public static final int BLOG_LIKED_TOP_LIMIT = 1000;
    /** 笔记日 UV：HyperLogLog，标准误差约 0.81% */
    public static final String BLOG_UV_KEY = "uv:blog:";
    public static final String FEED_KEY = "feed:";
    public static final String SHOP_GEO_KEY = "shop:geo:";
    public static final String USER_SIGN_KEY = "sign:";
}
