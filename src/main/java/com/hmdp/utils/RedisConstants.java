package com.hmdp.utils;

public class RedisConstants {
    public static final String LOGIN_CODE_KEY = "login:code:";
    public static final Long LOGIN_CODE_TTL = 2L;
    public static final String LOGIN_USER_KEY = "login:token:";
    public static final Long LOGIN_USER_TTL = 36000L;
    /** 登录态 hash 中记录上次续期时间的字段；距上次续期超过该间隔才再次 EXPIRE，避免每个请求都写 Redis */
    public static final String LOGIN_REFRESH_AT_FIELD = "refreshAt";
    public static final long LOGIN_REFRESH_INTERVAL_MS = 30 * 60 * 1000L;
    /** 验证码发送限流：同一手机号 60s 内 1 次，同一 IP 每小时 20 次 */
    public static final String LOGIN_CODE_LIMIT_PHONE_KEY = "limit:code:phone:";
    public static final String LOGIN_CODE_LIMIT_IP_KEY = "limit:code:ip:";

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
    /** 关注集合（Redis Set），关注/取关时同步维护 */
    public static final String FOLLOWS_KEY = "follows:";
    /** 热门笔记分页缓存 */
    public static final String CACHE_HOT_BLOG_KEY = "cache:blog:hot:";
    public static final Long CACHE_HOT_BLOG_TTL_SECONDS = 60L;
    /** 粉丝收件箱最多保留条数 */
    public static final int FEED_MAX_SIZE = 1000;
    /** 秒杀订单状态短缓存，供前端轮询 */
    public static final String ORDER_STATUS_KEY = "order:status:";
    public static final Long ORDER_STATUS_TTL_MINUTES = 5L;
    /** 秒杀活动时间（epoch 毫秒），由 SeckillStockInitializer 预热，seckill.lua 校验 */
    public static final String SECKILL_TIME_KEY = "seckill:time:";
}
