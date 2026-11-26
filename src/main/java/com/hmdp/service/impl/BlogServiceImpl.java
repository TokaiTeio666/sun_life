package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.FeedService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.LocalCache;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_BIT_KEY;
import static com.hmdp.utils.RedisConstants.BLOG_LIKE_DELTA_KEY;
import static com.hmdp.utils.RedisConstants.BLOG_LIKED_TOP_KEY;
import static com.hmdp.utils.RedisConstants.BLOG_LIKED_TOP_LIMIT;
import static com.hmdp.utils.RedisConstants.BLOG_UV_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_BLOG_SUMMARY_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_HOT_BLOG_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_HOT_BLOG_TTL_SECONDS;

/**
 * 探店笔记服务。
 *
 * <p>性能要点：列表类接口批量查询作者（1 次 IN 查询）+ pipeline 批量判断点赞（1 次往返），
 * 消除逐条 N+1；热门列表短 TTL 缓存；点赞切换用 Lua 原子完成；Feed 的分发与读取在 FeedService。</p>
 */
@Slf4j
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    /**
     * 原子切换点赞状态：返回切换前的状态（0 = 原来未点赞，本次变为已点赞；1 = 原来已点赞，本次取消）。
     * KEYS[1] = 位图 key，KEYS[2] = TopN ZSet key，KEYS[3] = 待回写的点赞数增量 hash；
     * ARGV[1] = userId，ARGV[2] = 当前毫秒，ARGV[3] = TopN 上限，ARGV[4] = blogId，ARGV[5] = 是否记录增量（1 / 0）
     *
     * <p>ARGV[5] = 1 时把 ±1 累加到增量 hash 的 blogId 字段，由 LikeCountFlusher 定时批量写回 tb_blog.liked；
     * 点赞又取消、增量归零的字段直接删掉，不用白写一次库。</p>
     */
    private static final DefaultRedisScript<Long> TOGGLE_LIKE_SCRIPT = new DefaultRedisScript<>(
            "local old = redis.call('getbit', KEYS[1], ARGV[1]) " +
            "local delta " +
            "if old == 0 then " +
            "  redis.call('setbit', KEYS[1], ARGV[1], 1) " +
            "  redis.call('zadd', KEYS[2], ARGV[2], ARGV[1]) " +
            "  redis.call('zremrangebyrank', KEYS[2], tonumber(ARGV[3]), -1) " +
            "  delta = 1 " +
            "else " +
            "  redis.call('setbit', KEYS[1], ARGV[1], 0) " +
            "  redis.call('zrem', KEYS[2], ARGV[1]) " +
            "  delta = -1 " +
            "end " +
            "if ARGV[5] == '1' then " +
            "  if redis.call('hincrby', KEYS[3], ARGV[4], delta) == 0 then " +
            "    redis.call('hdel', KEYS[3], ARGV[4]) " +
            "  end " +
            "end " +
            "return old", Long.class);

    /** 点赞数异步回写：true 时点赞接口只写 Redis；false 回到每次点赞更新一次 tb_blog */
    @Value("${hmdp.like.async-flush:true}")
    private boolean likeAsyncFlush;

    /** 列表接口不含正文时查询的列（前端列表页不显示正文，详情页单独取） */
    private static final String[] LIST_COLUMNS = {
            "id", "shop_id", "user_id", "title", "images", "liked", "comments", "create_time", "update_time"};

    /** 列表是否返回正文；false 时只查 LIST_COLUMNS */
    @Value("${hmdp.blog.list-content:false}")
    private boolean listContent;

    /** Feed 笔记摘要的 Redis 缓存秒数；<= 0 关闭摘要缓存（本地与 Redis 两级都不用） */
    @Value("${hmdp.blog.summary-cache.redis-ttl-seconds:60}")
    private long summaryRedisTtlSeconds;

    @Value("${hmdp.blog.summary-cache.local-ttl-seconds:10}")
    private long summaryLocalTtlSeconds;

    @Value("${hmdp.blog.summary-cache.local-max-size:50000}")
    private long summaryLocalMaxSize;

    /**
     * Feed 笔记摘要的本地缓存（笔记 id → 已填好作者信息的 Blog，不含 isLike）。
     * 不放进通用的 LocalCache：摘要条数多，会把店铺详情挤出去。缓存里的对象在请求之间共享，返回前必须复制。
     */
    private Cache<Long, Blog> summaryLocal;

    /** 已确认存在的笔记 id（笔记没有删除接口，可以缓存较久）：异步回写时点赞接口用它判断笔记是否存在，不必每次查库 */
    private final Cache<Long, Boolean> existingBlogIds = Caffeine.newBuilder()
            .maximumSize(200_000)
            .expireAfterWrite(Duration.ofMinutes(10))
            .build();

    @Resource
    private IUserService userService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private FeedService feedService;

    @Resource
    private LocalCache localCache;

    @Resource
    private ObjectMapper objectMapper;

    @PostConstruct
    void initSummaryCache() {
        if (summaryRedisTtlSeconds > 0 && summaryLocalTtlSeconds > 0 && summaryLocalMaxSize > 0) {
            summaryLocal = Caffeine.newBuilder()
                    .expireAfterWrite(Duration.ofSeconds(summaryLocalTtlSeconds))
                    .maximumSize(summaryLocalMaxSize)
                    .build();
        }
        log.info("笔记列表：正文 {}；Feed 摘要缓存 {}", listContent ? "返回" : "不返回",
                summaryRedisTtlSeconds <= 0 ? "关闭（每次查库）"
                        : "Redis " + summaryRedisTtlSeconds + "s"
                        + (summaryLocal == null ? "，本地关闭" : "，本地 " + summaryLocalTtlSeconds + "s / " + summaryLocalMaxSize + " 条"));
    }

    @Override
    public Result queryBlogById(Long id) {
        //1. 查询blog
        Blog blog = getById(id);
        if(blog == null){
            return Result.fail("笔记不存在！");
        }
        //2. 作者信息 + 是否点赞
        fillBlogUsers(List.of(blog));
        fillBlogLiked(List.of(blog));
        //3. 记录日 UV（HyperLogLog）
        recordBlogUv(id);
        return Result.ok(blog);
    }

    /**
     * 热门笔记：按页缓存「笔记 + 作者信息」60s（±10% 抖动），是否点赞因人而异，读缓存后再单独批量判断。
     * 用 LIMIT 直接取页，不再执行分页插件的 COUNT 查询（前端只用列表）。
     */
    @Override
    public Result queryHotBlog(Integer current) {
        int page = current == null || current < 1 ? 1 : current;
        String key = CACHE_HOT_BLOG_KEY + page;
        // 第一级：本地短缓存；第二级：Redis（60s）；都没有才查库
        List<Blog> shared = localCache.get(key);
        List<Blog> records = shared != null ? shared : readBlogCache(key);
        if (records == null) {
            int offset = (page - 1) * SystemConstants.MAX_PAGE_SIZE;
            records = listQuery()
                    .orderByDesc("liked")
                    .last("LIMIT " + SystemConstants.MAX_PAGE_SIZE + " OFFSET " + offset)
                    .list();
            fillBlogUsers(records);
            writeBlogCache(key, records);
        }
        if (shared == null) {
            localCache.put(key, records);
        }
        // 本地缓存里的对象在请求之间共享，而 isLike 因人而异：复制一份再填，不能改缓存里的对象
        records = records.stream().map(BlogServiceImpl::copyOf).collect(Collectors.toList());
        fillBlogLiked(records);
        return Result.ok(records);
    }

    @Override
    public Result likeBlog(Long id) {
        Long userId = UserHolder.getUser().getId();
        String bitKey = BLOG_LIKED_BIT_KEY + id;
        String topKey = BLOG_LIKED_TOP_KEY + id;
        List<String> keys = List.of(bitKey, topKey, BLOG_LIKE_DELTA_KEY);
        if (likeAsyncFlush) {
            //0. 异步回写：先确认笔记存在（本地缓存，未命中才查一次主键），再只写 Redis；点赞数由定时任务批量写回 DB
            if (!blogExists(id)) {
                return Result.fail("笔记不存在！");
            }
            stringRedisTemplate.execute(TOGGLE_LIKE_SCRIPT, keys,
                    userId.toString(), String.valueOf(System.currentTimeMillis()), String.valueOf(BLOG_LIKED_TOP_LIMIT),
                    id.toString(), "1");
            return Result.ok();
        }
        //1. Redis 原子切换点赞状态（避免 GETBIT + SETBIT 之间并发重复点赞，一次往返完成 bit + TopN）
        Long old = stringRedisTemplate.execute(TOGGLE_LIKE_SCRIPT, keys,
                userId.toString(), String.valueOf(System.currentTimeMillis()), String.valueOf(BLOG_LIKED_TOP_LIMIT),
                id.toString(), "0");
        boolean nowLiked = old != null && old == 0L;
        //2. 只有状态确实变化时才更新 DB 点赞数
        boolean isSuccess = update()
                .setSql(nowLiked ? "liked = liked + 1" : "liked = liked - 1")
                .eq("id", id)
                .update();
        if (!isSuccess) {
            // 笔记不存在：把 Redis 状态切回去
            stringRedisTemplate.execute(TOGGLE_LIKE_SCRIPT, keys,
                    userId.toString(), String.valueOf(System.currentTimeMillis()), String.valueOf(BLOG_LIKED_TOP_LIMIT),
                    id.toString(), "0");
            return Result.fail("笔记不存在！");
        }
        return Result.ok();
    }

    private boolean blogExists(Long id) {
        if (existingBlogIds.getIfPresent(id) != null) {
            return true;
        }
        // 不存在的 id 不缓存：防止随机 id 把缓存挤满
        boolean exists = query().select("id").eq("id", id).count() > 0;
        if (exists) {
            existingBlogIds.put(id, Boolean.TRUE);
        }
        return exists;
    }

    @Override
    public Result queryBlogByLikes(Long id) {
        //查询top5的点赞用户 zrange key 0 4
        String key = BLOG_LIKED_TOP_KEY + id;
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        if (top5 == null || top5.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        //解析出期中的用户id
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        //根据用户id查询用户，再按点赞先后（ids 的顺序）排列；在内存里排，不依赖 MySQL 的 ORDER BY FIELD
        Map<Long, User> userById = userService.listByIds(ids).stream()
                .collect(Collectors.toMap(User::getId, Function.identity()));
        List<UserDTO> userDTOS = ids.stream()
                .map(userById::get)
                .filter(java.util.Objects::nonNull)
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(userDTOS);
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 1.获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 2.保存探店笔记
        boolean isSuccess = save(blog);
        if(!isSuccess){
            return Result.fail("新增笔记失败!");
        }
        // 3.分发到粉丝的 Feed：只写发件箱 + 发一条消息，由消费者异步推送（见 FeedService），耗时与粉丝数无关。
        //   笔记已经落库，分发出错不影响发布结果
        try {
            feedService.publish(blog.getId(), user.getId());
        } catch (Exception e) {
            log.error("笔记 Feed 分发失败 blogId={} userId={}", blog.getId(), user.getId(), e);
        }
        // 4.返回id
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        // 1.获取当前用户
        Long userId = UserHolder.getUser().getId();
        // 2.滚动读取一页：收件箱 + 所关注大 V 的发件箱，合并去重（见 FeedService）
        FeedService.FeedPage page = feedService.read(userId, max, offset == null ? 0 : offset,
                SystemConstants.DEFAULT_PAGE_SIZE);
        // 3.非空判断
        if (page == null) {
            return Result.ok();
        }
        // 4.按 id 取笔记摘要（含作者，走摘要缓存），再批量补充点赞状态
        List<Blog> blogs = loadBlogSummaries(page.blogIds());
        fillBlogLiked(blogs);

        // 5.封装并返回
        ScrollResult r = new ScrollResult();
        r.setList(blogs);
        r.setOffset(page.offset());
        r.setMinTime(page.minTime());

        return Result.ok(r);
    }

    /**
     * 记录笔记日 UV：HyperLogLog 按天聚合，固定约 12KB/键，标准误差约 0.81%。
     * PFADD + EXPIRE 用 pipeline 一次往返。
     */
    private void recordBlogUv(Long id) {
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return;
        }
        String key = BLOG_UV_KEY + id + ":" + today();
        String uid = user.getId().toString();
        stringRedisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            StringRedisConnection conn = (StringRedisConnection) connection;
            conn.pfAdd(key, uid);
            // UV 只保留 35 天，过期自动清理
            conn.expire(key, TimeUnit.DAYS.toSeconds(35));
            return null;
        });
    }

    @Override
    public Result queryBlogUv(Long id, String date) {
        String day = StrUtil.isBlank(date) ? today() : date;
        if (!day.matches("\\d{8}")) {
            return Result.fail("日期格式应为 yyyyMMdd");
        }
        Long uv = stringRedisTemplate.opsForHyperLogLog().size(BLOG_UV_KEY + id + ":" + day);
        return Result.ok(uv == null ? 0L : uv);
    }

    private String today() {
        return LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);
    }

    /** 列表查询：不返回正文时只查 LIST_COLUMNS */
    private QueryChainWrapper<Blog> listQuery() {
        return listContent ? query() : query().select(LIST_COLUMNS);
    }

    /**
     * 按 id 批量取笔记摘要（笔记 + 作者昵称头像，不含 isLike），结果按 ids 的顺序排列，不存在的 id 跳过。
     * 本地缓存 → Redis 一次 MGET → 只查缺的那几篇（tb_blog 一次 IN + tb_user 一次 IN），查到的回填两级缓存。
     * 返回的是副本，调用方可以直接设置 isLike。
     */
    private List<Blog> loadBlogSummaries(List<Long> ids) {
        if (ids.isEmpty()) {
            return new ArrayList<>();
        }
        Map<Long, Blog> found = new HashMap<>(ids.size() * 2);
        List<Long> missing = new ArrayList<>(ids.size());
        for (Long id : ids) {
            Blog cached = summaryLocal == null ? null : summaryLocal.getIfPresent(id);
            if (cached != null) {
                found.put(id, cached);
            } else {
                missing.add(id);
            }
        }
        if (!missing.isEmpty() && summaryRedisTtlSeconds > 0) {
            missing = readSummaryCache(missing, found);
        }
        if (!missing.isEmpty()) {
            List<Blog> fromDb = listQuery().in("id", missing).list();
            fillBlogUsers(fromDb);
            for (Blog blog : fromDb) {
                found.put(blog.getId(), blog);
            }
            if (summaryRedisTtlSeconds > 0) {
                writeSummaryCache(fromDb);
            }
        }
        List<Blog> result = new ArrayList<>(ids.size());
        for (Long id : ids) {
            Blog blog = found.get(id);
            if (blog != null) {
                result.add(copyOf(blog));
            }
        }
        return result;
    }

    /** Redis MGET 读摘要，命中的放进 found 并回填本地缓存，返回仍未命中的 id。Redis 出错时全部按未命中处理（回源 DB）。 */
    private List<Long> readSummaryCache(List<Long> ids, Map<Long, Blog> found) {
        List<String> jsons;
        try {
            jsons = stringRedisTemplate.opsForValue().multiGet(
                    ids.stream().map(id -> CACHE_BLOG_SUMMARY_KEY + id).collect(Collectors.toList()));
        } catch (Exception e) {
            log.warn("笔记摘要缓存读取失败，回源 DB", e);
            return ids;
        }
        List<Long> missing = new ArrayList<>();
        for (int i = 0; i < ids.size(); i++) {
            String json = jsons == null ? null : jsons.get(i);
            Blog blog = null;
            if (json != null) {
                try {
                    blog = objectMapper.readValue(json, Blog.class);
                } catch (Exception e) {
                    log.warn("笔记摘要缓存反序列化失败，回源 DB id={}", ids.get(i), e);
                }
            }
            if (blog != null) {
                found.put(ids.get(i), blog);
                if (summaryLocal != null) {
                    summaryLocal.put(ids.get(i), blog);
                }
            } else {
                missing.add(ids.get(i));
            }
        }
        return missing;
    }

    /** 回填两级缓存：Redis 用一个 pipeline 批量 SET EX，TTL ±10% 抖动。写失败只记日志，下次再回源。 */
    private void writeSummaryCache(List<Blog> blogs) {
        if (blogs.isEmpty()) {
            return;
        }
        if (summaryLocal != null) {
            for (Blog blog : blogs) {
                summaryLocal.put(blog.getId(), blog);
            }
        }
        try {
            long base = summaryRedisTtlSeconds;
            List<String[]> entries = new ArrayList<>(blogs.size());
            for (Blog blog : blogs) {
                entries.add(new String[]{CACHE_BLOG_SUMMARY_KEY + blog.getId(), objectMapper.writeValueAsString(blog)});
            }
            stringRedisTemplate.executePipelined((RedisCallback<Object>) connection -> {
                StringRedisConnection conn = (StringRedisConnection) connection;
                for (String[] e : entries) {
                    conn.setEx(e[0], base + RandomUtil.randomLong(-base / 10, base / 10 + 1), e[1]);
                }
                return null;
            });
        } catch (Exception e) {
            log.warn("笔记摘要缓存写入失败", e);
        }
    }

    /**
     * 复制一份笔记（缓存里的对象在请求之间共享，isLike 因人而异，填之前先复制）。
     * 手写字段拷贝，代替按反射逐字段复制的 BeanUtil.copyProperties；Blog 增加字段时这里要同步。
     */
    private static Blog copyOf(Blog b) {
        return new Blog()
                .setId(b.getId())
                .setShopId(b.getShopId())
                .setUserId(b.getUserId())
                .setIcon(b.getIcon())
                .setName(b.getName())
                .setIsLike(b.getIsLike())
                .setTitle(b.getTitle())
                .setImages(b.getImages())
                .setContent(b.getContent())
                .setLiked(b.getLiked())
                .setComments(b.getComments())
                .setCreateTime(b.getCreateTime())
                .setUpdateTime(b.getUpdateTime());
    }

    /** 批量补充作者昵称与头像：一次 IN 查询替代逐条 getById */
    private void fillBlogUsers(List<Blog> blogs) {
        if (blogs.isEmpty()) {
            return;
        }
        Set<Long> userIds = blogs.stream().map(Blog::getUserId).collect(Collectors.toSet());
        Map<Long, User> users = userService.query()
                .select("id", "nick_name", "icon")
                .in("id", userIds)
                .list()
                .stream()
                .collect(Collectors.toMap(User::getId, Function.identity()));
        for (Blog blog : blogs) {
            User user = users.get(blog.getUserId());
            if (user != null) {
                blog.setName(user.getNickName());
                blog.setIcon(user.getIcon());
            }
        }
    }

    /** 批量判断当前用户是否点赞：pipeline 一次往返完成 N 个 GETBIT */
    private void fillBlogLiked(List<Blog> blogs) {
        UserDTO user = UserHolder.getUser();
        if (user == null || blogs.isEmpty()) {
            return;
        }
        long userId = user.getId();
        List<Object> results = stringRedisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            StringRedisConnection conn = (StringRedisConnection) connection;
            for (Blog blog : blogs) {
                conn.getBit(BLOG_LIKED_BIT_KEY + blog.getId(), userId);
            }
            return null;
        });
        for (int i = 0; i < blogs.size(); i++) {
            blogs.get(i).setIsLike(Boolean.TRUE.equals(results.get(i)));
        }
    }

    private List<Blog> readBlogCache(String key) {
        String json = stringRedisTemplate.opsForValue().get(key);
        if (json == null) {
            return null;
        }
        try {
            return objectMapper.readValue(json, new TypeReference<List<Blog>>() {});
        } catch (Exception e) {
            log.warn("热门笔记缓存反序列化失败，回源 DB key={}", key, e);
            return null;
        }
    }

    private void writeBlogCache(String key, List<Blog> blogs) {
        try {
            // TTL ±10% 抖动，避免各页同时失效
            long base = CACHE_HOT_BLOG_TTL_SECONDS;
            long ttl = base + RandomUtil.randomLong(-base / 10, base / 10 + 1);
            stringRedisTemplate.opsForValue().set(key, objectMapper.writeValueAsString(blogs), ttl, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("热门笔记缓存写入失败 key={}", key, e);
        }
    }
}
