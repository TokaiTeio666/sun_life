package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_BIT_KEY;
import static com.hmdp.utils.RedisConstants.BLOG_LIKED_TOP_KEY;
import static com.hmdp.utils.RedisConstants.BLOG_LIKED_TOP_LIMIT;
import static com.hmdp.utils.RedisConstants.BLOG_UV_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IUserService userService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IFollowService followService;

    @Override
    public Result queryBlogById(Long id) {
        //1. 查询blog
        Blog blog = getById(id);
        if(blog == null){
            return Result.fail("笔记不存在！");
        }
        //2. 查询blog有关的用户
        queryBlogUser(blog);
        //3. 查询blog是否被点赞
        isBlogLiked(blog);
        //4. 记录日 UV（HyperLogLog）
        recordBlogUv(id);
        return Result.ok(blog);
    }

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            this.queryBlogUser(blog);
            isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    private void isBlogLiked(Blog blog) {
        //1. 获取登录用户
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            //用户未登录，无需查询
            return;
        }
        Long userId = UserHolder.getUser().getId();
        //2. 判断当前登录用户是否已经点赞（BitMap：userId 作为 offset）
        String key = BLOG_LIKED_BIT_KEY + blog.getId();
        Boolean liked = stringRedisTemplate.opsForValue().getBit(key, userId);
        blog.setIsLike(Boolean.TRUE.equals(liked));
    }

    @Override
    public Result likeBlog(Long id) {
        //1. 获取登录用户
        Long userId = UserHolder.getUser().getId();
        //2. 判断当前登录用户是否已经点赞（BitMap：userId 作为 offset，1 bit/用户）
        String bitKey = BLOG_LIKED_BIT_KEY + id;
        String topKey = BLOG_LIKED_TOP_KEY + id;
        boolean liked = Boolean.TRUE.equals(stringRedisTemplate.opsForValue().getBit(bitKey, userId));

        if(!liked){
            //3. 如果未点赞，可以点赞
            //3.1 数据库点赞数+1
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            if(isSuccess){
                //3.2 位图记录点赞关系
                stringRedisTemplate.opsForValue().setBit(bitKey, userId, true);
                //3.3 有界 ZSet 只保留最早点赞的前 N 个用户，用于 TopN 展示
                stringRedisTemplate.opsForZSet().add(topKey, userId.toString(), System.currentTimeMillis());
                stringRedisTemplate.opsForZSet().removeRange(topKey, BLOG_LIKED_TOP_LIMIT, -1);
            }
        }else{
            //4.如果已点赞，取消点赞
            //4.1 数据库点赞-1
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            if(isSuccess){
                //4.2 清除位图并移出 TopN 集合
                stringRedisTemplate.opsForValue().setBit(bitKey, userId, false);
                stringRedisTemplate.opsForZSet().remove(topKey, userId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogByLikes(Long id) {
        //查询top5的点赞用户 zrange key 0 4
        String key = BLOG_LIKED_TOP_KEY + id;
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        //解析出期中的用户id
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String idStr = StrUtil.join(",", ids);
        //根据用户id查询用户
        List<UserDTO> userDTOS = userService.query()
                .in("id",ids)
                .last("ORDER BY FIELD(id,"+idStr+")").list()
                .stream()
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
        // 3.查询笔记作者的所有粉丝 select * from tb_follow where follow_user_id = ?
        List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();
        // 4.推送笔记id给所有粉丝
        for (Follow follow : follows) {
            // 4.1.获取粉丝id
            Long userId = follow.getUserId();
            // 4.2.推送
            String key = FEED_KEY + userId;
            stringRedisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());
        }
        // 5.返回id
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        // 1.获取当前用户
        Long userId = UserHolder.getUser().getId();
        // 2.查询收件箱 ZREVRANGEBYSCORE key Max Min LIMIT offset count
        String key = FEED_KEY + userId;
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        // 3.非空判断
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }
        // 4.解析数据：blogId、minTime（时间戳）、offset
        List<Long> ids = new ArrayList<>(typedTuples.size());
        long minTime = 0; // 2
        int os = 1; // 2
        for (ZSetOperations.TypedTuple<String> tuple : typedTuples) { // 5 4 4 2 2
            // 4.1.获取id
            ids.add(Long.valueOf(tuple.getValue()));
            // 4.2.获取分数(时间戳）
            long time = tuple.getScore().longValue();
            if(time == minTime){
                os++;
            }else{
                minTime = time;
                os = 1;
            }
        }
        os = minTime == max ? os : os + offset;
        // 5.根据id查询blog
        String idStr = StrUtil.join(",", ids);
        List<Blog> blogs = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();

        for (Blog blog : blogs) {
            // 5.1.查询blog有关的用户
            queryBlogUser(blog);
            // 5.2.查询blog是否被点赞
            isBlogLiked(blog);
        }

        // 6.封装并返回
        ScrollResult r = new ScrollResult();
        r.setList(blogs);
        r.setOffset(os);
        r.setMinTime(minTime);

        return Result.ok(r);
    }

    /**
     * 记录笔记日 UV：HyperLogLog 按天聚合，固定约 12KB/键，标准误差约 0.81%。
     */
    private void recordBlogUv(Long id) {
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return;
        }
        String key = BLOG_UV_KEY + id + ":" + today();
        stringRedisTemplate.opsForHyperLogLog().add(key, user.getId().toString());
        // UV 只保留 35 天，过期自动清理
        stringRedisTemplate.expire(key, 35, TimeUnit.DAYS);
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

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }


}
