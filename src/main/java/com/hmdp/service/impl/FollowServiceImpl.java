package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.FeedService;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.FOLLOWS_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IUserService userService;

    /**
     * FeedService 依赖本类（读粉丝），这里用 ObjectProvider 延迟获取，避免循环依赖。
     * 必须用 @Autowired：@Resource 会先按字段名找到 feedService 这个 bean 直接注入，循环依赖照样出现
     */
    @Autowired
    private ObjectProvider<FeedService> feedServiceProvider;

    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        if(UserHolder.getUser()==null){
            return Result.fail("用户未登录");
        }
        //获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        if(isFollow){
            //关注 新增数据
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(followUserId);
            boolean isSuccess;
            try {
                isSuccess = save(follow);
            } catch (org.springframework.dao.DuplicateKeyException e) {
                // uk_user_follow 唯一索引：重复关注视为成功（幂等）
                isSuccess = true;
            }
            if (isSuccess) {
                syncFollowsAfterChange(userId, followUserId, true);
            }
        }else{
            // 取关，删除
            boolean isSuccess = remove(new QueryWrapper<Follow>()
                    .eq("user_id", userId).eq("follow_user_id", followUserId));
            if (isSuccess) {
                syncFollowsAfterChange(userId, followUserId, false);
            }
        }
        return Result.ok();
    }

    /**
     * 关注集合只在已存在时才增量修改，「判断存在 + SADD/SREM」原子执行。
     * 不能直接 SADD：集合不在 Redis 里时（造数、被淘汰、从未加载），SADD 会建出一个只有这一个人的集合，
     * 之后 isFollow / 共同关注 / Feed 查大 V 都把它当成完整的关注列表。返回 1 = 已修改，0 = 集合不存在。
     */
    private static final DefaultRedisScript<Long> UPDATE_IF_EXISTS_SCRIPT = new DefaultRedisScript<>(
            "if redis.call('exists', KEYS[1]) == 1 then " +
            "  redis.call(ARGV[1], KEYS[1], ARGV[2]) " +
            "  return 1 " +
            "end " +
            "return 0", Long.class);

    /** 关注关系已写入 DB 后同步 Redis 关注集合，并让 Feed 的「我关注的大 V」缓存失效 */
    private void syncFollowsAfterChange(Long userId, Long followUserId, boolean added) {
        Long updated = stringRedisTemplate.execute(UPDATE_IF_EXISTS_SCRIPT, List.of(FOLLOWS_KEY + userId),
                added ? "sadd" : "srem", followUserId.toString());
        if (updated == null || updated == 0L) {
            // 集合不存在：DB 已是变更后的状态，整体重建
            reloadFollows(userId);
        }
        feedServiceProvider.getObject().onFollowChanged(userId);
    }

    /**
     * 从 DB 重建 follows:{userId} 并返回关注列表。没有关注任何人时不建 key（Redis 不存空集合），
     * 调用方据此回源 DB 判断。
     */
    private List<Follow> reloadFollows(Long userId) {
        List<Follow> follows = query().select("follow_user_id").eq("user_id", userId).list();
        if (!follows.isEmpty()) {
            stringRedisTemplate.opsForSet().add(FOLLOWS_KEY + userId, follows.stream()
                    .map(f -> f.getFollowUserId().toString()).toArray(String[]::new));
        }
        return follows;
    }

    /** 集合不在 Redis 里时从 DB 重建 */
    private void ensureFollowsLoaded(Long userId) {
        if (!Boolean.TRUE.equals(stringRedisTemplate.hasKey(FOLLOWS_KEY + userId))) {
            reloadFollows(userId);
        }
    }

    @Override
    public Result isFollow(Long followUserId) {
        if(UserHolder.getUser()==null){
            return Result.fail("用户未登录");
        }
        Long userId = UserHolder.getUser().getId();
        String key = FOLLOWS_KEY + userId;
        // 优先查 Redis 关注集合（关注/取关时同步维护）；集合不存在时回源 DB 并重建
        Boolean member = stringRedisTemplate.opsForSet().isMember(key, followUserId.toString());
        if (Boolean.TRUE.equals(member)) {
            return Result.ok(true);
        }
        if (Boolean.TRUE.equals(stringRedisTemplate.hasKey(key))) {
            return Result.ok(false);
        }
        List<Follow> follows = reloadFollows(userId);
        return Result.ok(follows.stream().anyMatch(f -> followUserId.equals(f.getFollowUserId())));
    }

    @Override
    public Result followCommons(Long id) {
        // 1.获取当前用户
        Long userId = UserHolder.getUser().getId();
        String key = FOLLOWS_KEY + userId;
        // 2.求交集（两边的关注集合可能不在 Redis 里，先补齐，否则交集会少）
        String key2 = FOLLOWS_KEY + id;
        ensureFollowsLoaded(userId);
        ensureFollowsLoaded(id);
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key, key2);
        if (intersect == null || intersect.isEmpty()) {
            // 无交集
            return Result.ok(Collections.emptyList());
        }
        // 3.解析id集合
        List<Long> ids = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
        // 4.查询用户
        List<UserDTO> users = userService.listByIds(ids)
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(users);
    }

}
