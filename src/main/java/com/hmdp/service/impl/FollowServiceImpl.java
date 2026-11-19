package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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

    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        if(UserHolder.getUser()==null){
            return Result.fail("用户未登录");
        }
        //获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        String key = "follows:" + userId;
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
                // 把关注用户的id，放入redis的set集合 sadd userId followerUserId
                stringRedisTemplate.opsForSet().add(key, followUserId.toString());
            }
        }else{
            // 取关，删除
            boolean isSuccess = remove(new QueryWrapper<Follow>()
                    .eq("user_id", userId).eq("follow_user_id", followUserId));
            if (isSuccess) {
                // 把关注用户的id从Redis集合中移除
                stringRedisTemplate.opsForSet().remove(key, followUserId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result isFollow(Long followUserId) {
        if(UserHolder.getUser()==null){
            return Result.fail("用户未登录");
        }
        Long userId = UserHolder.getUser().getId();
        String key = "follows:" + userId;
        // 优先查 Redis 关注集合（关注/取关时同步维护）；集合不存在时回源 DB 并重建
        Boolean member = stringRedisTemplate.opsForSet().isMember(key, followUserId.toString());
        if (Boolean.TRUE.equals(member)) {
            return Result.ok(true);
        }
        if (Boolean.TRUE.equals(stringRedisTemplate.hasKey(key))) {
            return Result.ok(false);
        }
        List<Follow> follows = query().select("follow_user_id").eq("user_id", userId).list();
        if (!follows.isEmpty()) {
            stringRedisTemplate.opsForSet().add(key, follows.stream()
                    .map(f -> f.getFollowUserId().toString()).toArray(String[]::new));
        }
        return Result.ok(follows.stream().anyMatch(f -> followUserId.equals(f.getFollowUserId())));
    }

    @Override
    public Result followCommons(Long id) {
        // 1.获取当前用户
        Long userId = UserHolder.getUser().getId();
        String key = "follows:" + userId;
        // 2.求交集
        String key2 = "follows:" + id;
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
