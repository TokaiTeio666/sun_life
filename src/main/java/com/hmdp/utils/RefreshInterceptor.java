package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 全路径拦截器：解析 token → 把用户放进 ThreadLocal，并按需续期。
 *
 * <p>只负责“识别身份”，不负责“拦截”：token 缺失或过期一律放行，由 {@link LoginInterceptor}
 * 对需要登录的路径返回 401，避免过期 token 把公开页面也挡住。</p>
 */
@Component
public class RefreshInterceptor implements HandlerInterceptor {

    private final StringRedisTemplate stringRedisTemplate;

    public RefreshInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        //1. 获取请求头中的token
        String token = request.getHeader("authorization");
        if (StrUtil.isBlank(token)) {
            return true;
        }

        //2. 查询 Redis 中的用户
        String key = RedisConstants.LOGIN_USER_KEY + token;
        Map<Object, Object> userMap = stringRedisTemplate.opsForHash().entries(key);

        //3. token 已过期：按未登录处理，交给 LoginInterceptor 决定是否拦截
        if (userMap.isEmpty()) {
            return true;
        }

        //4. 保存用户到 ThreadLocal（refreshAt 等多余字段会被忽略）
        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);
        UserHolder.saveUser(userDTO);

        //5. 按需续期：距上次续期超过阈值才写 Redis，避免每个请求多一次 EXPIRE
        long now = System.currentTimeMillis();
        Object refreshAt = userMap.get(RedisConstants.LOGIN_REFRESH_AT_FIELD);
        long last = refreshAt == null ? 0L : Long.parseLong(refreshAt.toString());
        if (now - last > RedisConstants.LOGIN_REFRESH_INTERVAL_MS) {
            stringRedisTemplate.opsForHash().put(key, RedisConstants.LOGIN_REFRESH_AT_FIELD, String.valueOf(now));
            stringRedisTemplate.expire(key, RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);
        }
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) {
        // Tomcat 线程会复用，必须清理，否则下一个无 token 请求会拿到上一个用户的身份
        UserHolder.removeUser();
    }
}
