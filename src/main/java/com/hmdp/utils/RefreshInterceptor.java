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
 *
 * <p>先查本地短缓存（{@link LoginUserCache}），未命中才 {@code HGETALL}。续期判断只在未命中、刚读过 Redis 时做：
 * 本地缓存的 TTL（十几秒）远小于续期间隔（30 分钟），不会漏掉续期；而且这样续期写入之前一定刚确认过 key 还在，
 * 不会给已经退出登录的 token 重新写出一个只有 refreshAt 字段的 key。</p>
 */
@Component
public class RefreshInterceptor implements HandlerInterceptor {

    private final StringRedisTemplate stringRedisTemplate;
    private final LoginUserCache loginUserCache;

    public RefreshInterceptor(StringRedisTemplate stringRedisTemplate, LoginUserCache loginUserCache) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.loginUserCache = loginUserCache;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        //1. 获取请求头中的token
        String token = request.getHeader("authorization");
        if (StrUtil.isBlank(token)) {
            return true;
        }

        //2. 本地短缓存命中：不访问 Redis
        UserDTO cached = loginUserCache.get(token);
        if (cached != null) {
            UserHolder.saveUser(cached);
            return true;
        }

        //3. 查询 Redis 中的用户
        String key = RedisConstants.LOGIN_USER_KEY + token;
        Map<Object, Object> userMap = stringRedisTemplate.opsForHash().entries(key);

        //4. token 已过期：按未登录处理，交给 LoginInterceptor 决定是否拦截
        if (userMap.isEmpty()) {
            return true;
        }

        //5. 保存用户到 ThreadLocal（refreshAt 等多余字段会被忽略）。没有 id 的不是有效登录态，按未登录处理
        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);
        if (userDTO.getId() == null) {
            return true;
        }
        UserHolder.saveUser(userDTO);
        loginUserCache.put(token, userDTO);

        //6. 按需续期：距上次续期超过阈值才写 Redis，避免每个请求多一次 EXPIRE
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
