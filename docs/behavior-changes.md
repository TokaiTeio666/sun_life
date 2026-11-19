# 行为变更说明（scaling 优化）

本文逐项列出这次优化后**外部可观察到的行为变化**：接口返回、Redis key、数据库结构、部署与运维方式。
设计动机与分阶段路线见 [scaling-roadmap.md](scaling-roadmap.md)，执行计划见 [../plan/scaling-optimization-plan.md](../plan/scaling-optimization-plan.md)。

---

## 1. 接口行为

### 1.1 登录与鉴权

| 场景 | 之前 | 之后 |
| --- | --- | --- |
| 不带 token 请求需登录的接口（如 `/user/me`） | 可能返回**同一 Tomcat 线程上一个用户**的信息（ThreadLocal 未清理） | 始终 `401` |
| 带过期/无效 token 请求公开接口（如 `/shop/1`） | `200` 空响应体 | 正常返回数据（按未登录处理） |
| 带过期/无效 token 请求需登录的接口 | `200` 空响应体 | `401` |
| 登录态续期 | 每个请求都执行 `EXPIRE` | 距上次续期超过 30 分钟才续期；TTL 仍为 36000 分钟 |
| `POST /user/code` 同一手机号 60s 内再次请求 | 正常发送 | `{"success":false,"errorMsg":"验证码发送过于频繁，请稍后再试"}` |
| `POST /user/code` 同一 IP 1 小时内超过 20 次 | 正常发送 | `{"success":false,"errorMsg":"请求过于频繁，请稍后再试"}`（IP 取 `X-Real-IP`，无则取连接地址） |
| 验证码日志 | `debug` 级别 | `info` 级别（模拟短信，本地从日志获取） |

### 1.2 图片上传 `/upload/**`

| 场景 | 之前 | 之后 |
| --- | --- | --- |
| 未登录上传/删除 | 允许 | `401` |
| 存储位置 | 硬编码 `D:\javaproj\nginx-1.18.0\html\hmdp\imgs\`（容器内无效） | `hmdp.upload.dir`（环境变量 `HMDP_UPLOAD_DIR`，默认 `./upload`；Docker 中为共享卷 `/data/upload`） |
| 文件类型 | 不限制 | 仅 `jpg/jpeg/png/gif/webp`，否则 `仅支持 jpg/jpeg/png/gif/webp 图片` |
| 文件大小 | Spring 默认 1MB | 单文件 5MB，单请求 10MB |
| 删除接口 `name` 参数 | 拼接路径直接删除（可 `../` 穿越） | 必须匹配 `/blogs/{0-15}/{0-15}/{uuid}.{ext}`，否则 `错误的文件名称` |
| 返回值 | `/blogs/x/y/uuid.ext` | 不变；前端仍通过 `/imgs/blogs/...` 访问 |

### 1.3 秒杀与订单

| 场景 | 之前 | 之后 |
| --- | --- | --- |
| `POST /voucher-order/seckill/{id}` 活动未开始 | 可下单 | `秒杀尚未开始` |
| 活动已结束 | 可下单 | `秒杀已经结束` |
| 活动时间缓存不存在（如启动后才导入的券） | — | 跳过时间校验（兼容旧数据） |
| `GET /voucher-order/status/{orderId}` | 每次查 DB | 先查 Redis 短缓存（5 分钟），未命中查 DB；返回值含义不变（0 = 尚未落库） |
| 超时关单后同一用户再次抢购 | 允许 | 仍允许（订单表不加唯一索引，见 4.2） |
| 消费者落库 | 逐条：用户锁 + SELECT 判重 + 扣库存 + INSERT | 批量：按券聚合 `stock = stock - n` + 多值 INSERT；失败降级逐条；单条仍失败重试 3 次后进 DLT |
| 消费端拿用户锁失败 | **ack 并丢弃消息（丢单）** | 不再使用消费端锁；重复消息靠订单 id 主键幂等 |

### 1.4 笔记、点赞、关注

| 场景 | 之前 | 之后 |
| --- | --- | --- |
| `GET /blog/hot` | 实时查询 | 按页缓存 60s（±10%），点赞数最多延迟约 1 分钟；`isLike` 仍按当前用户实时计算 |
| `GET /blog/of/follow` 每页条数 | 2 | 5 |
| `PUT /blog/like/{id}` 并发重复点击 | 可能重复计数 | Lua 原子切换，状态与计数一致 |
| 对不存在的笔记点赞 | `{"success":true}` | `笔记不存在！`（Redis 状态回滚） |
| 发笔记推送 | 粉丝收件箱无限增长 | 每个收件箱只保留最近 1000 条 |
| `PUT /follow/or/not/{id}` | 每次 DB COUNT | 查 Redis `follows:{userId}`；集合不存在时回源 DB 并重建 |
| 重复关注同一用户 | 插入重复行 | 唯一索引拦截，接口仍返回成功（幂等） |

### 1.5 商铺详情 `GET /shop/{id}`

缓存策略从“仅防穿透”改为“互斥锁防击穿 + 空值缓存 + 布隆过滤器”。热点 key 过期时只有一个请求回源 DB，
其他请求最多等待约 1s（20 × 50ms），超时直接回源。返回值不变。

## 2. Redis key 变化

| key | 类型 | 说明 |
| --- | --- | --- |
| `login:token:{token}` 新增字段 `refreshAt` | hash 字段 | 上次续期时间（毫秒），用于按需续期 |
| `limit:code:phone:{phone}` | string，TTL 60s | 验证码手机号限流 |
| `limit:code:ip:{ip}` | string 计数，TTL 1h | 验证码 IP 限流 |
| `seckill:time:{voucherId}` | hash `begin`/`end`（epoch 毫秒） | 秒杀活动时间；启动时预热、新增秒杀券时写入 |
| `order:status:{orderId}` | string，TTL 5min | 订单状态短缓存；支付、关单时删除 |
| `cache:blog:hot:{page}` | string(JSON)，TTL ~60s | 热门笔记分页缓存 |
| `lock:job:order-timeout` | Redisson 锁 | 超时关单任务集群互斥 |
| `feed:{userId}` | zset | 行为变化：截断到最近 1000 条 |
| `follows:{userId}` | set | 行为变化：缺失时由 `isFollow` 从 DB 重建 |

## 3. 配置变化（`application.yaml`）

| 配置 | 之前 | 之后 |
| --- | --- | --- |
| `spring.datasource.url` | `serverTimezone=UTC` | `serverTimezone=Asia/Shanghai&rewriteBatchedStatements=true` |
| `spring.datasource.hikari.maximum-pool-size` | 默认 10 | `${DB_POOL_MAX:30}`，`minimum-idle: 10`，`connection-timeout: 3000` |
| `spring.data.redis.lettuce.pool.max-active` | 10 | 50（`max-wait: 2000ms`，`timeout: 2000ms`） |
| `server.tomcat.*` | 默认 | `threads.max: 200`，`accept-count: 500`，`max-connections: 8192` |
| `spring.servlet.multipart` | 默认 | `max-file-size: 5MB`，`max-request-size: 10MB` |
| `spring.kafka.consumer.properties.max.poll.interval.ms` | 默认 | 300000 |
| `hmdp.kafka.seckill-partitions` | 无（硬编码 3） | 12 |
| `hmdp.upload.dir` | 无 | `${HMDP_UPLOAD_DIR:./upload}` |
| `logging.level.com.hmdp` | debug | info；`application-dev.yaml` 中为 debug（`--spring.profiles.active=dev`） |

## 4. 数据库变化

### 4.1 新增索引（`db/migration_v2.sql`，`hmdp.sql` 同步）

| 表 | 索引 | 服务的查询 |
| --- | --- | --- |
| `tb_voucher_order` | `idx_status_ctime(status, create_time)` | 超时关单扫描 |
| `tb_voucher_order` | `idx_user_voucher(user_id, voucher_id)` | 按用户/券查询、对账 |
| `tb_blog` | `idx_liked(liked)` | 热门笔记排序 |
| `tb_blog` | `idx_user_id(user_id)` | 个人主页笔记 |
| `tb_follow` | `uk_user_follow(user_id, follow_user_id)` **唯一** | 防重复关注 |
| `tb_follow` | `idx_follow_user(follow_user_id)` | 发笔记查粉丝 |
| `tb_blog_comments` | `idx_blog_id(blog_id)` | 按笔记查评论 |
| `tb_voucher` | `idx_shop_id(shop_id)` | 按店铺查券 |

已有数据卷的环境执行一次（可重复执行）：

```bash
docker exec -i hmdp-mysql mysql -uroot -p123456 hmdp < src/main/resources/db/migration_v2.sql
```

> `uk_user_follow` 创建前需保证 `tb_follow` 中没有重复的 `(user_id, follow_user_id)`，否则建索引失败。

### 4.2 刻意没有做的

订单表**不加** `(user_id, voucher_id)` 唯一索引：超时关单会把用户从 `seckill:order:{id}` 中移除、允许再次抢购，
唯一索引会导致第二次抢购的订单写库失败而 Redis 库存已扣。一人一单由 `seckill.lua` 原子保证。

## 5. 部署与运维

| 项 | 之前 | 之后 |
| --- | --- | --- |
| 后端容器 | 单个 `hmdp-app`，直接映射宿主 8081 | 服务 `app` 无固定容器名（`sun_life-app-N`），不映射宿主端口，可 `--scale app=N` |
| 宿主 8081 | 直连 app | 由 `hmdp-frontend`（nginx）监听并负载均衡到所有副本，路径与之前一致 |
| 宿主 8080 | 前端 + `/api` 代理到单个 app | 前端 + `/api` 负载均衡；秒杀接口按 IP 限流 20r/s（burst 40），超出返回 `429` |
| 看后端日志 | `docker logs hmdp-app` | `docker compose -f deploy/docker-compose.yml logs app` |
| 扩缩容 | 不支持 | `docker compose -f deploy/docker-compose.yml up -d --scale app=3`，随后 `restart frontend` |
| 上传图片 | 写本机硬编码目录 | 共享卷 `hmdp-upload-data`：app 挂 `/data/upload/blogs`，nginx 挂 `/usr/share/nginx/upload/imgs/blogs`（只读）；nginx 先找镜像内示例图，再找上传卷 |
| 超时关单 | 每个实例都执行 | Redisson 锁保证同一时刻只有一个实例执行 |
| Kafka 秒杀主题 | 3 分区 | 启动时自动扩到 12 分区（只能增不能减） |
| nginx | 单上游、无长连接复用 | `upstream` + `keepalive 64`、`proxy_next_upstream`、gzip、静态资源 7 天缓存、`X-Real-IP` 透传 |

**容量注意**：每个副本最多 30 个 DB 连接，`副本数 × DB_POOL_MAX` 需小于 MySQL `max_connections`（默认 151）。

## 6. 本地验证记录（2026-11-19，Docker Desktop，3 个 app 副本）

| 检查项 | 结果 |
| --- | --- |
| 无 token 连续 6 次 `/user/me` | 全部 401 |
| 无效 token 访问 `/shop/1` / `/user/me` | 200 正常数据 / 401 |
| 验证码 60s 内二次发送 | 被限流 |
| 点赞→取消 | DB 点赞数 1→2→1，`isLike` 正确；不存在的笔记返回错误 |
| 关注状态 | Redis 命中；删除集合后从 DB 重建；重复关注幂等，DB 仅 1 行 |
| 上传 | 匿名 401；登录后上传可经 8080 访问；示例图片仍可访问；穿越路径与非图片后缀被拒；删除后 404 |
| 秒杀活动时间 | 未开始 / 已结束 正确拦截 |
| 超时关单 | 走 `idx_status_ctime`；只有 1 个副本执行；DB/Redis 库存回补；状态缓存删除；用户可再次抢购 |
| 并发秒杀（2000 用户 × 每人 2 次，100 并发） | 成功 2000，重复 2000 全部拦截；订单 2000 / 用户 2000；DB 库存 = Redis 库存 = 98000；Kafka lag 0 |
| 批量落库效果 | ~2000 笔订单约 83 次库存 UPDATE；INSERT 为多值批量 |
| 非法消息 | 批量失败→逐条→重试 3 次→进入 `seckill.orders.DLT` |
| 负载均衡 | 3 个副本均有来自 nginx 的长连接 |

未完成：JMeter 性能对比压测（本机无 JMeter，Python 脚本的 QPS 受客户端限制，不可与 `perf/README.md` 原数据比较）；
`mvn test`（现有测试依赖本机默认端口的 MySQL/Redis）。
