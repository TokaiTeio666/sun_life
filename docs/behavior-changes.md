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

### 1.6 过载时返回 503（2026-11-20，阶段 3 的 E1）

| 场景 | 之前 | 之后 |
| --- | --- | --- |
| 回源 DB 的并发超过上限（`GET /shop/{id}` 缓存未命中，或关缓存直查） | 在连接池上排队，最长等 3s | 立即返回 HTTP `503`，`Retry-After: 1`，`{"success":false,"errorMsg":"系统繁忙，请稍后再试"}` |
| 任意接口等 DB 连接超时 | 等 3s 后返回 HTTP `200` + `服务器异常`，并打印堆栈 | 等 800ms 后返回同样的 `503`，不打印堆栈，计入 `hmdp_overload_rejected_total` |

- 上限（并发闸许可数）默认 = `DB_POOL_MAX × 2`，`DB_GATE_PERMITS` 指定，`0` 关闭；等连接超时由 `DB_CONN_TIMEOUT_MS` 指定（默认 800）。
  回到改造前的行为：`DB_GATE_PERMITS=0` + `DB_CONN_TIMEOUT_MS=3000`（503 的映射没有开关）。
- 并发闸只覆盖经 `CacheClient` 回源和商铺详情的直查；其他接口过载时走第二行（等 800ms）。
- 调用方需要把 503 当作可重试的失败。nginx 的 `proxy_next_upstream` 只对 `error timeout` 重试，不会把 503 转给其他副本放大流量。
- Kafka 消费者和定时任务共用连接池，等连接的超时同样变成 800ms；它们不经过并发闸，超时后走原有的重试 / 死信流程。

本机验证（1 个副本，`DB_POOL_MAX=4`，MySQL 限到 0.05 核模拟变慢，300 并发压关缓存的 `/shop/1` 20s；同机，不作基线）：

| 配置 | 全部请求 P50 / P99 | 成功请求平均耗时 | 成功 /s | 503 的平均耗时 |
| --- | --- | --- | --- | --- |
| 改造前：超时 3000ms，无并发闸 | 3000 / 3202 ms | 约 2.6 s | 约 70 | 约 3.0 s |
| 只降超时：800ms，无并发闸 | 803 / 998 ms | 约 0.74 s | 约 82 | 约 0.8 s |
| 默认：800ms + 并发闸（许可 8） | 22 / 103 ms | 约 0.12 s | 约 68 | 小于 1 ms |

MySQL 不限速、4 并发时 2 万次请求 0 失败；默认配置（连接池 30、许可 60）走缓存 50 并发 2 万次请求 0 失败、0 次拒绝。

### 1.7 商铺布隆过滤器容量（2026-11-20，阶段 3 的 E2）

| 场景 | 之前 | 之后 |
| --- | --- | --- |
| 设计容量 | 写死 1 万家；10 万家店时位图写满 99%，不存在的 id 有 95.9% 穿过过滤器 | 默认 100 万家（`BLOOM_EXPECTED_INSERTIONS`），位图约 1.2MB；10 万家店时填充率 5.14% |
| Redis key | `bloom:shop` | `bloom:shop:{位数}:{哈希数}`（默认 `bloom:shop:9585058:7`）；改容量后换新 key，其他 key（含 `bloom:shop`）24 小时后过期 |
| 启动预热 | 一次读出全部 id，一个 pipeline 写入 | 按 id 游标每页 1000 个，每页一个 pipeline；完成后日志打印填充率与估算误判率，店铺数超过设计容量时 WARN |
| 预热完成前的请求 | 位图为空时所有店铺都返回「店铺不存在」 | 过滤器未就绪时一律放行（空值缓存兜底） |
| 不经过应用写入的新店铺（造数脚本、旧版本副本新增） | 重启 app 前一直返回「店铺不存在」 | 定时增量同步（`hmdp.bloom.sync-interval-ms`，默认 60s）补进过滤器 |
| 位图被淘汰 / 清库 | 重启 app 前所有店铺返回「店铺不存在」 | 下一轮同步（最长 60s）发现后整体重建；重建完成前仍会误拒 |

- 没有单独的开关：`HMDP_CACHE_ENABLED=false` 时整条缓存链路（含过滤器）不参与请求。回退到旧容量可设
  `BLOOM_EXPECTED_INSERTIONS=10000`（key 为 `bloom:shop:95850:7`，不是旧的 `bloom:shop`）。
- 滚动发布：未升级的副本继续读写 `bloom:shop`，新副本用新 key。旧 key 在第一个新副本启动 24 小时后过期，
  **发布要在 24 小时内完成**，否则仍在运行的旧版本副本会把所有店铺判为不存在。
- 哈希函数没有变。按这套哈希离线模拟到设计容量（100 万个连续 id）：填充率 41.6%，不存在的 id 误判率 0.37%–0.50%。

本机验证（10 万家店，`python perf/baseline/bloom_check.py --shops 100000`）：10 万个存在的 id 全部放行；
10 万个不存在的随机 id 穿过过滤器 0 个（改造前抽样 2 万个，穿过 19183 个）。直接往库里插入一家店约 1 分钟后可访问；
删除位图后约 1 分钟自动重建；容量改为 5 万再改回，新旧 key 的过期时间按上表切换，超容量告警正常。
每请求的 Redis 命令数不变（存在的 id 仍是 1 × BITFIELD + 1 × GET）。

### 1.8 关注 Feed：异步推送与推拉结合（2026-11-20，阶段 3 的 E3）

| 场景 | 之前 | 之后 |
| --- | --- | --- |
| `POST /blog` | 请求线程里查出全部粉丝并逐个推送，耗时随粉丝数增长 | 落库 + 写作者发件箱（2 条 Redis 命令）+ 发一条 Kafka 消息（`blog.published`，key = 作者 id）后返回 |
| 普通作者的笔记 | 接口返回时粉丝已能看到 | 消费者按粉丝 id 游标分页（每页 1000）推送到收件箱，粉丝在消息消费后可见（本机约 0.1s） |
| 粉丝数超过阈值（默认 5000）的作者 | 同上，全部推送 | 不推送：作者记入 `feed:bigv`，粉丝读 Feed 时从作者的发件箱拉取 |
| `GET /blog/of/follow` | 只读自己的收件箱 | 收件箱 + 所关注大 V 的发件箱，按时间合并、按笔记 id 去重；同一毫秒的多条按笔记 id 倒序 |
| `GET /blog/of/follow` 返回的 `offset` | `minTime == max ? os : os + offset`（写反了，第 3 页起会漏掉笔记） | 等于「已返回的、时间等于 `minTime` 的条数」；入参与返回结构不变 |
| 新关注一个大 V | 只能看到关注之后的新笔记 | 立即能看到该大 V 发件箱里的历史笔记（最近 1000 条）；取关后立即看不到 |

- 开关：`FEED_ASYNC_PUSH=false` 回到在请求线程里同步推送给全部粉丝（不写发件箱，不新增大 V）；
  `FEED_BIGV_THRESHOLD` 调整大 V 阈值，`<= 0` 表示不区分大 V、全部异步推送。读取时的合并没有开关：`feed:bigv` 为空时等价于只读收件箱。
- 发件箱对**所有**作者都写（原方案是只给大 V 写）：这样作者粉丝数越过阈值时不用回填历史笔记，之前推送过的笔记在收件箱和发件箱各有一份，读取时去重。
- 大 V 标记只增不减：粉丝数回落也不会自动摘除，否则只存在于发件箱的笔记会从粉丝 Feed 里消失。需要摘除时手工 `SREM feed:bigv {userId}`。
- 作者是否是大 V 在消费时判断（`SISMEMBER`，未命中时用 `LIMIT 1 OFFSET 阈值` 查一次 tb_follow），不在发帖请求里判断。
- **会丢推送的情况**：笔记落库后 Kafka 消息发送失败，或消费重试 3 次仍失败——普通作者的粉丝收不到这一条（笔记本身不丢，大 V 的笔记不受影响）。
  分别计入 `hmdp_feed_publish_failures_total`、`hmdp_feed_fanout_failures_total` 并打 ERROR 日志。要做到不丢需要本地消息表，归入批次 C 的 Outbox。
- 关注集合 `follows:{userId}` 不在 Redis 里时（造数、被淘汰），读 Feed 会先从 DB 重建；一个人都没关注的用户每次读 Feed 都会多一次 DB 查询。
- 新增消费组 `feed-fanout-group`（每副本 2 个消费线程，主题 3 分区）；它不受 `FEED_ASYNC_PUSH` 影响、始终运行。

本机验证（造数：1 号作者 63193 粉丝，21 号作者 2888 粉丝；各发 3 条；同机，不作基线）：

| | `FEED_ASYNC_PUSH=false`（改造前的行为） | 默认 |
| --- | --- | --- |
| 1 号（大 V）发帖耗时 | 2066–3555 ms | 26–47 ms（首次 161 ms） |
| 21 号发帖耗时 | 79–93 ms | 22–36 ms |
| 6 条笔记产生的 ZADD | 198243（另有等量的 ZREMRANGEBYRANK） | 8670 = 3 × 2888 + 6 条发件箱 |
| 粉丝 Feed | 正确 | 只关注 21 号 / 只关注 1 号 / 两个都关注的粉丝各取一个：新帖齐全、无重复、时间倒序；12 条分 3 页翻完；同一毫秒 7 条时翻页无重复无遗漏 |

每次读 Feed 的 Redis 命令：`ZREVRANGEBYSCORE` 1 条 → `EXISTS` + `SINTER` + `ZREVRANGEBYSCORE` ×（1 + 关注的大 V 数），两次往返（原来一次）。

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
| `bloom:shop:{位数}:{哈希数}` | string（位图） | 商铺布隆过滤器，取代 `bloom:shop`（见 1.7） |
| `bloom:shop:keys` | set | 用过的布隆位图 key，用于容量变化后找到旧 key 并设过期 |
| `outbox:{userId}` | zset | 作者发件箱：member = 笔记 id，score = 发布时间毫秒，保留最近 1000 条（见 1.8） |
| `feed:bigv` | set | 大 V 的用户 id；这些作者的笔记不推送，由粉丝读取时拉取 |

## 3. 配置变化（`application.yaml`）

| 配置 | 之前 | 之后 |
| --- | --- | --- |
| `spring.datasource.url` | `serverTimezone=UTC` | `serverTimezone=Asia/Shanghai&rewriteBatchedStatements=true` |
| `spring.datasource.hikari.maximum-pool-size` | 默认 10 | `${DB_POOL_MAX:30}`，`minimum-idle: 10`，`connection-timeout: 3000`（之后改为 `${DB_CONN_TIMEOUT_MS:800}`，见 1.6） |
| `hmdp.db-gate.permits` | 无 | `${DB_GATE_PERMITS:-1}`（负数 = 连接池 × 2，0 = 关闭） |
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
