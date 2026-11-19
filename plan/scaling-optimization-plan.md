> 状态：已执行（2026-11-19）。下文为批准时的原始计划，执行中的偏差见文末「执行记录」。
> 行为变更明细见 [../docs/behavior-changes.md](../docs/behavior-changes.md)，扩容路线见 [../docs/scaling-roadmap.md](../docs/scaling-roadmap.md)。

# sun_life 用户量增长优化方案（分阶段 + 落地）

## Context
项目已在本地 Docker 跑通（app/MySQL/Redis/Kafka/前端各 1 个容器）。用户量变大后需要逐级扩容。
代码扫描发现：单机层面就有不少瓶颈（每请求 2 次 Redis、N+1 查询、缺索引、热点行锁、debug 日志），
另有 2 个正确性 bug（ThreadLocal 未清理导致串号、秒杀拿锁失败丢单），以及若干阻碍多实例部署的点
（本地磁盘上传、定时任务无分布式锁、compose 固定 container_name、nginx 单上游）。
用户选择：分阶段讲全，并在本项目落地改代码。**本次落地 = 阶段 0 + 阶段 1 + 阶段 2；阶段 3 只出方案文档。**

---

## 阶段 0：先修正确性 bug（必须先做）
1. `utils/RefreshInterceptor.java`
   - `afterCompletion` 调用 `UserHolder.removeUser()`（当前未清理 → 线程复用时 A 用户身份泄漏给无 token 请求）。
   - token 过期时 `return true`（当作未登录放行，由 `LoginInterceptor` 决定是否拦截），不再直接 `return false` 空响应。
2. `mq/SeckillOrderConsumer.java`：`tryLock` 失败时直接 ack 会丢单（Redis 库存已扣）。
   改为：去掉消费端 Redisson 锁（userId 作 key 已保证同用户同分区串行），
   依赖新增唯一索引 `uk_user_voucher(user_id, voucher_id)`，`createVoucherOrder` 捕获 `DuplicateKeyException` 视为幂等成功。

## 阶段 1：单机榨干（万级 QPS 内）
**数据库**
- 新增迁移脚本 `src/main/resources/db/migration_v2.sql`（已有数据卷不会重跑 hmdp.sql，需 `docker exec` 手动执行），同时把索引补进 `hmdp.sql`：
  - `tb_voucher_order`: `uk_user_voucher(user_id,voucher_id)`、`idx_status_ctime(status,create_time)`
  - `tb_blog`: `idx_liked(liked)`、`idx_user_id(user_id)`
  - `tb_follow`: `uk_user_follow(user_id,follow_user_id)`、`idx_follow_user(follow_user_id)`
  - `tb_blog_comments`: `idx_blog_id(blog_id)`；`tb_voucher`: `idx_shop_id(shop_id)`
- `application.yaml`：Hikari `maximum-pool-size: 30`、`minimum-idle: 10`、超时参数；Lettuce `max-active: 50`；
  Tomcat `threads.max`/`accept-count` 显式配置；日志 `com.hmdp: info`（debug 仅放 `application-dev.yaml`）。

**Redis / 登录**
- `RefreshInterceptor`：TTL 刷新改为“剩余 TTL < 阈值才 expire”——简单做法：用户 hash 里存 `refreshAt` 字段，超过 30 分钟才刷新，省掉每请求 1 次 EXPIRE。
- `UserServiceImpl.login`：`putAll + expire` 合并为 pipeline。

**热点读**
- `ShopServiceImpl.queryById`：热点商铺改用已有的 `CacheClient.queryWithMutex`（代码已写好、被注释）防击穿；保留布隆过滤器。
- `BlogServiceImpl.queryHotBlog`：结果按页缓存 60s（带 `CacheClient` TTL 抖动）；配合 `idx_liked`。
- `BlogServiceImpl` N+1：`queryHotBlog`/`queryBlogOfFollow` 中改为 `userService.listByIds(批量)` + `executePipelined` 批量 GETBIT。
- `FollowServiceImpl.isFollow`：改查 Redis `follows:{userId}`（SISMEMBER）。
- `VoucherOrderController` 订单状态轮询：先查 Redis 短缓存（订单创建成功后写 `order:status:{id}`，TTL 5min），未命中再查库。

**写路径**
- `likeBlog`：用 `SETBIT` 返回旧值做原子判断（替代 GETBIT+SETBIT 竞态），仅在状态变化时更新 DB；Redis 操作 pipeline。
- `saveBlog` 推送粉丝收件箱：pipeline 批量 ZADD，并 `ZREMRANGEBYRANK` 截断每个收件箱至 1000 条。
- `closeTimeoutOrders`：分批 `LIMIT 500` 循环，每批独立事务；库存回补按 voucher 聚合成一条 `stock = stock + n`。

**秒杀消费**
- 热点库存行锁是吞吐上限：消费端改 batch listener（`max-poll-records` 已是 200），同一批按 voucherId 聚合，
  一次 `UPDATE stock = stock - n WHERE stock >= n` + `saveBatch` 插入订单（唯一索引兜底幂等）；失败回退逐条处理。
- `seckill.lua` 增加活动时间校验（begin/end 预热进 Redis）。

## 阶段 2：多实例水平扩展（十万级 QPS）
- **无状态化**
  - `UploadController` 去掉硬编码 Windows 路径：抽象 `FileStorage` 接口，默认实现写共享目录（compose volume，前端 nginx 同时挂载提供静态访问），
    预留 OSS/MinIO 实现；修复 delete 接口路径穿越（校验规范化后路径在根目录内），上传加大小/类型限制。
  - `OrderTimeoutScheduler`：用 Redisson `tryLock` 保证集群只有一个实例执行。
  - 删除 `CacheClient` 中未用的静态线程池，或改为 Spring 管理的有界线程池。
- **部署** `deploy/docker-compose.yml` / `deploy/frontend/nginx.conf`
  - app 去掉 `container_name` 和固定宿主端口，支持 `docker compose up --scale app=3`。
  - nginx `upstream hmdp_app { server app:8081; keepalive 64; }`（Docker DNS 轮询多实例）、`proxy_set_header Connection ""`、
    `X-Real-IP/X-Forwarded-For`、gzip、静态资源 `expires`、秒杀接口 `limit_req` 限流。
  - Kafka topic 分区 3 → 12（`KafkaConfig`），使多实例消费者能真正分担。
  - 验证码接口加按手机号/IP 的 Redis 限流（`sendCode`）。

## 阶段 3：架构演进（只写方案，不落地）
写入 `docs/scaling-roadmap.md`：
- MySQL 主从读写分离（ShardingSphere-JDBC / 动态数据源），订单表按 user_id 分库分表 + 雪花/号段 ID 替代按天 INCR。
- Redis Cluster（Lua 涉及的 key 加 hash tag `{voucherId}`）、Kafka 3 broker 副本因子 3。
- 多级缓存：Caffeine 本地缓存 + Redis（热点 key 探测，如店铺详情/热门笔记），Canal 订阅 binlog 做缓存失效。
- 点赞计数异步化：Redis 计数 + 定时批量回写 DB；Feed 大 V 推拉结合。
- 秒杀：outbox 本地消息表解决“Lua 已扣、send 前宕机”；库存分段（多 key 分片）缓解单 key 热点。
- 网关层限流熔断（Sentinel）、监控（Prometheus + Grafana + SkyWalking）、K8s HPA 弹性伸缩、CDN。

---

## 关键文件
`utils/RefreshInterceptor.java`、`mq/SeckillOrderConsumer.java`、`mq/OrderTimeoutScheduler.java`、
`service/impl/{VoucherOrder,Blog,Shop,Follow,User}ServiceImpl.java`、`utils/CacheClient.java`（复用 `queryWithMutex`/`jitteredSeconds`）、
`controller/UploadController.java`、`config/KafkaConfig.java`、`resources/application.yaml`、`resources/seckill.lua`、
`resources/db/hmdp.sql` + 新 `migration_v2.sql`、`deploy/docker-compose.yml`、`deploy/frontend/nginx.conf`、新 `docs/scaling-roadmap.md`。

## 验证
1. Docker 内 Maven 编译 + `mvn test`（同之前的 docker run 命令）。
2. 执行 `migration_v2.sql`，`SHOW INDEX` 确认；`EXPLAIN` 超时扫描 / 热门笔记 SQL 走索引。
3. 正确性：无 token 请求 `/user/me` 不再返回他人信息；过期 token 访问 `/shop/1` 正常。
4. 按 `perf/README.md` 重跑秒杀与店铺详情 JMeter 压测（200/500/1000 并发），与原数据（~1 万 QPS、P99 31/73/117ms）对比；
   校验订单数 = 用户数、DB 库存 = Redis 库存、无重复订单。
5. `docker compose up -d --scale app=3`，确认 nginx 轮询多实例、超时关单只有一个实例执行、上传图片任意实例可访问、Kafka 12 分区均分到各实例。

---

## 执行记录：与计划的偏差

| 计划 | 实际 | 原因 |
| --- | --- | --- |
| 订单表加唯一索引 `uk_user_voucher(user_id, voucher_id)`，消费端靠它兜底幂等 | 改为普通索引 `idx_user_voucher`；幂等靠订单 id 主键（`DuplicateKeyException`） | 超时关单会把用户移出 Redis 去重集合、允许再次抢购，唯一索引会让第二次抢购写库失败而库存已扣（丢单）。一人一单由 Lua 原子保证 |
| `likeBlog` 用 `SETBIT` 返回旧值 | 改为 Lua 脚本原子切换 bit + TopN ZSet | 点赞是“切换”语义，需先读后写；Lua 一次往返同时完成 bit 与 ZSet |
| `queryHotBlog` 复用 `CacheClient` TTL 抖动 | 在 `BlogServiceImpl` 内按同样规则（±10%）实现，用 Jackson 序列化 | 列表含 `LocalDateTime`，Jackson（已配置 JavaTimeModule）比 Hutool JSON 更可靠 |
| `CacheClient` 静态线程池删除或改 Spring 管理 | 改为有界队列 + 丢弃策略 + 守护线程 | 逻辑过期方案仍保留可用 |
| — | 额外：`queryWithMutex` 修复解锁不校验持有者、无限递归，增加双重检查 | 启用互斥锁方案前必须修 |
| — | 额外：`/upload/**` 不再免登录 | 原先匿名可上传/删除 |
| — | 额外：nginx 同时监听 8081 作为后端 API 入口 | 多副本后 app 不再映射宿主端口，保持 `localhost:8081` 与压测脚本兼容 |
| — | 额外：上传卷挂到 nginx 独立目录 + `try_files` 回退 | 直接覆盖 `html/imgs/blogs` 会遮住镜像内 6.6MB 示例图片 |
| — | 额外：验证码日志改为 info | 日志级别降为 info 后本地无法取得验证码 |
| 验证 1：`mvn test` | 未执行 | 现有测试依赖本机默认端口的 MySQL/Redis |
| 验证 4：JMeter 压测对比 | 未执行；改用 Python 并发脚本验证正确性 | 本机无 JMeter，脚本 QPS 受客户端限制，不能与原数据比较 |
