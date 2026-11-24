# sun_life 扩容路线图：用户量增长时怎么办

按用户量 / QPS 逐级演进，每一级先把当前架构用到极限再升级，避免过早引入复杂度。
阶段 0–2 已在本仓库落地，阶段 3 为方案。

| 阶段 | 规模（参考） | 核心思路 | 状态 |
| --- | --- | --- | --- |
| 0 | — | 先修正确性 bug，扩容前必须做 | ✅ 已落地 |
| 1 | 单机 QPS 1–3 万 | 减少每个请求的 IO 次数：索引、批量、pipeline、缓存、连接池 | ✅ 已落地 |
| 2 | 十万级 QPS | 应用无状态化 + 多副本 + 网关负载均衡与限流 | ✅ 已落地 |
| 3 | 百万级 QPS / 亿级数据 | 存储层拆分：读写分离、分库分表、集群化、多级缓存 | 🚧 3-1 监控与工具已落地；2026-11-24 完成**本机受控基线**（被测机 = 压测机，只作下限），分机容量基线待有条件时实测；3-2 起为方案 |

---

## 阶段 0：正确性（用户量越大越容易暴露）

| 问题 | 后果 | 修复 |
| --- | --- | --- |
| `RefreshInterceptor` 不清理 ThreadLocal | Tomcat 线程复用，无 token 请求拿到上一个用户的身份（串号） | `afterCompletion` 中 `UserHolder.removeUser()` |
| token 过期时拦截器 `return false` | 带过期 token 访问公开页面得到空响应 | 过期视为未登录放行，由 `LoginInterceptor` 决定是否 401 |
| 秒杀消费者拿 Redisson 锁失败直接 ack | Redis 库存已扣、订单被丢弃 | 去掉消费端锁：userId 作 key 保证同用户同分区串行，订单 id 主键保证幂等 |
| `CacheClient.queryWithMutex` 解锁不校验持有者、无限递归 | 误删他人锁、栈溢出风险 | UUID 持有者 + `unlock.lua` 比对删除；有限次重试，超时直接回源 |
| `/upload/**` 免登录 + 删除接口可 `../` 穿越 | 匿名上传、任意文件删除 | 上传需登录；文件名白名单正则 + 路径规范化校验 |

> 订单表**没有**加 `(user_id, voucher_id)` 唯一索引：超时关单会把用户移出 Redis 去重集合、允许再次抢购，
> 唯一索引会让第二次抢购的订单写库失败而库存已扣（丢单）。一人一单由 Lua 原子保证，重复投递由主键保证。

## 阶段 1：单机榨干

**原则：先减少每个请求的 IO 次数，再考虑加机器。**

| 位置 | 优化前 | 优化后 |
| --- | --- | --- |
| 登录态续期 | 每个请求 HGETALL + EXPIRE | 距上次续期 > 30 分钟才 EXPIRE（hash 中记 `refreshAt`），绝大多数请求只有 1 次 Redis |
| 登录写 token | putAll、expire 两次往返 | pipeline 一次往返 |
| 商铺详情 | 仅防穿透，热点 key 过期时并发全部打 DB | 互斥锁重建 + 双重检查 + 空值缓存 + 布隆过滤器 |
| 热门笔记 | 每次 `ORDER BY liked`（无索引）+ COUNT + 每条笔记查一次用户、一次 GETBIT | `idx_liked`；按页缓存 60s（±10% 抖动）；去掉 COUNT；作者批量 IN 查询；点赞状态 pipeline 批量 GETBIT |
| 关注 Feed | 同上 N+1，每页只取 2 条 | 批量查询 + pipeline；每页 5 条 |
| 点赞 | GETBIT 判断再 SETBIT（并发可重复计数），3 次 Redis 往返 | Lua 原子切换 bit + TopN ZSet，一次往返；DB 仅在状态真正变化时更新 |
| 发笔记推送粉丝 | 每个粉丝一次 ZADD，收件箱无限增长 | pipeline 批量 ZADD + 截断到最近 1000 条；查粉丝走 `idx_follow_user` |
| 是否关注 | 每次 DB COUNT（无索引） | Redis SISMEMBER；集合缺失时回源 DB 并重建 |
| 订单状态轮询 | 每次查 DB | 订单落库后写 Redis 短缓存（5 分钟），支付/关单时删除 |
| 秒杀落库 | 每条消息：锁 + SELECT + UPDATE 热点库存行 + INSERT | **批量消费**：一批按券聚合成一条 `stock = stock - n` + 多值 INSERT；失败降级逐条，再失败进 DLT |
| 超时关单 | 全表扫描 + 一次性加载 + 单个大事务 | `idx_status_ctime`；每批 500 条独立短事务；库存回补按券聚合；Redis 回补 pipeline |
| 秒杀 Lua | 不校验活动时间 | 校验 `seckill:time:{id}`（启动/新增券时预热） |
| 索引 | 多张表只有主键 | 见 `db/migration_v2.sql` |
| 连接池 | Hikari 默认 10、Lettuce 10，对 200 个 Tomcat 线程 | Hikari 30、Lettuce 50，显式超时 |
| 日志 | `com.hmdp: debug`（SQL、每条消息都打日志） | 默认 info，`--spring.profiles.active=dev` 打开 debug |
| JDBC | 批量插入逐条执行 | `rewriteBatchedStatements=true` |

**本地验证（2000 用户 × 每人抢 2 次，3 副本）**：2000 单成功、2000 次重复下单全部拦截；
DB 库存 = Redis 库存；约 2000 笔订单只执行了约 83 次库存 UPDATE（批量聚合约 24 单/次）；Kafka 无积压。

## 阶段 2：水平扩展

**前提：应用实例无状态**，任何请求可以落到任何实例。

- **会话**：token 存 Redis，天然共享。
- **文件**：`FileStorage` 抽象，默认 `LocalFileStorage` 写共享卷 `hmdp-upload-data`，nginx 直接对外提供；生产换 OSS/MinIO 实现即可。
- **定时任务**：超时关单用 Redisson `tryLock` 保证集群内同一时刻只有一个实例执行。
- **消息消费**：秒杀主题 3 → 12 分区（只能增不能减），消费组并行度 = 分区数；`实例数 × concurrency ≤ 分区数`。
- **网关**（`deploy/frontend/nginx.conf`）：
  - `upstream` 负载均衡到所有 app 副本，`keepalive 64` 复用长连接，失败自动切换副本；
  - 秒杀接口按 IP 限流 `20r/s, burst 40`，超出返回 429；
  - gzip、静态资源 7 天缓存、`X-Real-IP` 透传（验证码 IP 限流依赖）。
- **防刷**：验证码同一手机号 60s 一次、同一 IP 每小时 20 次。
- **部署**：`docker compose -f deploy/docker-compose.yml up -d --scale app=3`，然后 `restart frontend`。

**容量估算**：每个副本 Hikari 30 连接，副本数 × 30 需小于 MySQL `max_connections`（默认 151），
副本超过 4 个时调大 MySQL 连接数或调小 `DB_POOL_MAX`。

## 阶段 3：架构演进（方案）

单机 MySQL / Redis / Kafka 成为瓶颈后，逐项拆分。**按监控数据决定先拆哪个，不要一次全上。**

### 3.1 先上监控（拆分的依据）——✅ 已落地，见 [monitoring.md](monitoring.md)；容量基线工具见 [../perf/baseline/README.md](../perf/baseline/README.md)
- Prometheus + Grafana：JVM、Hikari 活跃连接/等待、Redis 命中率/慢查询、Kafka 消费 lag、MySQL QPS/慢 SQL。
- SkyWalking 链路追踪定位慢接口。
- 告警：DLT 有消息、lag 持续增长、Redis 与 DB 库存不一致（定时对账）。

### 3.2 数据库
- **读写分离**：一主多从，ShardingSphere-JDBC 读写分离；写后立即读的场景（下单后查状态）强制走主库或走缓存。
- **分库分表**：订单表按 `user_id` 取模分片（用户查自己的订单不跨库）；商家维度查询用异构索引表或同步到 ES。
- **全局 ID**：现有 `RedisIdWorker` 按天 INCR 依赖单 Redis，分片后换雪花算法或号段模式（Leaf），去掉一次 Redis 往返。

### 3.3 缓存
- **Redis Cluster**：Lua 涉及多个 key 时必须同槽，秒杀 key 改为 `seckill:{voucherId}:stock` / `seckill:{voucherId}:order` 形式的 hash tag。
- **多级缓存**：Caffeine 本地缓存（店铺详情、热门笔记、商铺类型）+ Redis；热点 key 探测（如京东 hotkey）自动提升到本地缓存。
- **一致性**：Canal 订阅 binlog → MQ → 删除缓存，替代业务代码里的“更新 DB 后删缓存”，并加延迟双删兜底。
- **大 key / 热 key**：`seckill:order:{id}` 用户集合随抢购人数增长，可按 userId 分桶；单 key 库存可拆成多段分片库存分散热点。

### 3.4 消息
- Kafka 3 broker、副本因子 3、`min.insync.replicas=2`，配合已有的 `acks=all` 才是真正不丢。
- **Outbox 本地消息表**：解决“Lua 已扣库存、Kafka send 之前进程崩溃”的残余丢单窗口。
- 超时关单改为延迟队列（RocketMQ 延迟消息 / Redis ZSet 时间轮），取代定时扫表。

### 3.5 业务层
- 点赞数异步化：Redis 计数 + 定时批量回写 DB，DB 不再承受每次点击的行锁。
- Feed 推拉结合：大 V（粉丝数超过阈值）发帖不推送，粉丝读取时再拉取合并，避免写扩散。
- 网关层限流熔断降级（Sentinel / Spring Cloud Gateway），秒杀页静态化 + CDN，前置答题/验证码削峰。

### 3.6 部署
- K8s + HPA 按 CPU / QPS 自动扩缩容；Redis/MySQL/Kafka 使用云托管服务或 Operator。
- 多可用区部署，nginx 前加云负载均衡（SLB）。
