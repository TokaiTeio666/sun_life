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
| 关注 / 取关时 `follows:{userId}` 不在 Redis（2026-11-24 修复） | 直接 `SADD` / `SREM`，建出一个只含本次对象的残缺集合，之后 `isFollow`、共同关注、Feed 查大 V 都按残缺集合判断 | Lua 原子地「存在才改」；不存在时从 DB 整体重建。共同关注求交集前也先补齐两边的集合 |

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

### 1.9 登录态本地短缓存（2026-11-20，阶段 3 的 E4）

| 场景 | 之前 | 之后 |
| --- | --- | --- |
| 带 token 的请求 | 每次一条 `HGETALL login:token:{token}` | 副本内缓存 15s（Caffeine，上限 20 万条），同一 token 每个副本每 15s 最多一次 `HGETALL` |
| `POST /user/logout` | 删 Redis key | 删 Redis key → 删本机缓存 → Redis Pub/Sub（频道 `hmdp:local-cache:evict`）通知其他副本删除 |
| 退出登录后，其他副本上这个 token | 立即失效 | 收到广播后失效（通常是毫秒级）；广播丢失时最多 15s 后失效 |
| token 在 Redis 里过期 / 被手工删除 | 立即失效 | 最多 15s 后失效 |
| 登录态续期 | 距上次续期超过 30 分钟时续期 | 不变；只在本地缓存未命中、刚读过 Redis 时判断 |
| Redis 里的登录态没有 `id` 字段 | 当作已登录、用户 id 为空 | 按未登录处理 |

- 开关：`LOGIN_LOCAL_CACHE_SECONDS`（默认 15），`0` 关闭，回到每个请求一次 `HGETALL`。
- 这是一个安全上的取舍：被踢下线 / 退出登录的 token 在广播丢失时还能在其他副本上用最多一个 TTL。不能接受时调小 TTL 或关闭。
- 每个副本多一条 Redis 订阅连接。这个广播通道是通用的（消息格式 `缓存名|key`），E5 的多级缓存复用它。
- 指标：`cache_gets_total{cache="loginUser",result="hit|miss"}`、`cache_size{cache="loginUser"}`。

本机验证：同一 token 请求 2000 次 `/user/me`，`HGETALL` 次数 关闭时 2000、开启时 1（2 个副本各 1000 次时共 2）。
在副本 1 退出登录后立刻访问副本 2：5 次试验 4 次立即 401；应用启动后的第一次广播那次，紧跟着的一个请求仍返回了 200，1 秒后复查为 401。

### 1.10 多级缓存：本地缓存 + Redis（2026-11-20，阶段 3 的 E5）

| 接口 | 之前 | 之后 |
| --- | --- | --- |
| `GET /shop/{id}` | 每次 `BITFIELD` + `GET` | 副本内 Caffeine 缓存 10s，命中时不访问 Redis；只缓存存在的店铺，不存在的 id 仍由布隆过滤器拦 |
| `GET /shop-type/list` | 每次 `GET` | 同上 |
| `GET /blog/hot` | 每次 `GET`（Redis 里按页缓存 60s） | 同上；`isLike` 仍按当前用户实时计算（每次复制一份再填，不改缓存里的对象） |
| `PUT /shop` | 事务内更新 DB、删 Redis | 另外在事务提交后再删一次 Redis，并删本机本地缓存、广播其他副本删除（复用 1.9 的通道） |

- 开关：`LOCAL_CACHE_ENABLED=false` 关闭本地一级，只用 Redis。TTL 与容量在 `hmdp.cache.local.*`（默认 10s、1 万条，约 30MB 堆）。
- 新鲜度：更新店铺后，本副本立即生效，其他副本收到广播后生效；广播丢失时最多再读到 10s 的旧值。
  商铺类型没有修改接口，直接改库后最多 10s + Redis 的 12 小时。热门笔记的点赞数原本就有约 60s 延迟，现在最多约 70s。
- 不经过 `PUT /shop` 的修改（直接改库）不会触发失效，和原来的 Redis 缓存一样要等过期。
- 指标：`cache_gets_total{cache="l1",result="hit|miss"}`。

本机验证（2 个副本）：`/shop/1`、`/shop-type/list`、`/blog/hot` 各请求 2000 次，Redis 命令总数只增加 1–2 条（改造前约 4000 / 2000 / 2000 条）；
副本 2 读过店铺后在副本 1 上改名，立即读副本 2 得到新名字（3 次都是）；用户 A 点赞热门笔记后，A 读到 `isLike=true`，用户 B 读到 `false`。

### 1.11 秒杀售罄本地标记（2026-11-20，阶段 3 的 E6）

| 场景 | 之前 | 之后 |
| --- | --- | --- |
| 券卖完之后的 `POST /voucher-order/seckill/{id}` | 每个请求一次订单号 `INCR` + 一次 Lua | Lua 返回库存不足后，副本内记 5s 的售罄标记；标记期间直接返回「库存不足」，不访问 Redis |
| 标记期间，已经下过单的用户再次请求 | `不能重复下单` | `库存不足` |
| 标记期间活动结束 | `秒杀已经结束` | 最多 5s 内仍返回 `库存不足` |
| 超时关单回补库存、订单消息发送失败回滚库存、新增秒杀券 | — | 清除本机标记并广播其他副本清除（复用 1.9 的通道，消息 `soldOut\|{voucherId}`） |
| 直接改 Redis 库存（运维、压测重置） | 立即可抢 | 最多 5s 后可抢；`perf/baseline/reset-seckill.sh` 已加上清除标记的广播 |

- 开关：`SECKILL_SOLDOUT_FLAG=false`；标记保留时间 `hmdp.seckill.soldout-ttl-seconds`（默认 5）。
- 被标记直接拒绝的请求计入 `hmdp_seckill_soldout_shortcut_total`，**不再计入** `hmdp_seckill_requests_total{result="sold_out"}`
  （后者只统计真正执行了 Lua 的请求）。看板上的秒杀请求总数要把两者相加。
- 不存在的券 id（库存 key 不存在）同样会被标记 5s。

本机验证（库存 5 的券，1500 个不同用户、20 并发）：

| | 成功 | Lua 执行次数 | 订单号 `INCR` |
| --- | --- | --- | --- |
| `SECKILL_SOLDOUT_FLAG=false` | 5 | 1500 | 1500 |
| 默认（1 个副本） | 5 | 20 | 20 |

两种配置下 DB 订单数都是 5、DB 与 Redis 库存都是 0。2 个副本持续请求（保持两边的标记都有效）时把一笔订单改成超时：
定时任务在 18:58:26.647 关单回补，18:58:26.818 的下一个请求抢到，之后的请求重新返回库存不足。

### 1.12 订单号本地生成（2026-11-20，阶段 3 的 E7）

| | 之前 | 之后 |
| --- | --- | --- |
| 生成方式 | 每个订单号一次 Redis `INCR icr:order:{日期}` | 副本内生成，不访问 Redis |
| id 布局（63 位） | 高 31 位 = 相对起始时间的秒数；低 32 位 = 当天计数 | 高 31 位不变；低 32 位 = `0` `1` + 机器号 10 位 + 秒内序列 20 位 |
| 机器号 | — | 启动时在 Redis 抢占 0–1023 中的一个（`idworker:lease:{n}`，60s 过期），每 20s 续约，停机时释放 |

- 开关：`ORDER_ID_MODE=redis` 回到每个订单号一次 `INCR`。两种 id 靠低 32 位的 bit30 区分，不会重复，可以随时切换、新旧副本可以混跑；
  压测造数的 id 用 bit31，三者互不相交。
- id 仍然大致按时间递增（高位是秒数），但同一秒内不同副本的 id 不再按生成先后排序；数值比以前大（同一秒内约大 2^30）。
  订单号没有被当作连续序号使用，前端和 `/voucher-order/status/{orderId}` 不受影响。
- 最多 1024 个副本；每个副本每秒 104 万个 id，用完时借用下一秒。
- 安全措施：续约失败、或距上次成功续约超过 50s（Redis 里的租约 60s 才过期）时本地发号自动停用，退回 `INCR`，重新抢到机器号后恢复；
  时钟回拨时序列的秒数不后退；续约时把用到的最大秒数记在 `idworker:floor:{n}`，下一个拿到同一机器号的实例从其后 30s 开始。
- 定时任务线程池从 1 调到 4（`spring.task.scheduling.pool.size`），避免超时关单等长任务挡住续约。各任务自身仍是串行执行。

本机验证：2 个副本、2000 个用户 40 并发下单，2000 笔全部成功，订单号 2000 个无重复（两个机器号各约 1000 个），
DB 订单 2000 行、DB 与 Redis 库存都是 98000；期间 `INCR` 0 次（`ORDER_ID_MODE=redis` 时 500 单 500 次）。
手工删除一个副本的租约：下一次续约时发现并重新抢到新机器号。停机后租约立即释放。

### 1.13 点赞数异步回写（2026-11-20，阶段 3 的 E8）

| | 之前 | 之后 |
| --- | --- | --- |
| 点赞接口 | 每次点赞/取消同步 `UPDATE tb_blog SET liked = liked ± 1` | 只写 Redis：Lua 切换位图，并把 ±1 累加到 `blog:like:delta` |
| 写库 | 请求线程内完成 | `LikeCountFlusher` 每 5s 批量回写，同一篇笔记的多次点赞合并成一条 `UPDATE` |
| 笔记存在性 | 由 `UPDATE` 的影响行数判断 | 本地 Caffeine 缓存「已确认存在」的 id（10 分钟、上限 20 万），未命中才查一次主键 |

- 开关：`LIKE_ASYNC_FLUSH=false` 回到同步写库。开关关闭后 `LikeCountFlusher` 仍会运行（把之前累积的增量写完）；
  没有增量时每轮只多一次 Redis `EXISTS`。
- 回写不丢：先把 `blog:like:delta` 原子改名为 `blog:like:delta:flushing`，写库成功后才删除；写库失败则保留，下一轮先重试这一批。
  整批在一个事务里执行，不会只写一半。
- 最多一次重复：写库事务已提交、删除 `flushing` 之前进程被杀时，这一批会在下一轮再加一遍（至少一次语义）。
  窗口只有一次 `DEL` 的时间；点赞数不是账，接受这个误差。
- **可见性延迟**：`tb_blog.liked` 最多滞后一个回写周期（默认 5s，`hmdp.like.flush-interval-ms`）。
  依赖该列的「热门笔记」排序和笔记详情里的点赞数最多 5s 后更新；点赞状态本身（`isLike`）仍由位图实时判断，不受影响。
- 取消点赞多于点赞时，`liked` 最低减到 0（SQL 为 `GREATEST(COALESCE(liked, 0) + ?, 0)`，迁移 PG 前是 `CAST(liked AS SIGNED)`），不会违反 `liked >= 0` 约束。
- 点赞又取消（净 0）时，增量字段由 Lua 的 `HDEL` 删除，不会产生一次空写库。
- 多副本：`LikeCountFlusher` 用 Redisson 锁 `lock:job:like-flush` 保证同一时刻只有一个副本回写。
- 指标：`hmdp_like_flushed_rows_total`（每轮回写更新的行数）、`hmdp_like_flush_failures_total`（失败的轮数）。

本机验证（2026-11-21，MySQL + Redis 本地，单副本）：

| 场景 | 结果 |
| --- | --- |
| 点赞 | 立即 `HGET blog:like:delta` = 1、位图 = 1、DB 未变；≤5s 后 DB +1，`delta` / `flushing` 清空；指标 `flushed_rows` = 2、`failures` = 0 |
| 取消点赞 | 增量 -1、位图 = 0、DB 立即仍是旧值，下一轮 flush 后回到原值 |
| 不存在的笔记 | 返回「笔记不存在！」，Redis 不产生增量 |
| 点赞后立刻取消（净 0） | 增量字段被删除，DB 不变 |
| 10 个用户并发点赞同一篇笔记 | 期间已回写 4 + 残留 6 = 10，最终 DB 正好 +10，无丢失 |
| `LIKE_ASYNC_FLUSH=false` | 点赞后 DB 立即 +1，`blog:like:delta` 不存在；取消后 DB 立即回退 |

未验证：写库中途失败的重试路径、提交后删除前进程被杀（均为代码审查）；多副本锁竞争为单实例实测。

### 1.14 热门笔记与关注 Feed 读路径瘦身（2026-11-24，阶段 3-1 基线后的优化）

起因：本机基线混合场景 200 并发时，`/blog/hot` P99 251 ms、`/blog/of/follow` P99 335 ms，是唯一的应用侧拖尾
（见 [capacity-baseline-report.md](capacity-baseline-report.md) 3.2）。

| | 之前 | 之后 |
| --- | --- | --- |
| 列表正文 | 热门笔记、关注 Feed 返回完整 `content` | 默认不返回（`content` 为 `null`），前端列表页本来就不显示；详情 `GET /blog/{id}` 不变 |
| Feed 取笔记 | 每次请求 `tb_blog` 一次 `IN … ORDER BY FIELD` + `tb_user` 一次 `IN` | 摘要缓存：本地 Caffeine（10s）→ Redis `MGET cache:blog:summary:{id}`（60s ±10%）→ 只查缺的几篇；排序在 Java 里做 |
| Feed 查大 V | 每次请求 `EXISTS follows:{uid}` + `SINTER`；关注集合为空的用户每次还查一次 `tb_follow` | 「我关注的大 V」在副本内缓存 5s；关注 / 取关时广播删除 |
| 列表复制 | 热门笔记每次请求用 Hutool `BeanUtil.copyProperties` 反射复制 10 个对象 | 手写字段拷贝 |

每次请求的访问次数（缓存命中时）：

| 接口 | 之前 | 之后 |
| --- | --- | --- |
| `GET /blog/of/follow` | MySQL 2 次 + Redis 3 次往返 | MySQL 0 次 + Redis 2 次往返（取 ID、判断点赞）；本地摘要未命中时 +1 次 `MGET` |
| `GET /blog/hot` | Redis 1 次（判断点赞） | 不变，只是响应变小、不再反射复制 |

- 开关：`BLOG_LIST_CONTENT=true` 列表恢复返回正文；`BLOG_SUMMARY_CACHE_SECONDS=0` 关闭摘要缓存（Feed 每次查库）；
  `FEED_BIGV_LOCAL_CACHE_SECONDS=0` 关闭大 V 本地缓存。
- **点赞数可见性**：Feed 里的 `liked` 最多滞后 摘要 Redis TTL + 本地 TTL（默认约 70s）+ 点赞回写周期（5s），
  与热门笔记缓存（60s + 本地 10s）同一量级；`isLike` 仍按位图实时判断。点了赞的人在前端本地 +1，不受影响。
- 作者昵称 / 头像跟着摘要缓存，目前没有修改用户资料的接口，所以不存在不一致；以后加了需要同时删 `cache:blog:summary:*`。
- 关注 / 取关：本副本立即生效，其他副本靠 Pub/Sub 广播（`feed-bigv|{userId}`），广播丢失时最多 5s。
  作者刚越过大 V 阈值时，这条及之后的笔记最多晚 5s 出现在粉丝的 Feed 里。
- 不存在的笔记 id 不缓存（Feed 引用的都是已发布笔记，正常不会出现）。
- 大 V 本地缓存不用 `Cache.get(key, loader)`：loader 在 `ConcurrentHashMap.compute` 的锁里执行，访问 Redis 会钉住虚拟线程。

本机验证（2026-11-24，Docker Desktop，单副本，基线造数）：

| 场景 | 结果 |
| --- | --- |
| 热门笔记 | 10 条，响应里没有 `content` 字段；`isLike` 正确 |
| Feed 冷读 / 再读 | 两次结果、`minTime` / `offset` 一致；冷读后生成 5 个 `cache:blog:summary:*`，TTL ≈ 60s，内容与 DB（昵称、点赞数、时间）一致 |
| 关注大 V 后立即读 | 大 V 发件箱的 3 条立即出现在 Feed 里；取关后立即消失（同一副本） |
| 点赞状态隔离 | 测试用户点赞后 Feed / 热门里 `isLike = true`，另一个用户读同一篇仍是 `false`；取消后恢复 |
| `BLOG_LIST_CONTENT=true` + `BLOG_SUMMARY_CACHE_SECONDS=0` + `FEED_BIGV_LOCAL_CACHE_SECONDS=0` | 列表恢复返回正文；Feed 不再写摘要缓存，结果顺序不变 |

未验证：多副本之间的广播删除（与 1.10 同一套 Pub/Sub，未单独测）；尚未压测，性能收益待重跑混合场景拐点。
切换 `BLOG_LIST_CONTENT` 后，已缓存的热门笔记分页最多还按旧格式返回 60s。

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
| `idworker:lease:{0-1023}` | string，TTL 60s | 订单号机器号的租约，值是持有副本的随机标识（见 1.12） |
| `idworker:floor:{0-1023}` | string，TTL 1 天 | 该机器号已用到的最大秒数，防止换实例后时钟回拨造成重复 |
| `icr:order:{日期}` | string 计数 | 行为变化：默认不再使用，只在 `ORDER_ID_MODE=redis` 或租约丢失回退时递增 |
| `blog:like:delta` | hash | 待回写的点赞数增量：field = 笔记 id，value = 尚未写回 `tb_blog.liked` 的增量（见 1.13） |
| `blog:like:delta:flushing` | hash | 正在回写的那一批增量；写库成功后删除，失败则保留到下一轮重试 |
| `lock:job:like-flush` | Redisson 锁 | 点赞数回写任务集群互斥 |
| `cache:blog:summary:{blogId}` | string(JSON)，TTL ~60s | Feed 用的笔记摘要（笔记 + 作者昵称头像，不含 `isLike`），见 1.14 |

## 3. 配置变化（`application.yaml`）

| 配置 | 之前 | 之后 |
| --- | --- | --- |
| `spring.datasource.url` | `serverTimezone=UTC` | `serverTimezone=Asia/Shanghai&rewriteBatchedStatements=true` |
| `spring.datasource.hikari.maximum-pool-size` | 默认 10 | `${DB_POOL_MAX:30}`，`minimum-idle: 10`，`connection-timeout: 3000`（之后改为 `${DB_CONN_TIMEOUT_MS:800}`，见 1.6） |
| `hmdp.db-gate.permits` | 无 | `${DB_GATE_PERMITS:-1}`（负数 = 连接池 × 2，0 = 关闭） |
| `spring.data.redis.lettuce.pool.max-active` | 10 | 50（`max-wait: 2000ms`，`timeout: 2000ms`）；`max-idle` 20 → 50（2026-11-24）：小于 max-active 时归还的连接被关闭再新建，i5 本机 50 并发热门笔记每秒新建约 1100 个连接，改后 200 并发 QPS 2.2k → 9.9k、P99 1.36s → 67ms |
| `server.tomcat.*` | 默认 | `threads.max: 200`，`accept-count: 500`，`max-connections: 8192` |
| `spring.servlet.multipart` | 默认 | `max-file-size: 5MB`，`max-request-size: 10MB` |
| `spring.kafka.consumer.properties.max.poll.interval.ms` | 默认 | 300000 |
| `hmdp.kafka.seckill-partitions` | 无（硬编码 3） | 12 |
| `hmdp.upload.dir` | 无 | `${HMDP_UPLOAD_DIR:./upload}` |
| `hmdp.like.async-flush` / `hmdp.like.flush-interval-ms` | 无 | `${LIKE_ASYNC_FLUSH:true}` / 5000ms；false 回到每次点赞同步写库（见 1.13） |
| `hmdp.blog.list-content` | 无 | `${BLOG_LIST_CONTENT:false}`；列表不返回正文（见 1.14） |
| `hmdp.blog.summary-cache.*` | 无 | `redis-ttl-seconds: ${BLOG_SUMMARY_CACHE_SECONDS:60}`（<= 0 关闭）、`local-ttl-seconds: 10`、`local-max-size: 50000` |
| `hmdp.feed.bigv-local-cache-*` | 无 | `seconds: ${FEED_BIGV_LOCAL_CACHE_SECONDS:5}`（0 关闭）、`max-size: 200000` |
| `logging.level.com.hmdp` | debug | info；`application-dev.yaml` 中为 debug（`--spring.profiles.active=dev`） |

## 4. 数据库变化

### 4.1 新增索引（MySQL 时期由 `db/migration_v2.sql` 补齐，已删除；PG 版 `hmdp.sql` 已包含）

> MySQL 时期的记录。迁移到 PostgreSQL 后 `migration_v2.sql` 已删除，这些索引都在 PG 版 `hmdp.sql` 里，
> 其中 `idx_status_ctime` 换成了部分索引 `idx_voucher_order_unpaid_ctime`，见第 7 节。

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
| 点赞数回写 | 每个实例都执行 | `LIKE_ASYNC_FLUSH` 透传到 app 容器；回写任务由 Redisson 锁保证只有一个实例执行 |
| Kafka 秒杀主题 | 3 分区 | 启动时自动扩到 12 分区（只能增不能减） |
| nginx | 单上游、无长连接复用 | `upstream` + `keepalive 64`、`proxy_next_upstream`、gzip、静态资源 7 天缓存、`X-Real-IP` 透传 |
| 客户端 / 网关 keep-alive | nginx 1.18 默认每连接 100 个请求（1.19 起才是 1000），Tomcat 默认也是 100 | 都调到 `100000`：本机基线 20k QPS 时曾 30s 堆出 8874 个 TIME_WAIT、200 并发 1.33% `BindException`，调大后消失 |
| 旧数据卷升级 | 只有 `hmdp.sql` 初始化时的结构 | （MySQL 时期）早期数据卷需手动执行 `migration_v2.sql` 补索引。迁移到 PG 后该脚本已删除，新数据卷首次启动即包含全部索引；`seed_data.py` 仍会预检索引 |

**容量注意**：每个副本最多 30 个 DB 连接，`副本数 × DB_POOL_MAX` 需小于 PostgreSQL `max_connections`（默认 100，原 MySQL 为 151），见第 7.5 节。

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

---

## 7. 数据库迁移：MySQL 8 → PostgreSQL 16（2026-11-26）

表名、列名和接口都不变，下面是迁移后外部能观察到的差异。迁移动机：更严格的 SQL 与类型约束、部分索引 / 表达式索引等更丰富的索引类型，为后续演进留空间。

### 7.1 实体与查询

| 项 | 之前（MySQL） | 之后（PostgreSQL） |
| --- | --- | --- |
| `BlogComments.status`、`UserInfo.gender`、`UserInfo.level` | `tinyint(1)`，被驱动映射成 `Boolean`（实际取值 0–9，会被截成 true/false） | `smallint`，实体字段改为 `Integer`，JSON 里是数字 |
| 按名称搜索商铺 `/shop/of/name` | `LIKE`，`utf8mb4_general_ci` 不区分大小写 | `ILIKE`，仍不区分大小写（PG 的 `LIKE` 区分大小写）；其他字符串比较现在区分大小写 |
| 点赞 Top5 `/blog/likes/{id}`、附近商铺 `/shop/of/type` | SQL 里 `ORDER BY FIELD(id, ...)` 保持顺序 | `listByIds` 查出后在内存里按 id 列表排序，结果顺序不变；已被删除的 id 直接跳过 |
| 热门笔记分页 | `LIMIT offset, n` | `LIMIT n OFFSET offset` |
| 分页插件 | `DbType.MYSQL` | `DbType.POSTGRE_SQL` |
| JDBC | `rewriteBatchedStatements=true` | `reWriteBatchedInserts=true`，`saveBatch` 仍合并成多值 INSERT |
| 点赞数回写（`LikeCountFlusher`） | `GREATEST(CAST(liked AS SIGNED) + ?, 0)` | `GREATEST(COALESCE(liked, 0) + ?, 0)`（PG 没有 `SIGNED` 类型）；迁移初期这里漏改，回写会整批报错，已修复并验证 |
| 超时关单查询 | `.eq("status", 1)` | `.apply("status = 1")` 字面量，保证能用上部分索引（见 7.4） |
| `tb_blog.liked` | 允许 NULL | `NOT NULL DEFAULT 0`：PG 的 `ORDER BY liked DESC` 把 NULL 排最前（MySQL 排最后），禁止 NULL 消除这个差异 |

### 7.2 约束：非法值直接报错

MySQL 的 `UNSIGNED` 改为 `CHECK`，并给枚举列补了取值约束，写入非法值时 INSERT / UPDATE 报错（`DataIntegrityViolationException`）：

| 表 | 约束 |
| --- | --- |
| `tb_seckill_voucher` | `stock >= 0`；`begin_time` / `end_time` 必填（去掉了 `'0000-00-00 00:00:00'` 默认值） |
| `tb_voucher` | `pay_value >= 0`；`type IN (0, 1)`；`status IN (1, 2, 3)` |
| `tb_voucher_order` | `pay_type IN (1, 2, 3)`；`status BETWEEN 1 AND 6` |
| `tb_blog` | `liked >= 0`；`comments >= 0` |
| `tb_blog_comments` | `liked >= 0`；`status IN (0, 1, 2)` |
| `tb_shop` | `avg_price`、`sold`、`comments >= 0`；`score BETWEEN 0 AND 50` |
| `tb_shop_type` | `sort >= 0` |
| `tb_sign` | `month BETWEEN 1 AND 12`；`is_backup IN (0, 1)` |
| `tb_user_info` | `fans`、`followee`、`credits >= 0`；`gender IN (0, 1)`；`level BETWEEN 0 AND 9` |

自增主键改为 `bigint GENERATED BY DEFAULT AS IDENTITY`；`hmdp.sql` 导入固定 id 的初始数据后用 `setval` 把序列推到最大 id 之后。
唯一索引 `uniqe_key_phone` 改名为 `uk_user_phone`。

### 7.3 `update_time` 自动更新

MySQL 的 `ON UPDATE CURRENT_TIMESTAMP` 由触发器 `set_update_time()` 实现：UPDATE 语句**没有显式修改** `update_time` 时写入当前时间，
显式写了就保留写入的值，与 MySQL 行为一致。时间列是不带时区的 `timestamp`，数据库时区固定为 `Asia/Shanghai`，与应用的 `LocalDateTime` 对齐。
小差异：MySQL 只在行内容真的变化时刷新，触发器对「值没变的 UPDATE」也会刷新。

### 7.4 索引与热点行

- **部分索引**：超时关单扫描 `WHERE status = 1 AND create_time < ? ORDER BY create_time LIMIT n` 改用
  `idx_voucher_order_unpaid_ctime ON tb_voucher_order (create_time) WHERE status = 1`，只索引未支付订单，订单表再大索引也只有待关单的那一小部分。
- **`status = 1` 必须写成字面量**：`closeTimeoutOrders` 用 `.apply("status = 1")` 而不是 `.eq("status", 1)`。
  绑定参数 `status = ?` 在 pgjdbc 切到服务端预编译（同一语句执行 5 次后）后，PG 可能选通用计划，无法证明参数等于 1，就用不上部分索引。
  以后给其他部分索引写查询时同样要注意。
- **HOT 更新**：`tb_seckill_voucher` 设 `fillfactor = 70`。PG 的 UPDATE 是写一个新版本行（MVCC），页内留了空间、且没有改到索引列时，
  新版本留在同一页（HOT），不用改索引，死元组由页内清理回收，适合 `stock = stock - n` 这种热点行高频更新。
  例外：`tb_blog.liked` 上有 `idx_liked`，点赞数回写每次都改索引列，不能走 HOT，会产生索引膨胀，靠 autovacuum 回收；
  点赞数异步批量回写（1.13）正好减少了这类更新的次数。

### 7.5 部署与运维

| 项 | 之前 | 之后 |
| --- | --- | --- |
| 数据库服务 | `mysql`（mysql:8.0），容器 `hmdp-mysql`，宿主端口 3307 | `postgres`（postgres:16.4），容器 `hmdp-postgres`，宿主端口 5433（`PG_PORT`） |
| 数据卷 | `hmdp-mysql8-data` | `hmdp-pg16-data`。**旧 MySQL 数据不迁移**：初始数据由 `hmdp.sql` 导入，压测数据用 `perf/baseline/seed_data.py` 重新生成；旧卷不再使用，确认不需要后可手动删除 |
| 环境变量 | `MYSQL_ROOT_PASSWORD`、`MYSQL_PORT`、`APP_MYSQL_ADDR`、`MYSQL_MAX_CONNECTIONS`、`MYSQL_BUFFER_POOL`、`MYSQL_REDO_LOG` | `POSTGRES_PASSWORD`、`PG_PORT`、`APP_PG_ADDR`、`PG_MAX_CONNECTIONS`、`PG_SHARED_BUFFERS`、`PG_EFFECTIVE_CACHE_SIZE`、`PG_MAX_WAL_SIZE` |
| 慢查询 | 慢查询日志 `slow.log` + `Slow_queries` 计数 | 超过 200ms 的语句写容器日志（`docker logs hmdp-postgres`）；Top SQL 来自 `pg_stat_statements`；没有慢查询次数指标，所以去掉了慢查询告警 |
| 监控 | mysqld_exporter（:9104） | postgres-exporter（:9187），开启 `stat_statements` 采集器，自定义查询在 `deploy/monitoring/postgres-exporter/queries.yaml`；告警改为 PostgresDown / PostgresConnectionsHigh / PostgresLockWaits / PostgresDeadlocks / PostgresExporterDown，见 [monitoring.md](monitoring.md) |
| 连接数上限 | `max_connections` 默认 151 | 默认 100。PG 每个连接一个后端进程，`副本数 × DB_POOL_MAX` 要明显小于它；横向扩副本时考虑在前面加 PgBouncer（transaction 模式） |
| 压测工具 | sysbench + `sql-bench.lua` | pgbench（随 `postgres:16.4` 镜像提供），见 `perf/baseline/README.md`；MySQL 时期的压测数字需要在 PG 上重测 |
