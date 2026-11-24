> 状态：**3-1 评估规划已落地（2026-11-19）**：监控体系与基线工具已就绪，容量基线数据待在独立压测环境实测（见 3-1.5）；3-2 起仍为方案。
> 承接 [scaling-optimization-plan.md](scaling-optimization-plan.md) 中的「阶段 3：架构演进」，以及 [../docs/scaling-roadmap.md](../docs/scaling-roadmap.md) 的 3.1–3.6。
> 阶段 0–2 已落地：单机优化、应用无状态、多副本 + nginx 负载均衡与限流、Kafka 12 分区。

# 阶段 3 技术方案：架构演进（按生命周期四阶段）

## Context

阶段 2 之后，应用层可以靠加副本水平扩展。剩下的瓶颈和单点都在存储与中间件：MySQL、Redis、Kafka 都只有一个节点。
阶段 3 的目标**不是一次性把所有组件都换成集群**，而是按「先观测 → 再改造 → 验证上线 → 持续治理」的生命周期推进。
每一步都用监控数据证明有必要再做，避免过早引入复杂度。

| 生命周期阶段 | 目标 | 产出 | 进入下一阶段的门槛 |
| --- | --- | --- | --- |
| **3-1 评估规划** | 看清现状：真实容量、瓶颈在哪、业务目标是多少 | 监控体系、容量基线、容量模型、改造优先级 | 有基线数据，并且确认了先改哪个组件 |
| **3-2 设计改造** | 按优先级分批改造存储、缓存、消息与业务层 | 代码与部署改造（分 B/C/D 三批，每批独立可回滚） | 每批功能测试和一致性校验都通过 |
| **3-3 验证上线** | 证明改造后容量达标、故障可恢复、数据不出错 | 压测报告、故障演练报告、迁移与灰度记录 | 达到验收指标，回滚方案演练成功 |
| **3-4 运行治理** | 长期稳定运行，容量随业务滚动更新 | SLO、告警、预案、例行压测与演练 | 持续进行；新的瓶颈出现时回到 3-1 |

---

## 3-1 评估规划阶段

### 3-1.1 监控体系（拆分的依据，最先做）

| 层 | 工具 | 关键指标 |
| --- | --- | --- |
| 应用 | Spring Boot Actuator + Micrometer → Prometheus → Grafana | 各接口 QPS / 错误率 / P95 / P99（RED）、JVM GC 与堆、Tomcat 繁忙线程数、Hikari `active` / `pending`、Lettuce 命令耗时 |
| 链路 | SkyWalking（Java agent，零侵入） | 慢接口耗在 Redis、DB 还是 Kafka 哪一段 |
| Redis | redis_exporter | ops/s、CPU、内存使用率、命中率、慢查询、大 key / 热 key、连接数 |
| MySQL | mysqld_exporter + 慢查询日志 | QPS / TPS、行锁等待、连接数、慢 SQL、主从延迟（读写分离后） |
| Kafka | kafka-exporter | 消息流入速率、消费组 lag、DLT 消息数 |
| 业务 | 自定义 Micrometer 指标 | 秒杀成功 / 售罄 / 重复下单计数、批量落库聚合度、超时关单数、Redis 与 DB 库存差值 |

需要改的代码：`pom.xml` 加 `spring-boot-starter-actuator` 和 `micrometer-registry-prometheus`；
`application.yaml` 暴露 `/actuator/prometheus`，网关不对外暴露该路径；
`deploy/` 下新增 prometheus、grafana、exporter 的 compose 片段。

### 3-1.2 容量基线怎么测

**先看懂现有数据**：`perf/README.md` 里秒杀并发 200 / 500 / 1000 时，QPS 都约 1 万，平均响应时间约 20 / 49 / 101ms。
按 Little 定律（QPS = 并发数 ÷ 平均响应时间）验算，200 ÷ 20.4ms ≈ 9.8k、500 ÷ 49.1ms ≈ 10.2k、1000 ÷ 100.7ms ≈ 9.9k。
并发翻倍，QPS 不变，响应时间跟着翻倍，说明**大约 1 万 QPS 时已经饱和**，多出的并发只是在排队。
但当时所有组件和压测机都在同一台机器上，这个数字是**整机上限**，不能当作应用上限。

**测法（压测机必须与被测服务分开部署）**：

1. **组件单测**：分别找出各组件自己的上限。
   - Redis：`redis-benchmark` 测 GET / HGETALL / SISMEMBER；用 `EVALSHA` 单独测 `seckill.lua` 和点赞 Lua。
   - MySQL：`sysbench oltp_point_select` / `oltp_write_only`，并用业务真实 SQL 做 `mysqlslap`：热门笔记、超时关单扫描、`stock = stock - n`、多值 INSERT。
   - Kafka：`kafka-producer-perf-test.sh` / `kafka-consumer-perf-test.sh`。
2. **单实例找拐点**：给单个 app 副本逐级加并发，直到 P99 超过 SLO 或错误率超过 0.1%，这个点就是拐点。**安全容量按拐点的 60–70% 取**。
3. **测扩展效率**：分别用 1 / 2 / 4 / 8 个副本压测，算出 `扩展效率 = N 副本 QPS ÷ (N × 单副本 QPS)`。
   哪个副本数之后效率掉到 0.7 以下，就说明某个共享组件到顶了，这个组件就是改造的第一优先级。
4. **混合场景**：按真实流量配比压测，例如店铺详情 70%、笔记 / Feed 25%、秒杀 5%。只压单接口得出的结论偏乐观。

### 3-1.3 容量模型

**业务目标 → QPS**

```
平均 QPS = DAU × 人均每日请求数 ÷ 86400
峰值 QPS = 平均 QPS × 峰值系数（日常 3–5；秒杀开始时刻 10–100）
```

例：1000 万 DAU × 50 次 ÷ 86400 × 5 ≈ 2.9 万 QPS。说明阶段 2 已经能覆盖相当大的用户量，
阶段 3 更多是为了应对**数据量（订单亿级）、单点可靠性和秒杀尖峰**。

**QPS → 各组件负载**

```
组件负载 = Σ(接口 QPS × 该接口每次请求访问该组件的次数) × (1 − 上一层缓存命中率)
所需节点数 = 组件负载 × 冗余系数 1.5 ÷ 单节点安全容量
所需 app 实例数 = 峰值 QPS × 1.5 ÷ (单实例安全容量 × 扩展效率)
```

**单节点经验容量**（只用来定数量级，最终以 3-1.2 的实测为准）：

| 组件 | 单节点经验值 | 扩展方式 | 注意 |
| --- | --- | --- | --- |
| app（4C8G） | 3–5k QPS（带 Redis / DB 访问的接口） | 加副本，线性扩展 | 受连接池和下游限制 |
| Redis 简单命令 | 8–10 万 ops/s | Cluster 有 N 个主节点 ≈ N × 单节点 | 每个请求校验登录态要 1 次 HGETALL，这是固定开销 |
| Redis 执行 `seckill.lua` | 3–5 万次/s | **单张券的 key 只在一个分片上，加节点无效** | 只能靠削峰、售罄本地标记、库存分段 |
| MySQL 主库写 | 几千到 1 万 TPS | 分库后约等于库数 × 单库 | 秒杀批量聚合约 24 单 / 次，写压力已大幅下降 |
| MySQL 从库读 | 1–2 万 QPS | 加从库 | 大部分读应该被缓存挡掉 |
| Kafka | 每个分区每秒几万条 | 加分区、加 broker | 瓶颈通常在消费端写库 |
| Caffeine 本地缓存 | 不走网络，每实例每秒几万次以上 | 随实例扩展 | 一致性靠短 TTL + 广播失效 |

**示例推演（全站 100 万读 QPS）**：每个请求都要 1 次 HGETALL 校验 token，Redis 就要承受 100 万 ops/s，
需要约 13 个主节点，留出余量要 20 多个。所以到这个量级，**登录态也必须本地短缓存或改用无状态 token**（见 3-2 批次 B）。

### 3-1.4 产出与决策

- Grafana 大盘 + 第一版告警；
- 《容量基线报告》：各组件单测上限、单实例拐点、扩展效率曲线、混合场景结果；
- 按「瓶颈组件 × 改造成本 × 风险」排出 3-2 的执行顺序。**默认顺序是 B → C → D，但以基线数据为准**。

### 3-1.5 落地记录（2026-11-19）

| 产出 | 位置 | 状态 |
| --- | --- | --- |
| 应用指标：Actuator + Micrometer Prometheus，HTTP 直方图分位数，Tomcat / Hikari / Lettuce / JVM / Kafka 客户端 | `pom.xml`、`application.yaml` | ✅ |
| 业务指标：秒杀结果、发送失败与回滚、落库数、批量聚合度、重复投递、降级、死信、超时关单、Redis 与 DB 库存差值 | `metrics/BizMetrics`、`metrics/SeckillStockGapSampler` | ✅ |
| Prometheus + Grafana + redis / mysqld / kafka exporter（+ 可选 cAdvisor） | `deploy/docker-compose.monitoring.yml`、`deploy/monitoring/` | ✅ |
| SkyWalking 链路追踪（可选叠加） | `deploy/docker-compose.tracing.yml` | ✅ |
| Grafana 大盘（8 组 43 个面板）+ 第一版告警（21 条告警 + 5 条记录规则，含 runbook） | `deploy/monitoring/`、`docs/monitoring.md` | ✅ |
| MySQL 慢查询日志（> 200ms） | `deploy/docker-compose.yml` | ✅ |
| 组件单测 / 单实例拐点 / 扩展效率 / 混合场景 / 容量模型工具 | `perf/baseline/`、`perf/mixed-bench.jmx` | ✅ |
| 《容量基线报告》 | `docs/capacity-baseline-report.md` | ✅ 组件单测 + 单实例拐点已按**本机受控基线**填写（2026-11-24，被测机 = 压测机，只作下限）；⏳ 分机容量基线（扩展效率）待有条件时实测 |
| 3-2 执行顺序 | 同上第 7 节 | ⏳ 依赖基线数据 |

与方案的偏差：

| 方案 | 实际 | 原因 |
| --- | --- | --- |
| 暴露 `/actuator/prometheus`，网关不对外暴露该路径 | 管理端口独立为 9081，只在容器网络内可达；nginx 另对 `/actuator` 返回 404 兜底 | 端口隔离不依赖网关配置，也不受登录拦截器影响 |
| 用 `mysqlslap` 测业务 SQL | 改用 sysbench + 自定义 Lua 脚本（`perf/baseline/sql-bench.lua`），并临时创建 `mysql_native_password` 压测账号 | `mysql:8.0` 镜像不含 mysqlslap；sysbench 1.0 的客户端不支持 `caching_sha2_password` |
| — | 业务指标不以 `created` 结尾、DistributionSummary 不设 baseUnit | 实测发现 `*_created` 被 OpenMetrics 当作保留后缀丢弃，baseUnit 会被拼进指标名 |
| — | 未部署 Alertmanager，告警在 Prometheus 中可见 | 本地无通知渠道；上线时按 `severity` 标签路由 |
| SkyWalking 常驻 | 独立叠加编排，按需开启；OAP 用 H2、数据保留 2 天（10.x 下限）；agent 忽略 `/actuator/**` | OAP 较重，agent 有开销，做拐点压测时不应开启 |
| 容量基线在压测机与被测服务分离的环境实测 | 2026-11-24 先做**本机受控基线**（被测机 = 压测机），扩展效率未测；分机步骤保留在 `docs/bench-deploy.md` | 不再长期租服务器；本机数字只作下限，进入 3-2 前需补一次分机 scale 测试 |

本地验证（单机 Docker Desktop，2 副本）：Prometheus 全部 target 正常（cAdvisor 未启用除外）、26 条规则（21 告警 + 5 记录）全部健康；
看板和规则引用的 57 个指标名在 Prometheus 中全部存在；200 用户秒杀后 `hmdp_seckill_orders_persisted_total` = DB 订单数，
库存差值收敛到 0；`/actuator` 经网关 8080 / 8081 均返回 404；组件单测、`baseline.py knee / scale` 均实跑通过；
SkyWalking 能看到 `/shop/{id}` 等请求的 Redis / MySQL 子调用。

链路追踪顺带发现：`/shop/{id}` 每次请求有 **7 次逐个 GETBIT**（布隆过滤器按 hash 逐位检查，未 pipeline）+ 1 次 GET，
登录用户再加 1 次 HGETALL，共 9 次 Redis 往返。这是容量模型里店铺详情 Redis 次数的实测值，
也是批次 B 的一个低成本改造点（布隆过滤器改为 pipeline 或 Lua，一次往返）。
同机结果不作为基线（压测客户端与被测服务抢 CPU）。

---

### 3-1.6 下一步：进入 3-2 之前要做的事（2026-11-19）

进入 3-2 的门槛是「有基线数据，并且确认了先改哪个组件」。基线还有第二个用途：3-3 验收需要**改造前**的数据做对比。
因此不要求一次跑完全部 5 步，但第 2、3 步必须先做。

**A. 基线测量**（步骤见 [../perf/baseline/README.md](../perf/baseline/README.md)，结果填进 [../docs/capacity-baseline-report.md](../docs/capacity-baseline-report.md)）

| # | 事项 | 必要性 | 状态 |
| --- | --- | --- | --- |
| A1 | 准备压测环境：至少另借一台机器只跑 JMeter；暂时没有就本机同机测（绝对值偏低，但改造前后对比仍然有效，环境与参数要保持一致） | 前置 | ⏳ |
| A2 | 第 2 步：单实例拐点，**用 `perf/mixed-bench.jmx`**，同时存一份店铺详情、秒杀的单接口拐点 | **必须**（改造前对照数据） | ⏳ |
| A3 | 第 3 步：扩展效率 1 / 2 / 4 / 8 副本（混合场景），同时段截图 Grafana 的 Redis / MySQL / Kafka / 连接池面板 | **必须**（决定 B / C / D 的顺序） | ⏳ |
| A4 | 第 1 步：组件单测 `component-bench.sh` | 建议（解释 A3 里为什么是那个组件到顶） | ⏳ |
| A5 | 第 5 步：容量模型，用 A2–A4 的实测值替换经验值 | 可后补（回答「要扩多少台」，不影响「先改什么」） | ⏳ |
| A6 | 填写报告第 7 节「改造优先级」，确认 3-2 的执行顺序 | **门槛** | ⏳ |

> 同机测量时扩展效率会被严重低估（工具验证时 2 副本只有 0.24），这种情况下以 Grafana 上「哪个组件先饱和」为准。

**B. 可与基线并行的批次 B 低风险项**（不依赖基线结论；动手前必须先完成 A2，留下改造前数据）

| # | 事项 | 依据 | 涉及代码 | 状态 |
| --- | --- | --- | --- | --- |
| B1 | 布隆过滤器检查从 7 次逐个 GETBIT 改为一次往返（pipeline 或 Lua） | SkyWalking 实测：`/shop/{id}` 每次 7 × GETBIT + 1 × GET | `utils/RedisBloomFilter`、`ShopServiceImpl` | ✅ 2026-11-20，改为一条 `BITFIELD`（见 3-1.7） |
| B2 | 秒杀售罄本地标记：Lua 返回库存不足后实例内记 `soldOut[voucherId]`，后续请求直接返回；超时关单回补库存时广播清除 | 单券 Lua 是单 key 热点，加 Redis 节点无效 | `VoucherOrderServiceImpl`、`OrderTimeoutScheduler` | ⏳ |

每项完成后用同样的参数重跑 A2，把前后对比写进报告；`hmdp.cache.*` 之类的功能开关按 3-3.5 预留。

**C. 排序待定**：批次 B 其余项（多级缓存、登录态本地缓存、点赞异步化、Feed 推拉结合、Sentinel、线程池隔离）以及批次 C / D，
等 A6 确定优先级后再排。

---

### 3-1.7 调整：基线实测暂缓，先做不依赖基线的改造（2026-11-20）

**决定**：A 组的基线实测先放一放（本机缺 JMeter / JDK，且同机测不出可信的扩展效率；分机环境见
[../docs/bench-deploy.md](../docs/bench-deploy.md)，待租机器后执行）。在此期间只做**不看数据也能确定**的改造，
读写分离、分库分表、库存分段、Redis Cluster 等高成本项仍然等基线数据。

**与 3-1.6 的偏差**：3-1.6 要求「动手前先完成 A2，留下改造前数据」。现在没有改造前基线，用两条措施弥补：

1. 每项改造都带功能开关（环境变量），补测时可以逐项关掉，在同一环境里测出改造前后的差值；
2. 每项完成时记录不依赖压测环境的指标：每请求的 Redis 命令数（`redis-cli info commandstats` 前后相减）、
   每请求的 SQL 条数、功能回归结果。吞吐类数字只在同机做开 / 关对比，标注「同机，不作基线」。

#### 已完成（2026-11-20）

| 事项 | 结果 | 提交 |
| --- | --- | --- |
| 压测造数 `perf/baseline/seed_data.py`：100 万用户 / 10 万店铺 / 1000 万订单，含券、笔记、关注、Redis 店铺坐标与收件箱 | 一次性容器内全量跑通，数据 + 索引约 3.1 GB | `95a0348`、`d131aee` |
| 分机部署：中间件地址、Kafka 对外监听器、JVM 参数、MySQL 容量参数可配置；应用机覆盖文件；`bench.env.example` | compose 配置与 Kafka 双监听器已验证；**未在真实多机环境启动过** | 同上 |
| B1 布隆过滤器改为一条 `BITFIELD` | `/shop/{id}` 每请求 Redis 命令 8 → 2；与旧位图兼容 | `dc70094` |
| JDK 21 + 虚拟线程（只切换 Tomcat 请求线程，开关 `VIRTUAL_THREADS`）；MySQL 驱动 9.1.0 | 同机 2 核 200 并发：走缓存约 1.5k → 6k QPS，直查 MySQL 约 0.9k → 4.5k QPS（不作基线） | `dc70094` |

虚拟线程带来的两个后续影响，下面 E1 处理：Tomcat 不再有 200 线程的上限，过载时请求全部堆到 Hikari 连接池上；
`tomcat_threads_*` 指标失效，应用饱和改看 `hikaricp_connections_pending` 与接口 P99。

#### 待做：第一组，规模上来必然出问题的（按顺序执行）

| # | 事项 | 现状与问题 | 做法 | 涉及代码 | 开关 | 验证 | 状态 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| E1 | **过载保护** | 等 DB 连接最长 3s（`connection-timeout: 3000`）；虚拟线程下并发不受限，过载时大量请求一起等 3s 再失败，拖垮全部接口 | ① 等连接超时降到 800ms；② 回源 DB 的入口加并发闸（信号量，默认 = 连接池大小 × 2），拿不到许可立即返回「系统繁忙」；③ `WebExceptionAdvice` 把获取连接超时映射为同样的繁忙响应（HTTP 503），不再当作 500 | `application.yaml`、`config/WebExceptionAdvice`、`utils/CacheClient`（回源处）、新增 `utils/DbGate` | `DB_GATE_PERMITS=0` 关闭并发闸；`DB_CONN_TIMEOUT_MS` | 关缓存压 `/shop/{id}`：过载时 P99 不超过约 1s，失败请求返回 503 而不是超时；正常流量错误率不变 | ✅ 2026-11-20。同机过载对比：全部请求 P99 3202ms → 103ms，成功请求平均耗时约 2.6s → 0.12s；每请求 Redis 命令数 / SQL 条数不变。数据与范围说明见 [behavior-changes.md](../docs/behavior-changes.md) 1.6 |
| E2 | **布隆过滤器容量** | `EXPECTED_INSERTIONS = 10_000` 写死；10 万家店时位图几乎写满，对不存在的 id 基本不拦截。启动预热把全部 id 放进一个 pipeline | ① 容量与误判率改为配置（`hmdp.bloom.expected-insertions`，默认 100 万）；② key 带上位图长度（`bloom:shop:{numBits}`），容量变化时写入新 key，旧 key 设 1 天过期，滚动发布期间新旧副本各用各的；③ 预热分批（每批 1000 个 id）并按 id 游标分页读库；④ 启动日志打印实际填充率，超过设计值告警 | `utils/RedisBloomFilter`、`config/BloomFilterInitializer`、`application.yaml` | 已有 `HMDP_CACHE_ENABLED` | 造数 10 万店后：存在的 id 全部通过；随机 10 万个不存在的 id 误判率 < 2% | ✅ 2026-11-20。10 万家店：存在的 id 全部通过，不存在的 id 误判率 95.9% → 0%（填充率 99% → 5.14%）。与原方案的差异：key 为 `bloom:shop:{numBits}:{k}`；增加了就绪标记和定时增量同步（见 [behavior-changes.md](../docs/behavior-changes.md) 1.7） |
| E3 | **大 V 发笔记的推送** | `BlogServiceImpl.saveBlog` 在请求线程里同步查出全部粉丝并 pipeline 推送；造数中 1 号创作者约 6 万粉丝，一次发帖约 12 万条 Redis 命令，期间占着请求和 Redis 连接 | ① 发帖只落库并发一条 Kafka 消息（新 topic `blog.published`，key = 作者 id）；② 消费者按粉丝 id 游标分页（每页 1000）查粉丝、分批 pipeline 推送；③ 推拉结合：粉丝数超过阈值（默认 5000）的作者不推送，只写自己的发件箱 `outbox:{userId}`，粉丝读 Feed 时合并收件箱与所关注大 V 的发件箱（对应批次 B「Feed 推拉结合」） | `service/impl/BlogServiceImpl`、`mq/` 新增消费者、`config/KafkaConfig`、`utils/RedisConstants` | `FEED_ASYNC_PUSH=false` 回到同步推送；`FEED_BIGV_THRESHOLD` | 6 万粉丝的作者发帖：接口耗时与粉丝数无关；普通作者的粉丝数秒内可见；大 V 的笔记出现在粉丝 Feed 且时间线顺序正确 | ✅ 2026-11-20。6.3 万粉丝的作者发帖 2–3.5s → 约 30ms，推送的 Redis 命令约 12.6 万条 / 帖 → 0；普通作者的粉丝约 0.1s 可见。与原方案的差异：发件箱对所有作者都写；大 V 标记只增不减；顺带修了滚动分页 `offset` 写反的问题；消息发送失败会丢推送（待批次 C 的 Outbox）。见 [behavior-changes.md](../docs/behavior-changes.md) 1.8 |

#### 待做：第二组，减少每个请求的 IO（E1–E3 之后）

| # | 事项 | 现状 | 做法 | 涉及代码 | 开关 | 验证 | 状态 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| E4 | **登录态本地短缓存** | 每个带 token 的请求一次 `HGETALL` | Caffeine 缓存 token → 用户，TTL 10–30s、容量上限；退出登录时删本机缓存并通过 Redis Pub/Sub 广播其他副本删除；续期逻辑保持「距上次续期超过 30 分钟才写 Redis」 | `utils/RefreshInterceptor`、`controller/UserController`（退出）、`pom.xml`（caffeine） | `LOGIN_LOCAL_CACHE_SECONDS=0` 关闭 | 带 token 连续请求：`hgetall` 次数 ≈ 用户数 ÷ TTL，而不是请求数；退出后其他副本在 TTL 内失效 | ✅ 2026-11-20。TTL 取 15s；同一 token 2000 次请求的 `HGETALL` 2000 → 1；跨副本退出通常立即生效，最坏 15s。续期判断移到本地未命中时。见 [behavior-changes.md](../docs/behavior-changes.md) 1.9 |
| E5 | **多级缓存** | 店铺详情、商铺类型、热门笔记每次读 Redis | Caffeine + Redis：本地 TTL 5–30s、容量上限；更新店铺时删 Redis 并广播删本地（复用 E4 的广播通道） | `utils/CacheClient`、`service/impl/ShopServiceImpl`、`ShopTypeServiceImpl`、`BlogServiceImpl` | `LOCAL_CACHE_ENABLED=false` | 热点店铺请求的 Redis 命令数趋近 0；更新店铺后各副本在秒级内读到新值 | ✅ 2026-11-20。本地 TTL 10s、上限 1 万条；三个接口热点读的 Redis 命令数降到约 0；更新店铺后另一副本立即读到新值。本地缓存做成独立的 `utils/LocalCache`（没有放进 `CacheClient`）；更新店铺改为事务提交后再删一次缓存。见 [behavior-changes.md](../docs/behavior-changes.md) 1.10 |
| E6 | **秒杀售罄本地标记**（原 B2） | 售罄后请求照样执行 Lua | Lua 返回库存不足后在实例内记 `soldOut[voucherId]`，后续直接返回；超时关单回补库存时广播清除；标记带短 TTL 兜底 | `VoucherOrderServiceImpl`、`OrderTimeoutScheduler` | `SECKILL_SOLDOUT_FLAG=false` | 售罄后 `evalsha` 次数不再增长；关单回补后能继续抢到 | ✅ 2026-11-20。标记 TTL 5s；1500 次请求的 Lua 执行 1500 → 20 次（订单号 `INCR` 同样减少）；关单回补后下一个请求即抢到。发送失败回滚、新增秒杀券、压测重置脚本也会清除标记。见 [behavior-changes.md](../docs/behavior-changes.md) 1.11 |
| E7 | **订单号本地生成** | 每单一次 Redis `INCR`（`RedisIdWorker`） | 雪花算法：时间戳 + 机器号 + 序列；机器号启动时从 Redis 抢占并续约（对应批次 C「全局 ID」） | `utils/RedisIdWorker`、`VoucherOrderServiceImpl` | 保留旧实现，配置切换 | 多副本并发生成无重复；秒杀每请求 Redis 命令数少 1 | ✅ 2026-11-20。没有用标准雪花布局：保留「高 31 位秒数」，低 32 位改为 标记 + 机器号 10 位 + 序列 20 位，与旧 id、造数 id 都不相交，可随时切换。2 副本 2000 单无重复，`INCR` 2000 → 0。租约丢失自动退回 Redis 发号。见 [behavior-changes.md](../docs/behavior-changes.md) 1.12 |
| E8 | **点赞数异步回写** | 每次点赞更新一次 `tb_blog` 行 | Redis 计数 + 脏集合，定时批量写回 DB（对应批次 B「点赞数异步化」） | `BlogServiceImpl`、新增定时任务 | `LIKE_ASYNC_FLUSH=false` | 点赞接口不再产生 UPDATE；定时回写后 DB 点赞数与 Redis 一致 | ✅ 2026-11-21。本机单副本（MySQL + Redis 本地）实测：点赞后 DB 最多滞后 5s 再 +1，`delta` / `flushing` 清空；取消点赞同样异步回退；不存在的笔记不产生增量；净 0 的点赞不写库；10 个用户并发点赞同一篇笔记，期间已回写 4 + 残留 6，最终 DB 正好 +10；`LIKE_ASYNC_FLUSH=false` 时 DB 立即更新且无增量 key。未实测写库失败重试与删除前进程被杀（代码审查）。见 [behavior-changes.md](../docs/behavior-changes.md) 1.13 |

#### 仍然等基线数据的

- 批次 C：Outbox 防丢单、Kafka 3 节点、MySQL 主从与读写分离、延迟队列关单。其中 Outbox 和高可用属于「确定要做」，
  但涉及部署形态，和读写分离一起排；
- 批次 D：Redis Cluster、秒杀库存分段、订单分库分表、Canal 缓存一致性；
- 批次 B 剩余项：热 key 探测、Sentinel 限流熔断、线程池隔离（E1 的并发闸是其简化版，接入 Sentinel 后可替换）。

#### 执行约定

- 顺序：E1 → E2 → E3，然后 E4 → E5 → E6 → E7 → E8；每项单独提交，提交信息写明开关名；
- 每项完成后在本表更新状态，并把「每请求 Redis 命令数 / SQL 条数」的前后值记到
  [../docs/capacity-baseline-report.md](../docs/capacity-baseline-report.md) 第 5 节和 `perf/baseline/capacity-model.example.json`；
- 行为变化（例如过载时返回 503、Feed 对大 V 改为拉取）同步写进 [../docs/behavior-changes.md](../docs/behavior-changes.md)；
- 补测基线时，先在全部开关关闭的状态下测一轮作为「改造前」，再逐项打开。

---

## 3-2 设计改造阶段

分三批，每批之间互不依赖或只单向依赖，**每批都能独立上线、独立回滚**。

```
批次 B（不改存储结构，低风险） ──┐
                                 ├──> 批次 D（存储层拆分，高风险，按需）
批次 C（可靠性 + ID + 读写分离）──┘
```

### 批次 B：缓存与业务层减压（低风险，收益最大）

| 项 | 方案 | 涉及代码 |
| --- | --- | --- |
| **多级缓存** | Caffeine 本地缓存（店铺详情、商铺类型、热门笔记首页）+ Redis。本地 TTL 短（5–30s），容量上限 + LRU | `utils/CacheClient`、`ShopServiceImpl`、`ShopTypeServiceImpl`、`BlogServiceImpl` |
| **本地缓存失效** | 更新 DB 后删 Redis，再通过 Redis Pub/Sub（或 Kafka 广播 topic）通知所有实例删本地缓存；本地短 TTL 兜底 | 新增 `cache/LocalCacheInvalidator` |
| **热 key 探测** | 实例内滑动窗口统计 key 访问频率，超过阈值自动提升到本地缓存（参考京东 hotkey 的思路，先做单机版） | 新增 `cache/HotKeyDetector` |
| **登录态减压** | `RefreshInterceptor` 对 `login:token:*` 做 10–30s 本地缓存。退出登录 / 封禁时广播失效。中长期可评估 JWT + 黑名单 | `utils/RefreshInterceptor` |
| **秒杀售罄本地标记** | Lua 返回库存不足后，在实例内存记录 `soldOut[voucherId]=true`，之后的请求直接返回，不再访问 Redis；超时关单回补库存时广播清除 | `VoucherOrderServiceImpl`、`OrderTimeoutScheduler` |
| **点赞数异步化** | 点赞只改 Redis（Lua 切换 bit + `blog:liked:count:{id}` 计数），变更的 blogId 记入脏集合；定时任务每 5–10s 批量 `UPDATE tb_blog SET liked = ? WHERE id = ?` 回写 | `BlogServiceImpl.likeBlog`、新增定时任务 |
| **Feed 推拉结合** | 粉丝数超过阈值（如 5000）的大 V 发帖不推送，只写自己的发件箱 `outbox:{userId}`；粉丝读 Feed 时合并收件箱和所关注大 V 的发件箱 | `BlogServiceImpl.saveBlog` / `queryBlogOfFollow` |
| **限流熔断降级** | 接入 Sentinel：按接口 QPS 限流、按响应时间或异常比例熔断。降级策略：Feed 降级为热门列表；点赞失败只记 Redis；店铺详情 DB 不可用时返回缓存旧值 | 新增 `config/SentinelConfig`，在 Controller 上加注解 |
| **线程池隔离** | 秒杀和普通接口分开（Sentinel 线程数隔离，或独立的 Tomcat connector + 线程池），避免秒杀尖峰拖垮全站 | `MvcConfig` / 网关 |

### 批次 C：可靠性补齐 + 分库分表前置

| 项 | 方案 | 涉及代码 |
| --- | --- | --- |
| **全局 ID** | `RedisIdWorker` 按天 INCR 依赖单个 Redis，并且每次下单多一次 Redis 往返。改为**雪花算法**：时间戳 + 机器号 + 序列号。机器号由 Redis `INCR` 在启动时分配并续约，或用 K8s StatefulSet 的序号。需处理时钟回拨：小幅回拨等待，大幅回拨拒绝服务并告警。也可用号段模式（Leaf-segment） | `utils/RedisIdWorker` 替换实现，接口不变 |
| **Kafka 集群** | 3 个 broker（KRaft）、`replication.factor=3`、`min.insync.replicas=2`。配合已有的 `acks=all` + 幂等生产者，任意一个 broker 宕机都不丢消息 | `deploy/docker-compose.yml`、`config/KafkaConfig` |
| **Outbox 防丢单** | 现状：Lua 已扣库存 → Kafka 异步发送，这中间进程崩溃会丢单。方案：Lua 成功后把订单消息写入 Redis Stream / List 作为本地待发队列（与 Lua 同一个原子脚本，库存扣减和消息记录同时成功）。后台线程投递 Kafka，成功后删除。定时扫描超时未投递的记录重发，消费端靠主键幂等 | `seckill.lua`、`VoucherOrderServiceImpl`、新增 `mq/OutboxRelay` |
| **超时关单** | 定时扫表改为延迟队列：Redis ZSet（score = 到期时间）+ 定时拉取到期订单；或换支持延迟消息的 MQ。扫表保留为低频兜底 | `mq/OrderTimeoutScheduler` |
| **MySQL 读写分离** | 一主两从（半同步复制）。ShardingSphere-JDBC 读写分离规则。**写后立即读**（下单后查状态、发笔记后查详情）强制走主库（`HintManager`），或走已有的 Redis 状态缓存 | 新增 ShardingSphere 依赖与配置、`application.yaml` |
| **连接数核算** | 读写分离后每个实例的连接数 = 副本数 × (主库池 + 每个从库池)，要分别小于各库的 `max_connections`。主库池调小，从库池按读流量分配 | `application.yaml`、MySQL 配置 |

### 批次 D：存储层拆分（高风险，基线证明单机不够时才做）

| 项 | 方案 | 涉及代码 |
| --- | --- | --- |
| **Redis Cluster** | 3 主 3 从起步。**Lua 必须改造**：现在 `seckill.lua` 是在脚本里用 ARGV 拼出 key，Cluster 要求所有 key 通过 `KEYS` 传入，并且落在同一个 slot。key 改为 hash tag 形式：`seckill:{voucherId}:stock`、`seckill:{voucherId}:order`、`seckill:{voucherId}:time`；点赞的 `blog:liked:bit:{id}` / `blog:liked:top:{id}` 同理；`seckill_rollback.lua`、`unlock.lua` 一并检查。pipeline 和 Redisson 切到 Cluster 模式；`KEYS` / `SCAN` 类操作改为逐节点执行 | `seckill.lua`、`seckill_rollback.lua`、点赞 Lua、`RedisConstants`、`SeckillStockInitializer`、`RedissonConfig` |
| **秒杀库存分段** | 单张券库存拆成 K 段 `seckill:{voucherId:i}:stock`，请求按 userId 哈希到某一段，某段扣完后去试相邻段。一人一单的去重集合要和该用户所在的段在同一个 slot（按 userId 路由可以保证）。单券上限约提升 K 倍 | `seckill.lua`、`VoucherOrderServiceImpl`、库存预热与回补 |
| **大 key 拆分** | `seckill:order:{id}` 用户集合随抢购人数增长，按 userId 分桶；`follows:` 和 `feed:` 有截断，监控大 key 即可 | 同上 |
| **订单分库分表** | `tb_voucher_order` 按 `user_id` 取模分片，例如 2 库 × 16 表，用户查自己的订单不用跨库。订单 id 中嵌入 user_id 的基因位，按订单 id 查询也能直接定位分片。商家 / 运营的多维查询同步到 ES 或异构索引表。超时关单按分片并行扫描，或完全依赖延迟队列 | ShardingSphere 分片规则、`VoucherOrderMapper`、`OrderTimeoutScheduler` |
| **缓存一致性** | Canal 订阅 binlog → Kafka → 统一删除 Redis 缓存并广播本地缓存失效，替代散落在业务代码里的「更新 DB 后删缓存」。保留延迟双删作为兜底 | 新增 cache-sync 消费者 |
| **部署形态** | K8s：app 用 Deployment + HPA（按 CPU 和 QPS 自定义指标）；MySQL / Redis / Kafka 优先用云托管，或用 Operator。多可用区部署，nginx 前加云负载均衡；秒杀页静态化 + CDN | `deploy/k8s/` |

---

## 3-3 验证上线阶段

### 3-3.1 功能与一致性验证（每批都做）

- 单元测试 + 集成测试：用 Testcontainers 起 MySQL / Redis / Kafka，替代现在依赖本机端口的测试；
- 秒杀对账脚本：订单数 = 成功下单用户数、DB 库存 = Redis 库存（分段库存求和）、没有重复订单；
- 点赞异步回写：`tb_blog.liked` 与 Redis 计数的差值在一个回写周期后收敛到 0；
- Feed 推拉结合：大 V 与普通用户混合关注时，分页结果不重、不漏、按时间有序。

### 3-3.2 性能验收

| 场景 | 验收指标（示例，按 3-1 定的 SLO 调整） |
| --- | --- |
| 店铺详情（多级缓存命中） | P99 < 20ms；本地缓存命中率 > 80%；Redis ops/s 相比改造前下降一半以上 |
| 混合场景 | 达到目标峰值 QPS × 1.5 时 P99 < 100ms、错误率 < 0.1%，各组件资源使用率 < 70% |
| 秒杀单券 | 10 倍库存量的请求进来，售罄后的请求直接本地返回；Kafka lag 在活动结束后 1 分钟内清零；对账零差异 |
| 扩展效率 | 1 → 8 副本扩展效率 ≥ 0.8 |

压测方式：混合场景用 JMeter 分布式压测或 k6，压测机和被测服务分开部署。生产环境做全链路压测时，用影子库 / 影子表 + 压测流量标记（请求头透传到 Kafka header），保证压测数据不污染真实数据。

### 3-3.3 故障演练（上线前必须通过）

| 故障 | 预期 |
| --- | --- |
| kill Redis 主节点 | 哨兵或 Cluster 在 15s 内完成切换；对账能发现异步复制丢失的扣减并修正；应用短暂报错后自动恢复 |
| kill 1 个 Kafka broker | 生产和消费不中断，消息不丢（`min.insync.replicas=2`） |
| MySQL 主从切换 | 写入中断时间 < 30s；切换后读写分离路由正确 |
| app 实例在秒杀中崩溃 | Outbox 重发补齐订单，对账零差异 |
| 下游变慢（注入 DB 延迟） | Sentinel 熔断生效，非核心接口降级，核心接口可用 |

现有 `perf/lock-failover-demo.ps1` 可以作为演练脚本的起点；进阶可引入 ChaosBlade。

### 3-3.4 数据迁移（分库分表专用）

按「双写 → 历史数据迁移 → 校验 → 切读 → 停旧写」推进，每一步都可回退：

1. 新老库双写，以老库为准，新库写失败只记录、不影响业务；
2. 用 DataX 或自研批量任务迁移历史订单，按 id 范围分片执行；
3. 全量对账加抽样比对，差异自动修复；
4. 读流量按百分比灰度切到新库（1% → 10% → 50% → 100%）；
5. 稳定观察一个周期后停掉老库写入，老库只读保留一段时间。

### 3-3.5 灰度与回滚

- 每项改造都配功能开关（配置中心或 `application.yaml` 开关），例如 `hmdp.cache.local.enabled`、`hmdp.feed.pull.enabled`、`hmdp.seckill.stock-segments`；
- 发布方式：金丝雀发布（先 1 个副本）→ 按比例放量，同时观察大盘核心指标；
- 回滚条件：错误率、P99 或对账差异超过阈值就自动或手动回滚。**回滚步骤必须在 3-3.3 中演练过**；
- 不可逆操作（Kafka 加分区、Redis key 改名、分表）单独评审，并提前准备兼容读（新老 key 都读）。

---

## 3-4 运行治理阶段

### 3-4.1 SLO 与告警

| 对象 | SLO（示例） | 告警 |
| --- | --- | --- |
| 核心接口（店铺详情、秒杀、登录） | 可用性 99.95%，P99 < 100ms | 5 分钟错误率 > 0.5% 或 P99 超标，用燃烧率（burn rate）告警 |
| 秒杀链路 | 订单最终一致，延迟 < 1 分钟 | Kafka lag 持续增长、DLT 有消息、对账差值不为 0 |
| 存储 | 资源使用率 < 70% | Redis 内存 > 70%、MySQL 主从延迟 > 5s、Hikari `pending` > 0 持续 1 分钟、出现大 key / 热 key |

告警分级（P0 电话 / P1 IM / P2 工单），每条告警都要对应一份处理预案（runbook）。

### 3-4.2 稳定性手段

| 维度 | 做法 |
| --- | --- |
| **防过载** | 多层限流：CDN / 云负载均衡 → nginx `limit_req` → Sentinel 接口限流 → 应用内信号量；过载时优先丢弃非核心流量 |
| **防雪崩** | 所有远程调用都设超时，重试带退避和上限；熔断 + 降级；线程池隔离；缓存继续用空值、互斥锁、TTL 抖动 |
| **容量水位** | 日常资源使用率控制在 50–60%；大促 / 秒杀前按 3-1.3 模型**提前扩容**，不要只靠 HPA，因为它有分钟级滞后；活动前预热缓存和库存 |
| **数据正确** | 每天定时对账（库存、订单、点赞数），差异自动修复并告警；DLT 消息可一键重放；Outbox 积压告警 |
| **变更管控** | 灰度发布 + 功能开关 + 一键回滚；大表结构变更用 gh-ost 在线做；高峰期封网 |
| **定期演练** | 每季度一次全链路压测，刷新容量基线；每月一次故障演练（轮流覆盖 3-3.3 的场景）；核验备份能否恢复 |

### 3-4.3 持续迭代

- 每次故障或大促后复盘，把问题沉淀为新的告警、预案或改造项；
- 季度容量评审：用最新基线和业务增长预测，更新所需节点数；
- **出现新瓶颈时回到 3-1**：例如订单量再增长一个数量级就扩分片，Feed 读压力大就引入独立的 Feed 服务。

---

## 关键文件（实施时涉及）

`pom.xml`、`resources/application.yaml`、`resources/seckill.lua` / `seckill_rollback.lua` / `unlock.lua`、
`utils/{RedisIdWorker,RedisConstants,CacheClient,RefreshInterceptor}.java`、
`service/impl/{VoucherOrder,Blog,Shop,ShopType}ServiceImpl.java`、`mq/{OrderTimeoutScheduler,SeckillOrderConsumer}.java`、
`config/{KafkaConfig,RedissonConfig,SeckillStockInitializer}.java`、`deploy/docker-compose.yml`、新增 `deploy/k8s/`、`perf/`。

## 风险与取舍

| 风险 | 应对 |
| --- | --- |
| 引入组件过多，运维复杂度陡增 | 严格按 3-1 的数据决定是否做；能用云托管就不自建 |
| 本地缓存导致短暂读到旧数据 | 只缓存能容忍秒级不一致的数据；库存、订单状态不进本地缓存 |
| Redis Cluster 主从异步复制，切换时可能丢掉扣减 | 对账修正；对超卖零容忍的场景以 DB 扣减为最终依据 |
| 分库分表后跨分片查询和分页变复杂 | 订单查询只按 user_id；其他维度走 ES |
| 雪花算法依赖时钟 | 时钟回拨检测 + NTP 监控 |
