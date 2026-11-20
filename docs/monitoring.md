# 监控体系（阶段 3-1）

> 对应 [../plan/phase3-architecture-plan.md](../plan/phase3-architecture-plan.md) 的 3-1.1。
> 目的：给阶段 3 的每一项改造提供依据——**先看到瓶颈，再动架构**。

## 1. 启动

```bash
# 业务 + 监控（Prometheus / Grafana / redis_exporter / mysqld_exporter / kafka-exporter）
docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.monitoring.yml up -d --build

# 再加容器级 CPU / 内存（cAdvisor，需要 Linux 或 Docker Desktop 的 WSL2 后端）
docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.monitoring.yml --profile resources up -d

# 排查慢接口时再叠加链路追踪（SkyWalking；做拐点压测时不要开，agent 有开销）
docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.monitoring.yml \
               -f deploy/docker-compose.tracing.yml up -d
```

| 入口 | 地址 | 说明 |
| --- | --- | --- |
| Grafana | http://localhost:3000 | admin / `GRAFANA_ADMIN_PASSWORD`（默认 admin），首页即「sun_life 总览（容量基线）」 |
| Prometheus | http://localhost:9090 | Status → Targets 看抓取状态；Alerts 看告警 |
| SkyWalking UI | http://localhost:8088 | 仅叠加 tracing 编排时存在 |
| 应用指标 | `app:9081/actuator/prometheus` | **只在容器网络内可达** |

端口都可在 `deploy/.env` 中用 `GRAFANA_PORT`、`PROMETHEUS_PORT`、`SKYWALKING_UI_PORT` 覆盖。

### 安全：/actuator 不对外

- 应用的管理端口与业务端口分离（`management.server.port=9081`），网关 nginx 只代理 8081；
- nginx 额外对 `/actuator`、`/api/actuator` 直接返回 404，防止有人把管理端口改回 8081 后意外暴露；
- 扩缩容（`--scale app=N`）后 Prometheus 通过 Docker DNS 在 15s 内自动发现新副本，无需改配置。

## 2. 指标来源

| 层 | 来源 | 关键指标 |
| --- | --- | --- |
| 应用 | Actuator + Micrometer | `http_server_requests_seconds_*`（按接口的 QPS / 错误 / 直方图分位数）、`jvm_*`、`tomcat_threads_busy_threads`、`hikaricp_connections_{active,pending}`、`lettuce_command_completion_seconds_*`、Kafka 客户端指标 |
| Redis | redis_exporter | ops/s、主线程 CPU、内存、命中率、慢查询、连接数、`seckill:order:*` 集合大小 |
| MySQL | mysqld_exporter + 慢查询日志 | QPS / TPS、行锁等待、连接数、慢查询（`long_query_time=200ms`）、perf_schema Top SQL |
| Kafka | kafka-exporter | 每个分区的 offset（流入速率）、消费组 lag、DLT 消息数 |
| 链路 | SkyWalking agent | 单个请求在 Redis / MySQL / Kafka 各段的耗时 |
| 业务 | 自定义 Micrometer（`com.hmdp.metrics`） | 见下表 |

> **虚拟线程下的线程指标**：应用默认用 JDK 21 虚拟线程处理请求（`VIRTUAL_THREADS=false` 可关闭），这时 Tomcat 没有线程池，
> `tomcat_threads_*` 恒为 -1，Grafana 的「Tomcat 繁忙线程」面板没有意义，`TomcatThreadsSaturated` 告警也不会触发。
> 并发上限变成下游连接池，应用是否饱和看 `hikaricp_connections_pending`（有请求在等 DB 连接）和接口 P99。

**HTTP 分位数为什么用直方图**：开启了 `percentiles-histogram`，P95 / P99 在 Prometheus 端用 `histogram_quantile` 跨实例聚合；
客户端预计算的分位数无法跨副本合并。

### 业务指标

| 指标 | 类型 | 含义 |
| --- | --- | --- |
| `hmdp_seckill_requests_total{result}` | counter | 秒杀请求按 Lua 结果计数：success / sold_out / duplicate / not_started / ended |
| `hmdp_seckill_send_failures_total{rollback}` | counter | 订单消息投递 Kafka 失败；`rollback=failed` 表示 Redis 库存回滚也失败，必须人工对账 |
| `hmdp_seckill_orders_persisted_total{path}` | counter | 消费端实际落库订单数；`batch` 批量快速路径、`single` 逐条降级路径 |
| `hmdp_seckill_stock_update_orders` | summary | 每条库存 UPDATE 合并的订单数，`_sum / _count` = **批量聚合度**，`_count` = 热点行 UPDATE 次数 |
| `hmdp_seckill_consume_batch_size` | summary | 每次 poll 的消息数 |
| `hmdp_seckill_consume_duplicates_total` | counter | 重复投递被幂等拦截的消息数 |
| `hmdp_seckill_consume_batch_fallback_total` | counter | 批量落库失败降级为逐条的批次数 |
| `hmdp_seckill_db_stock_short_total` | counter | 消费时 DB 库存不足（Redis 与 DB 不一致） |
| `hmdp_seckill_dead_letters_total` | counter | 进入死信的消息数（应用侧计数；kafka-exporter 的 DLT offset 是独立来源） |
| `hmdp_order_timeout_closed_total` | counter | 超时关单数 |
| `hmdp_seckill_stock{voucher,source}` | gauge | 每 15s 采样的 Redis / DB 库存 |
| `hmdp_seckill_stock_gap{voucher}` | gauge | **DB 库存 − Redis 库存**。Redis 先扣、DB 后扣，正常 ≥ 0 且等于在途订单数，lag 清零后归零 |
| `hmdp_overload_rejected_total{reason}` | counter | 过载保护返回 503 的请求数：`db_gate` 并发闸拒绝、`db_conn_timeout` 等 DB 连接超时 |
| `hmdp_feed_fanout_total{mode}` | counter | 笔记 Feed 分发次数：`push` 推送到粉丝收件箱、`pull` 大 V 不推送 |
| `hmdp_feed_pushed_entries_total` | counter | 推送写入收件箱的条数（被推送的粉丝数之和） |
| `hmdp_feed_publish_failures_total` / `hmdp_feed_fanout_failures_total` | counter | 发布消息投递 Kafka 失败 / 分发重试耗尽被跳过；非 0 表示有粉丝少收到笔记 |
| `cache_gets_total{cache,result}` / `cache_size{cache}` | counter / gauge | 本地缓存（Caffeine）命中 / 未命中次数与条数；`cache="loginUser"` 是登录态短缓存，未命中数 ≈ `HGETALL` 次数；`cache="l1"` 是多级缓存的本地一级（店铺详情 / 商铺类型 / 热门笔记） |
| `hmdp_db_gate_active` / `hmdp_db_gate_permits` | gauge | 并发闸内正在回源 DB 的请求数 / 许可数（0 = 闸已关闭） |

> 命名约定：不要以 `created` 结尾（OpenMetrics 保留后缀，会被丢弃）；DistributionSummary 不设 `baseUnit`（会被拼进指标名）。
> 标签只用有限枚举值，不放 userId / orderId。

## 3. 看板

`deploy/monitoring/grafana/dashboards/hmdp-overview.json`，由 Grafana 启动时自动加载，分 8 组：

1. **概览**：总 QPS、5xx 错误率、全局 P99、在线副本数、秒杀 lag、库存差值；
2. **应用 RED**：按接口的 QPS、4xx/5xx、P95、P99；
3. **容量**：各副本 QPS、扩展效率（顶部变量 `single_qps` 填单副本拐点 QPS）、在途请求 vs Tomcat 线程（Little 定律）；
4. **JVM 与连接池**：Tomcat 繁忙线程、Hikari active / pending / 获取耗时、堆、GC、CPU、Lettuce 命令耗时、**每请求 Redis 命令数**；
5. **Redis**、6. **MySQL**、7. **Kafka**、8. **秒杀业务**。

「每请求 Redis 命令数」和「MySQL QPS ÷ 接口 QPS」就是容量模型（`perf/baseline/capacity_model.py`）的输入。

在 UI 里改过看板后，用 Dashboard → Share → Export 导出 JSON 覆盖回仓库文件。

## 4. 告警与处理

规则在 `deploy/monitoring/prometheus/rules/hmdp-alerts.yml`，修改后热加载：`curl -X POST http://localhost:9090/-/reload`。
本地未部署 Alertmanager，告警只在 Prometheus Alerts 页可见；上线时接 Alertmanager，按 `severity` 路由：**P0 电话 / P1 IM / P2 工单**。

阈值是第一版经验值，跑完容量基线后按实测拐点和 SLO 调整。

| 告警 | 级别 | 先看什么 | 处理 |
| --- | --- | --- | --- |
| AppInstanceDown | P0 | `docker compose ps`、该实例日志 | nginx 已自动摘除；确认剩余副本 QPS 未超安全容量，不够就先扩副本 |
| HighErrorRate | P1 | RED 面板定位接口 → 日志 / SkyWalking | 下游故障按对应存储告警处理；发布引起的直接回滚 |
| HighP99Latency | P1 | 同时段 Hikari pending、Redis CPU、MySQL 行锁 | 找到先饱和的组件；临时手段：扩副本、网关限流 |
| HikariPoolPending | P1 | MySQL 慢查询 / 行锁、Hikari 获取耗时 | DB 慢就先查 SQL；DB 不慢而池小，按 `副本数 × 池 < max_connections` 调 `DB_POOL_MAX` |
| OverloadShedding | P1 | `reason` 标签、MySQL 慢查询、缓存命中率 | `db_gate`：回源并发超过许可数，先查 DB 为什么慢 / 缓存为什么没命中；`db_conn_timeout`：没走并发闸的接口在等连接。确认是容量不足再扩副本或调 `DB_GATE_PERMITS` |
| TomcatThreadsSaturated | P2 | 在途请求 vs 线程面板 | 线程都在等下游 → 修下游；确实是 CPU 不够 → 扩副本 |
| JvmHeapHigh | P2 | GC 面板、堆走势 | 持续上涨疑似泄漏，dump 分析；平稳偏高则调 `-Xmx` |
| **SeckillOversellRisk** | P0 | 库存面板、`tb_voucher_order` 计数 | DB 比 Redis 扣得多：暂停该券（改 `end_time`），对账后修正 Redis 库存 |
| SeckillStockGapNotConverging | P1 | 发送失败计数、应用日志「回滚」 | lag 已清零但 DB 仍多：有订单在 Lua 扣减后丢失，按 `seckill:order:{id}` 与订单表比对补单或回补库存 |
| SeckillConsumerLagGrowing | P1 | 批量聚合度、MySQL 行锁等待 | 消费写库跟不上：确认聚合度正常；`实例数 × concurrency ≤ 分区数` 内加消费者 |
| SeckillDeadLetter | P1 | 应用日志「秒杀订单进入死信」 | 看失败原因，修复后从 DLT 重放 |
| SeckillSendRollbackFailed | P0 | 应用日志「库存回滚失败」 | 按日志里的 voucherId / userId 人工对账 |
| SeckillDbStockShort | P1 | 库存面板 | Redis 库存比 DB 多（如手工改过 DB），以 DB 为准重设 Redis |
| RedisDown / MySQLDown | P0 | 容器状态、宿主机资源 | 恢复服务；阶段 3-2 批次 C/D 之前它们都是单点 |
| RedisCpuHigh | P1 | 命令耗时 Top、慢查询 | 单线程上限：先减命令数（登录态本地缓存、售罄标记），再考虑 Cluster |
| RedisMemoryHigh / RedisBigKey | P2 | `redis_key_size` | 清理过期活动的 `seckill:order:*`；大 key 分桶 |
| MySQLConnectionsHigh | P1 | 副本数 × `DB_POOL_MAX` | 调小池或调大 `max_connections`，再扩副本 |
| MySQLSlowQueries / MySQLRowLockWaits | P2 | Top SQL 面板、`slow.log` | `EXPLAIN` 补索引；热点行看批量聚合度 |
| KafkaExporterDown | P2 | exporter 日志 | lag / DLT 告警此时失效，尽快恢复 |

慢查询日志在 MySQL 容器内：`docker exec hmdp-mysql tail -f /var/lib/mysql/slow.log`。
