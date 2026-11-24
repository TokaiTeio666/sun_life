# 稳定性与故障演练（2026-11-24）

> 本机受控环境（WSL2 16 vCPU / 20 GB，Docker Desktop），压测端与服务同机；
> 目的是验证**降级与恢复行为**，不是容量结论。容量数字见
> [capacity-baseline-report.md](capacity-baseline-report.md)。
>
> 被测版本：master + 本机基线改动（含 Redis 连接池修复，见容量基线报告第 9 节）。

## 环境与工具

| 项 | 值 |
| --- | --- |
| 负载 | `perf/mixed-bench.jmx`（店铺 70 / 笔记 15 / Feed 10 / 秒杀 5）、`perf/seckill-bench.jmx`、`perf/shop-detail-bench.jmx` |
| 副本 | 长稳 + 杀副本用 2 个 app 副本；Redis / Kafka / 过载演练用 1 个 |
| 监控 | Prometheus + Grafana + cAdvisor；采样脚本 `perf/baseline/sample_metrics.py` |
| 原始数据 | `perf/baseline/results/drill-*.jtl` 与 `drill-*-report/`（不入库）、`drill-metrics.csv`（入库） |

## 1. 5 分钟长稳 + 杀副本

- **负载**：2 副本，`mixed-bench` 60 线程，300 秒；
- **注入**：运行开始时杀掉 `sun_life-app-2`（约 60 秒后拉起）；
- **结果**：

| 指标 | 值 |
| --- | --- |
| 请求数 / 平均 QPS | 4,157,745 / 13,870 |
| 错误 | **0** |
| P99 | 10–15 ms；杀副本窗口内单个 10s 桶升到 25 ms，随后恢复 |
| 吞吐 | 副本缺失期间约 9–12k QPS，恢复后稳定在约 14.2k QPS |

nginx 的 `max_fails=3 fail_timeout=10s` 把流量切到存活副本；token 在 Redis，会话不受影响。
**结论**：2 副本下丢失一个副本不会产生失败请求，只有短暂的 P99 抬升。

## 2. Redis 重启（负载中）

- **负载**：1 副本，`mixed-bench` 40 线程，120 秒；
- **注入**：第 ~35 秒 `docker restart hmdp-redis`；
- **结果**：

| 指标 | 值 |
| --- | --- |
| 请求数 / QPS | 1,554,220 / 12,972 |
| 错误 | **0** |
| P99 / 最大 RT | 8 ms / 1,408 ms（仅一条请求横跨重启） |

- Redis 约 1 秒恢复；Lettuce 自动重连，本地一级缓存继续挡住店铺详情；
- **数据**：`save 3600 1 300 100 60 10000`、AOF 关闭；`docker restart` 走优雅关闭，触发一次 RDB 保存。
  重启后 `seckill:stock:21` 仍在，`DBSIZE` 约 9.97 万；
- 对账：351 单 / 351 用户，DB 与 Redis 库存都是 **99,649**，Kafka lag 0。

> 风险：AOF 关闭时，**进程崩溃 / 断电**而不是优雅重启会丢最后一次 RDB 之后的数据；
> 秒杀库存是 Redis 里的关键状态，生产应开启 AOF（`appendonly yes`）或接受重启后的库存对账。

## 3. Kafka 停机 20 秒（秒杀负载）

- **负载**：1 副本，`seckill-bench` 80 线程，120 秒；
- **注入**：第 ~33 秒 `docker stop hmdp-kafka`，23 秒后 `docker start`；
- **结果**：

| 指标 | 值 |
| --- | --- |
| 请求数 / QPS | 2,046,608 / 17,091 |
| 错误 | **0** |
| P99 / 最大 RT | 10 ms / 62 ms |
| `hmdp_seckill_send_failures_total` | **0** |
| 消费 lag（恢复后） | 0 |

- 请求线程只做 Lua + 异步发送，不等待 Kafka；Broker 不可用时消息进入生产者缓冲区，恢复后补发；
- 应用日志只有 producer / consumer 的 `Node 1 disconnected` / `could not be established` 警告，无 ERROR；
- 对账：2,000 单 / 2,000 用户，DB 与 Redis 库存都是 **98,000**，lag 0。

> 边界：停机时间小于 `delivery.timeout.ms`（默认 120 秒），所以没有触发发送失败与库存回滚。
> 超过该时间的失败由 `sendSeckillOrder` 的回调回滚 Redis 库存；消息发送成功但进程在落库前崩溃仍会丢单，
> 这是已知的 Outbox 缺口（批次 C）。

## 4. 过载保护（关缓存 + 小连接池，600 并发）

- **配置**：1 副本，`HMDP_CACHE_ENABLED=false`、`DB_POOL_MAX=10`，店铺详情全部直查 MySQL；
- **负载**：`shop-detail-bench` 600 线程，30 秒；
- **结果**：

| 指标 | 值 |
| --- | --- |
| 请求数 / QPS | 416,980 / 13,948 |
| 503 数 / 错误率 | 329,922 / **79.1%** |
| 平均 RT / P99 | 42.4 ms / 411 ms（最大 1,072 ms） |
| `hmdp_overload_rejected_total` | 70,723 |
| Hikari active / pending | 10 / 10（池上限 10） |
| 应用日志 ERROR / Exception | **0** |

- 回源并发闸在连接池耗尽时直接拒绝，返回 `503`，**不等 MySQL 连接超时（3 秒）**；
- 应用不打印堆栈，只计指标，符合“过载时快速失败、保留可观测性”的设计；
- 另有 16 条客户端 `HttpHostConnectException`（压测端瞬时连接问题），可忽略。

## 5. 演练结论

| 场景 | 结果 | 需要跟进 |
| --- | --- | --- |
| 杀 1 个副本（2 副本） | 0 错误，P99 短暂 13→25 ms | 真正跨机故障需要多节点验证（当前所有组件单机） |
| Redis 优雅重启 | 0 错误，库存/登录态保留 | 生产开启 AOF；崩溃场景会丢最后一次快照后的数据 |
| Kafka 停机 23 秒 | 0 错误，0 发送失败，lag 归零 | 超过 120 秒或进程崩溃需要 Outbox；补一次长停机验证回滚与 DLT |
| 过载（600 并发，小池） | 79% 快速 503，P99 411 ms，无堆栈 | 网关层限流/熔断（Sentinel）仍可补；当前依赖应用并发闸 |

> 本机演练覆盖的是**进程/容器级故障恢复**。MySQL、Redis、Kafka 都是单节点，没有验证主从切换、
> 分区不可用等场景；这些需要多机环境，属于批次 C/D 的验证范围。
