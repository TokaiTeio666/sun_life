# 容量基线报告

> 状态：**本机受控基线**（2026-11-24）。压测客户端与整套服务跑在同一台开发机上，
> 所有绝对数字只代表这台机器的**下限**，不能当作云端或线上容量承诺。
> 扩展效率（第 3 步）在同机上没有意义，未测；分机基线有条件时按 [bench-deploy.md](bench-deploy.md) 补。
> 对应 [phase3-architecture-plan.md](../plan/phase3-architecture-plan.md) 3-1.4；测法与工具见 [../perf/baseline/README.md](../perf/baseline/README.md)。
>
> **注意**：本报告全部数据在 **MySQL 8** 上测得，之后数据库已迁移到 PostgreSQL 16（见 [behavior-changes.md](behavior-changes.md)）。
> 数据库相关的数字（组件单测、热点行扣库存、MySQL CPU / 行锁）需要在 PG 上重新测量；文中的 `migration_v2.sql`、`mysql:3306`、sysbench 等均为当时的工具与环境。

## 1. 环境与口径

| 项 | 值 |
| --- | --- |
| 日期 / 版本 | 2026-11-24；master `89f3ab2` + 本机基线相关改动（见第 8 节） |
| 被测机 | Intel i7-14700HX（20 核 / 28 线程），31.7 GB，YMTC 1 TB NVMe SSD，Windows 11 Pro |
| 压测机 | **与被测机分开：否**。服务跑在 WSL2（16 vCPU / 20 GB），JMeter 5.6.3 跑在 Windows 宿主 |
| 部署 | app 1 副本；`-Xms2g -Xmx4g`；`DB_POOL_MAX=30`；MySQL `max_connections=300`、buffer pool 4G、redo 1G |
| 数据量 | 10.0 万用户 / 1.0 万店铺 / 100.0 万订单 / 5.0 万笔记 / 30.0 万关注；库表 149 MB |
| SLO | 核心接口 P99 < 100 ms，错误率 < 0.1% |

口径限制（读数字前必看）：

- JMeter 和服务共享**同一颗物理 CPU**，只是把 WSL 限到 16 个 vCPU；并发越高，压测端和服务端互相抢得越厉害；
- 组件单测的压测客户端也以容器形式跑在同一台机器上（`component-bench.sh` 默认网络），组件数字同样是下限；
- Docker Desktop 的虚拟磁盘 fsync 明显慢于云 SSD，MySQL 热点行 UPDATE 受影响最大；
- 因此本报告只用于**同一环境下的横向对比、瓶颈排序和改造前后回归**，不用于容量规划。

## 2. 组件单测上限（第 1 步）

结果目录：

- Redis / Kafka：`perf/baseline/results/20261124-020058-component/component.md`
- MySQL：`perf/baseline/results/20261124-020454-component/component.md`（补索引后重跑，见第 8 节）

### Redis（`redis:6379`，n=200000，c=50，客户端同机）

| 命令 | ops/s | 平均 ms | P50 | P95 | P99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| GET | 136,986 | 0.201 | 0.183 | 0.279 | 0.503 |
| HGETALL（登录态校验） | 135,410 | 0.199 | 0.191 | 0.279 | 0.447 |
| SISMEMBER（是否关注） | 139,082 | 0.198 | 0.183 | 0.327 | 0.543 |
| EVALSHA `seckill.lua`（单券） | 126,904 | 0.341 | 0.335 | 0.503 | 0.647 |
| EVALSHA 点赞 Lua | 127,959 | 0.339 | 0.327 | 0.527 | 0.711 |

### MySQL（`mysql:3306`，sysbench 执行真实业务 SQL，50 线程，每条 30s）

| SQL | 每秒执行 | 平均 ms | P95 ms | 说明 |
| --- | ---: | ---: | ---: | --- |
| 主键点查（店铺详情回源） | 202,186 | 0.25 | 0.64 | 数据 149 MB，全在 4G buffer pool |
| 热门笔记（`ORDER BY liked`） | 148,932 | 0.34 | 0.68 | 补 `idx_liked` 前只有 **799 /s**，全表 filesort |
| 超时关单扫描（`idx_status_ctime`） | 157,867 | 0.32 | 0.77 | 造数没有「未支付」订单，匹配 0 行，**不代表真实积压下**的成本 |
| 热点行扣库存 `stock-1` | 83 | 590.16 | 669.89 | 50 线程同一行，`Innodb_row_lock_current_waits=49`，平均等锁 658 ms |
| 热点行扣库存 `stock-24`（批量聚合） | 85 | 582.57 | 657.93 | 同样约 85 次 UPDATE/s，每次 24 单 ⇒ **约 2,040 单/s** |
| 多值 INSERT 24 行 | 1,436 | 34.79 | 61.08 | 约 34,500 行/s |

> 单条扣减的硬上限就是 83 单/s；批量聚合把同样次数 UPDATE 的落库能力放大到约 2,040 单/s。
> 这个绝对值受 Docker Desktop fsync 拖累，云端 SSD 会更高，但「单券 = 一行」的锁串行结论不变。

### Kafka（`kafka:9092`，KRaft 单节点，12 分区，200 B）

```
producer: 1,000,000 records sent, 552,486 records/s (105.4 MB/s), acks=all + 幂等, avg 205 ms, 99th 317 ms
consumer: 1,000,000 records in 0.72 s, 1,383,126 records/s
```

### 组件结论

- 读路径（Redis GET / MySQL 主键 / 热门笔记）都在十万级每秒，**远高于本次应用拐点**；
- 秒杀落库的瓶颈在**单券热点行的写**，不在 Kafka，也不在 Redis 的单 key Lua（12.7 万/s）；
- 索引是数量级因素：热门笔记缺 `idx_liked` 时只有 799 /s，补上后 148,932 /s（约 186 倍）。

## 3. 单实例拐点（第 2 步）

结果目录：

- 店铺详情：`perf/baseline/results/20261124-021518-knee-shop-detail-bench/summary.md`
- 混合场景：`perf/baseline/results/20261124-021923-knee-mixed-bench/summary.md`
- 秒杀：`perf/baseline/results/20261124-022236-knee-seckill-bench/summary.md`

### 3.1 店铺详情（`/shop/{id}`，1 万店铺，L1 缓存开启）

| 并发 | QPS | 平均 RT ms | P95 ms | P99 ms | 错误率 | QPS 增幅 | 判定 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 50 | 19,890 | 2.4 | 4 | 5 | 0.00% | — | ok |
| 100 | 23,642 | 4.1 | 6 | 8 | 0.00% | +19% | ok |
| 200 | 25,438 | 7.7 | 12 | 15 | 0.00% | +8% | ok |
| 400 | 25,217 | 15.6 | 33 | 172 | 0.00% | −1% | P99 > SLO |

**客户端观测拐点**：200 并发，25,438 QPS（P99 15 ms）；安全容量 15.3k–17.8k QPS。
**但 400 并发时服务端 P99 只有约 14 ms**（Prometheus 直方图），客户端 P99 172 ms —— 吞吐不再增长、服务端延迟没变，
说明这一级是**压测端先到顶**（同机 400 个 JMeter 线程 + Windows 调度），不是应用容量到顶。

### 3.2 混合场景（店铺 70% / 热门笔记 15% / Feed 10% / 秒杀 5%）

| 并发 | QPS | 平均 RT ms | P95 ms | P99 ms | 错误率 | QPS 增幅 | 判定 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 50 | 13,593 | 3.3 | 7 | 10 | 0.00% | — | ok |
| 100 | 14,977 | 6.1 | 16 | 23 | 0.00% | +10% | ok |
| 200 | 14,039 | 13.1 | 35 | 188 | 0.00% | −6% | P99 > SLO |

**拐点**：100 并发，14,977 QPS（P99 23 ms）；安全容量 9.0k–10.5k QPS。

200 并发时各接口：

| 接口 | QPS | P99 ms | 错误率 |
| --- | ---: | ---: | ---: |
| `GET /shop/{id}` | 9,829 | 21 | 0.00% |
| `GET /blog/hot` | 2,109 | **251** | 0.00% |
| `GET /blog/of/follow` | 1,406 | **335** | 0.00% |
| `POST /voucher-order/seckill/{id}` | 703 | 55 | 0.00% |

拖尾集中在热门笔记和关注 Feed，店铺详情与秒杀仍然很低；服务端 P99 约 116 ms，客户端 188 ms，**是应用侧的真实拖尾**，不是压测端。

### 3.3 秒杀（`/voucher-order/seckill/21`，2000 个模拟登录用户）

| 并发 | QPS | 平均 RT ms | P95 ms | P99 ms | 错误率 | QPS 增幅 | 判定 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 100 | 17,361 | 5.6 | 9 | 12 | 0.00% | — | ok |
| 200 | 17,948 | 10.9 | 16 | 22 | 0.00% | +3% | 饱和（QPS 不再增长） |
| 400 | 18,806 | 21.0 | 72 | 259 | 0.00% | +5% | P99 > SLO |

**客户端观测拐点**：100 并发，17,361 QPS（P99 12 ms）；安全容量 10.4k–12.2k QPS。
400 并发时服务端 P99 约 33 ms、应用约 5.8 核、Redis 约 0.2 核、MySQL 0.03 核、Kafka lag 0，同样是压测端先到顶。

口径说明：token 文件只有 2000 个用户，每轮重置后最多 2000 单成功，之后全是「重复下单」快速路径。
所以这组数字测的是**请求侧（Lua + 售罄标记）**，不是在测消费端落库上限。

### 3.4 拐点时的资源

| 场景 | 并发 | QPS | 客户端 P99 | 服务端 P99 | app CPU（核） | Redis CPU | MySQL CPU | Kafka CPU | Hikari pending | Kafka lag | 判断 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 店铺详情 | 400 | 25,217 | 172 ms | ~14 ms | ~3.7 | 0.06 | 0.01 | 0.20 | 0 | — | 压测端先到顶；GC 开销 0.2%、最大暂停 11 ms、堆 170 MB/4G |
| 混合 | 200 | 14,039 | 188 ms | ~116 ms | ~6.6 | 0.52 | 0.59 | 0.26 | 0 | 0 | 应用侧拖尾（blog/feed），无共享组件饱和 |
| 秒杀 | 400 | 18,806 | 259 ms | ~33 ms | ~5.8 | ~0.2 | 0.03 | 0.21 | 0 | 0 | 压测端先到顶 |

### 3.5 秒杀端到端对账（最后一轮）

| 项 | 结果 |
| --- | --- |
| DB 订单 / 去重用户 | 2,000 / 2,000 |
| DB 库存 / Redis 库存 | 98,000 / 98,000 |
| Kafka 消费组 lag | 12 个分区全部 0 |
| `hmdp_seckill_orders_persisted_total`（本次所有轮次累计） | 6,985 |

一人一单、库存一致、异步消费不积压。

## 4. 扩展效率（第 3 步）

**未测**。同机上 `--scale app=N` 只是把同一组 16 个 vCPU 分给更多副本，测出来的「效率」反映的是 CPU 配额，不是架构扩展性。
分机基线按 [bench-deploy.md](bench-deploy.md) 部署后，用：

```bash
python perf/baseline/baseline.py scale --jmx perf/mixed-bench.jmx --token-file token.txt \
    --host <网关IP> --threads 1600 --replicas 1,2,4,8   # 压测机无法操作被测机 docker 时加 --manual-scale
```

这是进入 3-2 前**唯一还缺**的实测项。

## 5. 每请求的组件访问次数（容量模型输入）

从 `perf/baseline/results/local-metrics.csv`（[sample_metrics.py](../perf/baseline/sample_metrics.py) 每 5s 采样）按稳定级别折算：

| 场景 | Redis 命令 / 请求 | 说明 |
| --- | ---: | --- |
| 店铺详情（200 并发） | ~0.08 | L1 Caffeine 命中为主；1 万店铺 × 10s TTL，回源很少 |
| 混合（100 并发） | ~2.5 | 含秒杀 Lua 内部调用与登录 HGETALL 摊销 |
| 秒杀（200 并发） | ~4.1 | 1 次 `EVALSHA` + Lua 内 2–4 次 `redis.call`；订单号本地生成，0 次 `INCR` |

- `redis_commands_processed_total` 把 Lua 内部调用也算作命令，**不等于客户端往返次数**；
- 秒杀成功路径的客户端 Redis 往返是 **1 次**（登录本地缓存未命中时 +1 次 `HGETALL`），订单号走本地雪花；
- MySQL：店铺详情与秒杀请求路径基本 0 次（缓存命中 / 落库在消费端异步）；混合压测时 Hikari active 5–11、pending 0；
- Kafka：只有成功秒杀产生消息，本次每轮 ≤2000 条，远低于组件上限。

### 已记录的改造项对访问次数的影响（不依赖压测环境）

| 改造 | 接口 | 变化 |
| --- | --- | --- |
| B1 布隆过滤器 `BITFIELD` | 店铺详情 | Redis 8 → 2 |
| E4 登录态本地短缓存 | 全部带 token 请求 | Redis −1（`HGETALL`），每个 token 每副本每 15s 最多 1 次 |
| E5 多级缓存 | 店铺详情、商铺类型、热门笔记 | 本地命中时回源 0；每个 key 每副本每 10s 最多回 Redis 1 次 |
| E6 秒杀售罄本地标记 | 售罄后的秒杀 | Redis 2 → 0；每个券每副本每 5s 确认 1 次 |
| E7 订单号本地生成 | 秒杀 | 每单少 1 次 `INCR`；副本每 20s 续租 |
| E8 点赞数异步回写 | 点赞 | 请求内不再 UPDATE `tb_blog`，定时批量回写 |

## 6. 容量模型结论

**未计算**：缺少业务目标（DAU、人均请求、峰值系数、单券峰值）。容量模型工具已就绪：

```bash
cp perf/baseline/capacity-model.example.json perf/baseline/capacity-model.json
# 用本报告第 2、3 节填 capacity.*，用第 5 节填 interfaces.*
python perf/baseline/capacity_model.py perf/baseline/capacity-model.json --md
```

本机绝对 QPS 不能直接当线上容量；模型里的 `capacity.app_safe_qps` 应以**分机基线**为准。

## 7. 改造优先级（3-1.4 决策）

| 瓶颈（实测依据） | 对应改造项 | 批次 | 成本 | 风险 | 优先级 |
| --- | --- | --- | --- | --- | --- |
| 单券热点行 UPDATE 83 次/s；批量聚合后约 2,040 单/s。目标单券峰值超过 2k/s 时才会撞上 | 库存分段 | D | 高 | 高 | **条件触发**，不是现在 |
| 单券 Lua 12.7 万/s；应用拐点时 Redis ≤0.52 核 | Redis Cluster | D | 高 | 高 | 暂缓 |
| 1 副本 25.4k（店铺）/ 15.0k（混合）/ 17.9k（秒杀）；拐点时无共享组件饱和 | 多副本 + 分机复测扩展效率 | 验证 | 低 | 低 | **先做** |
| 混合 200 并发：`/blog/hot` P99 251 ms、Feed 335 ms，服务端 P99 116 ms | 热门笔记/Feed 应用侧优化（先看 trace） | B | 低 | 低 | **先做** |
| MySQL / Redis / Kafka 各 1 节点；消息发送失败仍可能丢推送 | Outbox、3 broker、主从 | C | 中 | 中 | 与基线并行 |
| 旧数据卷缺索引；nginx/Tomcat keep-alive 默认值 | 已修复（第 8 节） | — | 低 | 低 | 已完成 |

**结论**：在本机测试区间内，第一个饱和的不是 Redis / MySQL / Kafka，而是**应用侧的处理与排队**；
秒杀真正的硬上限是单券热点行写（约 2k 单/s 批量聚合后），Redis 不是首要矛盾。
3-2 的默认顺序 B → C → D 与数据不冲突：先用低风险的 B 项削 blog/feed 拖尾，同时按 C 补可靠性；
D 的库存分段在目标单券峰值明确超过 2k/s 后再做。

**进入 3-2 的门槛**：已有一套可复现的本机基线；**分机扩展效率仍缺**。建议至少用按量付费几小时补一次 scale 测试，
再决定多副本的线性度。

## 8. 结果文件与本次修掉的问题

| 内容 | 路径 |
| --- | --- |
| 组件单测（Redis / Kafka） | `perf/baseline/results/20261124-020058-component/component.md` |
| 组件单测（MySQL，补索引后） | `perf/baseline/results/20261124-020454-component/component.md` |
| 店铺详情拐点 | `perf/baseline/results/20261124-021518-knee-shop-detail-bench/summary.md` |
| 混合场景拐点 | `perf/baseline/results/20261124-021923-knee-mixed-bench/summary.md` |
| 秒杀拐点 | `perf/baseline/results/20261124-022236-knee-seckill-bench/summary.md` |
| 指标采样（CSV + PromQL） | `perf/baseline/results/local-metrics.csv`、`local-metrics.promql` |
| 本机部署参数 | `deploy/bench.local.env.example`、[bench-deploy.md](bench-deploy.md)「本机基线」 |

本次暴露并修掉的两个非基线问题：

1. **旧数据卷没有执行 `migration_v2.sql`**：`tb_blog` / `tb_follow` / `tb_voucher_order` 只有主键，
   热门笔记退化成全表 filesort（799 /s），关注表还因为缺 `uk_user_follow` 插入了 830 条重复行。
   已补索引并清理重复行；`seed_data.py` 现在会在造数前检查所需索引，缺失时直接报错并给出迁移命令。
2. **nginx 1.18 的 `keepalive_requests` 默认是 100**（1.19 才改成 1000），Tomcat 默认也是 100：
   20k QPS 下大约每 5 ms 就重建一条连接，30 秒堆出约 6k 条连接 / 8874 个 TIME_WAIT，
   200 并发出现 1.33% 的 `BindException`。把两者都调到 100,000 后重跑，才得到第 3 节能用的拐点。
