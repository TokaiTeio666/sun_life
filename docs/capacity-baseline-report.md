# 容量基线报告

> 状态：**模板，待在独立压测环境实测后填写**。
> 对应 [../plan/phase3-architecture-plan.md](../plan/phase3-architecture-plan.md) 3-1.4 的产出；测法见 [../perf/baseline/README.md](../perf/baseline/README.md)。
> 每张表下面注明结果目录（`perf/baseline/results/...`）和 Grafana 截图，保证数字可追溯。

## 1. 环境与口径

| 项 | 值 |
| --- | --- |
| 日期 / 版本（git commit） | |
| 被测机 | CPU / 内存 / 磁盘 / OS |
| 压测机 | 与被测机分开：是 / 否 |
| 部署 | app 副本数、JVM 参数（`JAVA_OPTS`）、`DB_POOL_MAX`、MySQL `max_connections` |
| 数据量 | 用户数、店铺数、订单数 |
| SLO | 核心接口 P99 < ___ ms，错误率 < 0.1% |

## 2. 组件单测上限（第 1 步）

结果目录：

| 组件 | 操作 | 上限 /s | P99 ms | 备注 |
| --- | --- | ---: | ---: | --- |
| Redis | GET | | | |
| Redis | HGETALL（登录态） | | | 每个请求固定 1 次 |
| Redis | `EVALSHA seckill.lua`（单券） | | | **单 key 上限，加节点无效** |
| Redis | 点赞 Lua | | | |
| MySQL | 主键点查 | | | |
| MySQL | 热点行 `stock - 1` | | | 秒杀落库的硬上限 |
| MySQL | 热点行 `stock - 24` | | | × 24 = 批量聚合后的订单上限 |
| MySQL | 24 行多值 INSERT | | | |
| Kafka | 生产（acks=all） | | | |
| Kafka | 消费 | | | |

## 3. 单实例拐点（第 2 步）

| 场景 | 拐点并发 | 拐点 QPS | 拐点 P99 | 安全容量（× 0.6–0.7） | 拐点时先饱和的资源 |
| --- | ---: | ---: | ---: | ---: | --- |
| 店铺详情 | | | | | |
| 秒杀 | | | | | |
| 混合场景 | | | | | |

## 4. 扩展效率（第 3 步，混合场景）

| 副本数 | QPS | P99 ms | 扩展效率 | Redis CPU | MySQL 行锁等待 /s | Hikari pending | Kafka lag |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | | | 1.00 | | | | |
| 2 | | | | | | | |
| 4 | | | | | | | |
| 8 | | | | | | | |

**效率跌破 0.7 的副本数**：___；**同时饱和的组件**：___。

## 5. 每请求的组件访问次数（容量模型输入）

从 Grafana「每请求 Redis 命令数」、MySQL QPS ÷ 接口 QPS、Kafka 流入速率 ÷ 秒杀 QPS 得到：

| 接口 | Redis 次数 | MySQL 读 | MySQL 写 | Kafka |
| --- | ---: | ---: | ---: | ---: |
| 店铺详情 | | | | |
| 热门笔记 / Feed | | | | |
| 秒杀 | | | | |

改造项对本表的影响（不依赖压测环境，逐项记录）：

| 改造 | 接口 | 变化 |
| --- | --- | --- |
| B1 布隆过滤器 `BITFIELD` | 店铺详情 | Redis 8 → 2 |
| E1 过载保护 | 全部 | Redis 命令数、SQL 条数不变（只改变过载时的失败方式）；被并发闸拒绝的请求 SQL 为 0 |
| E2 布隆过滤器容量 | 店铺详情 | 存在的 id 不变（Redis 2）；不存在的 id 在 10 万家店时从 Redis 6 + MySQL 读 1（几乎全部穿透）回到 Redis 1 |
| E3 Feed 异步推送 + 推拉结合 | 发笔记 | 请求内 Redis：2 × 粉丝数（6.3 万粉丝约 12.6 万条）→ 2；MySQL 读「全部粉丝」→ 0；Kafka 0 → 1。消费端：普通作者每 1000 粉丝 1 次 MySQL 读 + 2000 条 Redis 命令，大 V 为 1 条 `SISMEMBER` |
| E3 Feed 异步推送 + 推拉结合 | 读关注 Feed | Redis 1 → 3 + 关注的大 V 数（两次往返）；MySQL 不变 |

## 6. 容量模型结论

业务目标：DAU ___，人均每日请求 ___，峰值系数 ___，秒杀单券峰值 ___ /s。

（粘贴 `python perf/baseline/capacity_model.py perf/baseline/capacity-model.json --md` 的输出）

## 7. 改造优先级（3-1.4 决策）

按「瓶颈组件 × 改造成本 × 风险」排序。默认顺序 B → C → D，**以本报告数据为准调整**。

| 瓶颈（实测依据） | 对应改造项 | 批次 | 成本 | 风险 | 优先级 |
| --- | --- | --- | --- | --- | --- |
| 例：Redis CPU 先到顶，HGETALL 占命令数 ___% | 登录态本地短缓存 | B | 低 | 低 | |
| ~~已知：`/shop/{id}` 每次 7 次逐个 GETBIT（布隆过滤器）~~ | 已完成：改为一条 `BITFIELD`，每请求 Redis 命令数 8 → 2 | B | 低 | 低 | 已完成 |
| 例：秒杀单券 Lua 达到单 key 上限 | 售罄本地标记 → 库存分段 | B → D | 低 → 高 | 低 → 高 | |
| 例：热点行 UPDATE / 行锁等待 | 已有批量聚合；不够再库存分段 | D | 高 | 高 | |
| 例：MySQL 连接数 / 读 QPS | 读写分离 + 连接数核算 | C | 中 | 中 | |
| 单点：MySQL / Redis / Kafka 各 1 节点 | Kafka 3 broker、主从 / 哨兵 | C | 中 | 中 | |

**结论**：先做 ___，因为 ___。进入 3-2 的门槛（有基线数据、确认先改哪个组件）是否满足：___。

---

## 附：本机同机冒烟（2026-11-19，不作为基线）

工具链验证时在开发机（Windows 11 + Docker Desktop，压测客户端与所有组件同机）上的小规模结果，**只说明工具可用和数量级**：

| 项 | 结果 | 说明 |
| --- | --- | --- |
| Redis HGETALL / `seckill.lua` | 约 8 万 / 4–7 万 ops/s | redis-benchmark，c=50 |
| MySQL 热点行 `stock = stock - 1` | 约 480 次/s，P95 54ms | 20 线程；同一行串行加锁 + 每次提交刷盘 |
| MySQL 热点行 `stock = stock - 24` | 约 430 次/s ≈ 1 万单/s | 同样的 UPDATE 次数，每次扣 24 单：批量聚合是秒杀落库能跑起来的前提 |
| Kafka 生产（acks=all，200B） | 约 5.5 万条/s | 5 万条小样本 |
| `/shop/{id}` 的 Redis 往返 | 7 × GETBIT + 1 × GET（+ 登录用户 1 × HGETALL） | SkyWalking 链路；与数据量无关，可直接用于容量模型。**之后已把 7 × GETBIT 合成 1 × BITFIELD**，容量模型里按 2 次（登录用户 3 次）算 |

热点行 UPDATE 只有几百次每秒这一点值得在正式环境优先复测：它决定了不做库存分段时，单张券落库的上限约等于「热点行 UPDATE 次数 × 批量聚合度」。
