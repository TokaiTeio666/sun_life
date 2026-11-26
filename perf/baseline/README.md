# 容量基线测量（阶段 3-1.2 / 3-1.3）

> 方法见 [../../plan/phase3-architecture-plan.md](../../plan/phase3-architecture-plan.md) 3-1.2–3-1.4；
> 结果填进 [../../docs/capacity-baseline-report.md](../../docs/capacity-baseline-report.md)。

## 为什么不能直接用 perf/README.md 里的数字

`perf/README.md` 中秒杀 200 / 500 / 1000 并发的 QPS 都在 1 万左右，平均 RT 随并发线性增长（20 / 49 / 101ms）。
按 Little 定律 QPS = 并发 ÷ RT：200 ÷ 20.4ms ≈ 9.8k、500 ÷ 49.1ms ≈ 10.2k、1000 ÷ 100.7ms ≈ 9.9k。
**约 1 万 QPS 时已经饱和**，多出来的并发只是在排队。但那次压测机、应用和所有中间件都在同一台机器上，
这是**整机上限**，不是应用上限，也看不出是哪个组件先到顶。

## 环境要求

- **压测机与被测服务分开部署**。同机时压测机抢 CPU，副本越多越被低估；分机部署步骤见 [../../docs/bench-deploy.md](../../docs/bench-deploy.md)；
- 没有条件分机时，按 [../../docs/bench-deploy.md](../../docs/bench-deploy.md) 的「完全不想租服务器：本机基线」跑：
  结果只能当这台机器的下限，并且跳过扩展效率（第 3 步）。2026-11-24 的实测见
  [../../docs/capacity-baseline-report.md](../../docs/capacity-baseline-report.md)；
- 被测环境带监控启动：`docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.monitoring.yml up -d --build`；
- 不要叠加 `docker-compose.tracing.yml`（agent 有开销）；
- 每轮压测时 Grafana 时间范围对准压测窗口，截图「容量」「JVM 与连接池」「Redis」「PostgreSQL」「Kafka」几组面板存档。

> 2026-11-24 之前的基线数字是在 MySQL 8 上测的；项目已迁移到 PostgreSQL 16，下面的命令都是 PostgreSQL 版本，
> 历史数字不能直接和 PostgreSQL 上的新结果对比（引擎、索引结构、热点行锁实现都不同），需要重测。

工具：`component-bench.sh` 需要 bash + docker；`baseline.py`、`capacity_model.py`、`seed_data.py` 只用标准库，需要 Python 3.12+；JMeter 5.5+。

## 第 0 步：造数

课程自带的 `hmdp.sql` 只有 14 家店、一千个用户，所有数据都在内存里，测出的读性能和秒杀落库性能都偏乐观。
正式基线要先把数据量灌到目标规模（默认 100 万用户 / 10 万店铺 / 1000 万订单，约 3–4 GB）：

```bash
python perf/baseline/seed_data.py --shared-buffers-gb 4       # 默认规模；shared_buffers 按机器内存的 25–40% 给（改完要重启 postgres，见下）
python perf/baseline/seed_data.py --users 100000 --shops 10000 --orders 1000000   # 1/10 规模试跑
```

- 数据由 PostgreSQL 服务端 `INSERT ... SELECT ... generate_series()` 生成，客户端是一次性 `postgres:16.4` 容器里的 psql，连接参数与 `component-bench.sh` 通用（`NET`、`PG_HOST`、`PG_PORT` 等）；
- 可重复执行、可中断续跑（进度记在 `seed_progress` 表），调大目标后重跑只补差额，不会删数据；
  但分几次调大 `--orders` 补出来的订单，时间分布和主键顺序与一次灌满的不同，要对比基线就在空库上一次灌到目标规模；
- 同时生成笔记、关注关系（含粉丝数万的大 V）、优惠券，以及 Redis 里的店铺坐标和关注收件箱，混合场景的每个接口都有数据可读；
- 订单没有「未支付」状态、秒杀券活动都已结束，不会触发超时关单，也不会拖慢应用启动；
- 灌数会话设 `synchronous_commit = off`（提交不等 WAL 刷盘，进度与数据同事务，断电后重跑即可续上）；PostgreSQL 无法关闭 WAL，所以没有 MySQL 版的 `--keep-binlog`。

灌完后**重启 app**（布隆过滤器启动时预热；不重启也会在 1 分钟内由定时增量同步补上）、重新生成 token，压测时加 `--prop SHOP_MAX=100000`。
PostgreSQL 默认的 `shared_buffers` 只有 128M，放不下这些数据。它不能在线修改：`--shared-buffers-gb`（旧名 `--buffer-pool-gb`）
只会执行 `ALTER SYSTEM SET shared_buffers` 写入 `postgresql.auto.conf` 并提示退出，
需要 `docker restart hmdp-postgres` 后再重跑本命令；如果 compose 用 `command: -c shared_buffers=...` 启动，命令行参数优先，要直接改 compose。
正式压测务必调大，否则测的是磁盘而不是数据库。
灌完后脚本会对各表 `ANALYZE`，让规划器拿到准确的统计信息。

> `RedisBloomFilter` 的设计容量默认 100 万家店铺（`BLOOM_EXPECTED_INSERTIONS`），启动日志会打印填充率和估算误判率，
> 店铺数超过设计容量时告警。灌数后可以用 `python perf/baseline/bloom_check.py --shops 100000` 检查：
> 存在的店铺全部放行、不存在的 id 误判率低于 2%。

## 第 1 步：组件单测

```bash
bash perf/baseline/component-bench.sh              # redis pg kafka 全部
bash perf/baseline/component-bench.sh redis        # 只测 Redis
PGBENCH_STD=1 bash perf/baseline/component-bench.sh pg    # 额外跑 pgbench 自带的 tpcb-like / select-only（scale=10，独立库 pgbench_std）
```

| 组件 | 测什么 | 工具 |
| --- | --- | --- |
| Redis | GET、HGETALL（登录态）、SISMEMBER（是否关注）、`EVALSHA seckill.lua`（随机 userId 抢同一张券）、点赞 Lua | redis-benchmark |
| PostgreSQL | 主键点查、热门笔记、超时关单扫描（走部分索引 `WHERE status = 1`）、热点行 `stock = stock - 1` 与 `- 24`、24 行多值 INSERT | pgbench（`-M prepared -f`）执行业务真实 SQL，P95 由 `--log` 的逐事务耗时算出 |
| Kafka | 12 分区 topic，`acks=all` + 幂等生产、消费吞吐 | kafka-producer/consumer-perf-test |

测试数据全部隔离并在结束后清理（Redis `bench:*` 与券 999999 的 key、PostgreSQL 券 999999 + `bench_voucher_order` 表与 `bench_id_seq` 序列、Kafka `bench.perf`）。
压 SQL 用 pgbench 而不是 sysbench：pgbench 随 `postgres:16.4` 镜像提供，版本与被测库一致，自带 `-c/-j/-T`、预编译协议和逐事务日志；
也不需要像 MySQL 那样临时建账号，直接用 `PG_USER`（默认 postgres）。
被测组件在另一台机器时：

```bash
NET=host REDIS_HOST=10.0.0.5 REDIS_PORT=6380 PG_HOST=10.0.0.5 PG_PORT=5433 \
KAFKA_BOOTSTRAP=10.0.0.5:9094 bash perf/baseline/component-bench.sh
```

> 跨机测 Kafka 用它的对外监听器 `<中间件机 IP>:9094`（由 `KAFKA_EXTERNAL_HOST` 指定对外地址），不要用 9092。
> 分机部署的完整步骤见 [../../docs/bench-deploy.md](../../docs/bench-deploy.md)。

可调参数：`REDIS_N`、`REDIS_CLIENTS`、`PG_CONCURRENCY`、`PG_TIME`、`KAFKA_RECORDS`、`KAFKA_RECORD_SIZE`；
连接与镜像：`PG_HOST`（默认 `postgres`）、`PG_PORT`（容器网络内默认 5432，宿主机映射端口默认 5433）、`PG_USER`、`PG_PASSWORD`、`PG_DB`、`PG_IMAGE`。

## 第 2 步：单实例找拐点

```bash
docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.monitoring.yml up -d --scale app=1
docker compose -f deploy/docker-compose.yml restart frontend

python perf/baseline/baseline.py knee --jmx perf/shop-detail-bench.jmx --host <网关IP> \
    --threads 50,100,200,400,800,1200 --duration 60
```

逐级加并发，P99 超过 SLO（`--slo-p99-ms`，默认 100）、错误率超过 0.1%（`--max-error`）、
或 QPS 比上一级增长不到 5%（已饱和，`--flat-gain`）就停。**拐点** = 最后一个达标级别；**安全容量** = 拐点 QPS × 60–70%。

秒杀脚本每轮前要重置库存，用 `--before-each` 挂上重置命令（参考 `perf/README.md` 第 1 节）：

```bash
python perf/baseline/baseline.py knee --jmx perf/seckill-bench.jmx --token-file token.txt \
    --before-each "bash perf/baseline/reset-seckill.sh"
```

## 第 3 步：扩展效率

```bash
python perf/baseline/baseline.py scale --jmx perf/mixed-bench.jmx --token-file token.txt \
    --host <网关IP> --threads 1600 --replicas 1,2,4,8
```

脚本依次 `--scale app=N`、等所有副本 healthy、重启网关、预热 15s，再正式压测。
`扩展效率 = N 副本 QPS ÷ (N × 单副本 QPS)`，**哪一级之后效率掉到 0.7 以下，就说明某个共享组件到顶了**——
对照同一时段的 Redis CPU、PostgreSQL 锁等待会话（`pg_lock_waiting_sessions`）/ 连接数（`pg_stat_database_numbackends`）、Kafka lag、Hikari pending，先饱和的就是改造的第一优先级。

- `--threads` 要大到能压满最大副本数（例如单副本拐点并发 × 最大副本数），否则高副本数时是压力不够而不是效率低；
- PostgreSQL `max_connections` 默认 100（每个连接是一个后端进程，开销比 MySQL 线程大），4 副本 × `DB_POOL_MAX=30` = 120 就会超，
  先调小 `DB_POOL_MAX`、调大 `max_connections`，副本多时在应用与库之间加 PgBouncer；上限看 Prometheus 的 `pg_conn_limit_max_connections`；
- 压测机在另一台机器、无法操作被测机的 docker 时加 `--manual-scale`，每轮前按提示手动调副本数。

## 第 4 步：混合场景

`perf/mixed-bench.jmx` 按真实流量配比：店铺详情 70% / 热门笔记 15% / 关注 Feed 10% / 秒杀 5%。
比例用 `--prop PCT_SHOP=60 --prop PCT_SECKILL=15` 之类覆盖；`knee` / `scale` 都可以用它。
只压单接口得出的结论偏乐观，**容量结论以混合场景为准**。

## 第 5 步：容量模型

```bash
cp perf/baseline/capacity-model.example.json perf/baseline/capacity-model.json
# 用第 1–4 步的实测值替换 capacity.*，用 Grafana「每请求 Redis 命令数」等替换 interfaces.*
python perf/baseline/capacity_model.py perf/baseline/capacity-model.json --md
python perf/baseline/capacity_model.py perf/baseline/capacity-model.json --peak-qps 1000000   # 推演
```

## 结果目录

每次运行写到 `perf/baseline/results/<时间>-<类型>-<脚本>/`：`summary.md`（结论表格）、`results.json`（原始数字）、
每一级的 JMeter HTML 报告。`baseline.py report <目录>` 可在调整判定参数后重新汇总。
JMeter 原始 jtl 与 HTML 报告不入库（见 `.gitignore`），`summary.md` / `results.json` / `component.md` 入库。
