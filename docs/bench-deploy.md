# 分机压测部署

容量基线（阶段 3-1）要求压测机与被测服务分开，并且能看出是哪个共享组件先饱和。
这份文档说明怎么把现有的单机 compose 编排拆到几台机器上。测法本身见 [../perf/baseline/README.md](../perf/baseline/README.md)。

## 拓扑与规格

| 机器 | 规格 | 运行的服务 |
| --- | --- | --- |
| 压测机 | 8 核 16 G | JMeter、`baseline.py`、造数与重置脚本 |
| 应用机 | 16 核 32 G | app × N、nginx 网关、Prometheus、Grafana、三个 exporter、cAdvisor |
| MySQL 机 | 8 核 16 G，SSD 云盘 200 G 以上 | mysql |
| 中间件机 | 8 核 16 G，SSD 云盘 | redis、kafka |

- 四台机器在同一可用区，全部走内网 IP；选独享型实例，不要用突发性能型（CPU 积分用完会被限速）。
- 监控放在应用机上：Prometheus 靠 compose 网络内的 DNS 发现 app 的所有副本，跨机器发现不了。
  exporter 也在应用机上，远程连接 MySQL / Redis / Kafka。它们加起来占不到 1 核。
- MySQL 机和中间件机的整机 CPU、磁盘 IO 没有接入 Prometheus，压测时用云厂商的主机监控或 `top` / `iostat` 看。

> **安全**：Redis 没有密码，MySQL、Kafka、Prometheus 的端口都会监听在所有网卡上。
> 安全组只放行这四台机器所在的内网网段；需要从外面看 Grafana（3000）时只放行自己的 IP。

## 改了什么

单机一键启动的行为不变，下面这些都是新增的可选变量（默认值与原来相同）：

| 变量 | 默认 | 作用 |
| --- | --- | --- |
| `APP_MYSQL_ADDR` / `APP_REDIS_HOST` / `APP_REDIS_PORT` / `APP_KAFKA_BOOTSTRAP` | `mysql:3306` / `redis` / `6379` / `kafka:9092` | app 和 exporter 连接的中间件地址 |
| `KAFKA_EXTERNAL_HOST` / `KAFKA_EXTERNAL_PORT` | `localhost` / `9094` | Kafka 给 compose 网络外的客户端用的对外地址 |
| `APP_JAVA_OPTS` | `-Xms256m -Xmx512m ...` | app 的 JVM 参数 |
| `MYSQL_MAX_CONNECTIONS` / `MYSQL_BUFFER_POOL` / `MYSQL_REDO_LOG` | `151` / `128M` / `100M` | MySQL 容量参数 |
| `APP_CPUS` | `2` | 每个 app 副本的 CPU 上限（只在 `docker-compose.bench-app.yml` 里生效） |

Kafka 现在有两个监听器：compose 网络内的客户端仍然用 `kafka:9092`，网络外的客户端用 `<KAFKA_EXTERNAL_HOST>:9094`。

## 部署步骤

三台被测机都装好 Docker（带 compose 插件，2.24 以上），拉下本仓库，并准备同一份环境变量文件：

```bash
cp deploy/bench.env.example deploy/bench.env
# 编辑 deploy/bench.env：换成实际的内网 IP 和密码
```

**MySQL 机**（首次启动自动导入 `hmdp.sql`）：

```bash
docker compose --env-file deploy/bench.env -f deploy/docker-compose.yml up -d mysql
```

**中间件机**：

```bash
docker compose --env-file deploy/bench.env -f deploy/docker-compose.yml up -d redis kafka

# 确认 Kafka 的对外地址：第一行应是「<中间件机内网 IP>:9094 (id: 1 ...」
docker exec hmdp-kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9094 | head -1
```

如果第一行是 `localhost:9094`，说明 `KAFKA_EXTERNAL_HOST` 没有生效（漏配，或启动时没带 `--env-file`）。
这时应用机上的 app 能连上 9094，但拿到的 broker 地址是 `localhost`，秒杀订单发不出去。改好后重新 `up -d kafka`。

**应用机**（先打 jar，再叠加监控和应用机专用覆盖文件）：

```bash
# 没装 JDK / Maven 时用容器打包
docker run --rm -v "$PWD":/w -v hmdp-m2:/root/.m2 -w /w maven:3.9-eclipse-temurin-17 mvn -q clean package -DskipTests

BENCH="--env-file deploy/bench.env -f deploy/docker-compose.yml -f deploy/docker-compose.monitoring.yml -f deploy/docker-compose.bench-app.yml --profile resources"
docker compose $BENCH up -d --build --scale app=1
curl http://localhost:8081/shop/1        # 返回店铺 JSON 即三台机器已连通
```

`docker-compose.bench-app.yml` 让应用机不启动 mysql / redis / kafka，并给每个 app 副本加 CPU 上限。

## 造数与压测（在压测机上）

压测机需要 Docker、Python 3.12+、JDK 和 JMeter 5.5+。先把被测地址导出成环境变量，后面的脚本都认这一组：

```bash
export NET=host MYSQL_HOST=10.0.0.11 MYSQL_PASSWORD=<密码> REDIS_HOST=10.0.0.12 KAFKA_BOOTSTRAP=10.0.0.12:9094

# 1. 造数（默认 100 万用户 / 10 万店铺 / 1000 万订单）并生成 5000 个登录 token
#    在空库上一次灌到目标规模；分几次调大 --orders 补出来的订单分布与一次灌满的不同
python3 perf/baseline/seed_data.py --tokens 5000

# 2. 组件单测
SYSBENCH=1 bash perf/baseline/component-bench.sh
```

造数后**在应用机上重启 app**，让布隆过滤器重新预热：

```bash
docker compose $BENCH restart app frontend
```

然后回到压测机：

```bash
# 3. 单实例拐点
python3 perf/baseline/baseline.py knee --jmx perf/mixed-bench.jmx --host 10.0.0.10 \
    --token-file token.txt --prop SHOP_MAX=100000 --before-each "bash perf/baseline/reset-seckill.sh"

# 4. 扩展效率：压测机操作不了应用机的 docker，用 --manual-scale，每轮按提示到应用机上调副本数
python3 perf/baseline/baseline.py scale --jmx perf/mixed-bench.jmx --host 10.0.0.10 \
    --token-file token.txt --prop SHOP_MAX=100000 --before-each "bash perf/baseline/reset-seckill.sh" \
    --threads 1600 --replicas 1,2,4,6 --manual-scale
```

应用机上调副本数：

```bash
docker compose $BENCH up -d --no-build --no-recreate --scale app=4 && docker compose $BENCH restart frontend
```

副本数 × `DB_POOL_MAX` 要小于 `MYSQL_MAX_CONNECTIONS`；示例配置是 6 × 30 = 180，上限 500。

## 预算有限：两台机器

压测机 + 一台 16 核 32 G 的被测机。被测机直接用单机编排，只借用 `bench.env` 里的容量参数：
把 `APP_*` 地址和 `KAFKA_EXTERNAL_HOST` 那几行删掉（回到容器服务名），`MYSQL_BUFFER_POOL` 改成 `6G`，然后

```bash
docker compose --env-file deploy/bench.env -f deploy/docker-compose.yml -f deploy/docker-compose.monitoring.yml up -d --build
```

这样单实例拐点可信，但所有组件抢同一台机器的 CPU，扩展效率偏低估，瓶颈归因也不如分机清楚。

## 收尾

测完把 `perf/baseline/results/` 里的 `summary.md`、`results.json`、`component.md` 和 Grafana 截图拷回来，
填进 [capacity-baseline-report.md](capacity-baseline-report.md)，然后释放按量付费的机器。
