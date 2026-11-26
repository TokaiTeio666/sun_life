# 分机压测部署

容量基线（阶段 3-1）要求压测机与被测服务分开，并且能看出是哪个共享组件先饱和。
这份文档说明怎么把现有的单机 compose 编排拆到几台机器上。测法本身见 [../perf/baseline/README.md](../perf/baseline/README.md)。

## 拓扑与规格

| 机器 | 规格 | 运行的服务 |
| --- | --- | --- |
| 压测机 | 8 核 16 G | JMeter、`baseline.py`、造数与重置脚本 |
| 应用机 | 16 核 32 G | app × N、nginx 网关、Prometheus、Grafana、三个 exporter、cAdvisor |
| PostgreSQL 机 | 8 核 16 G，SSD 云盘 200 G 以上 | postgres |
| 中间件机 | 8 核 16 G，SSD 云盘 | redis、kafka |

- 四台机器在同一可用区，全部走内网 IP；选独享型实例，不要用突发性能型（CPU 积分用完会被限速）。
- 监控放在应用机上：Prometheus 靠 compose 网络内的 DNS 发现 app 的所有副本，跨机器发现不了。
  exporter 也在应用机上，远程连接 PostgreSQL / Redis / Kafka。它们加起来占不到 1 核。
- PostgreSQL 机和中间件机的整机 CPU、磁盘 IO 没有接入 Prometheus，压测时用云厂商的主机监控或 `top` / `iostat` 看。

> **安全**：Redis 没有密码，PostgreSQL、Kafka、Prometheus 的端口都会监听在所有网卡上。
> 安全组只放行这四台机器所在的内网网段；需要从外面看 Grafana（3000）时只放行自己的 IP。

## 去哪租机器

阿里云 ECS 或腾讯云 CVM，按量付费，四台放同一个 VPC、同一可用区。两家都行，选已有账号和余额的那家。

- 规格族选计算型（阿里云 c7 / c8 系列，腾讯云 S / SA 标准型或 C 计算型）：核数和内存 1:2，正好对应上表的 8 核 16 G 和 16 核 32 G。
- 不要选突发性能型（阿里云 t5 / t6）和轻量应用服务器：前者 CPU 积分用完会限速，后者是共享资源且带宽受限。
- 磁盘选 ESSD 或 SSD 云盘：PostgreSQL 机 200 G 以上，其余 40–100 G 够用。
- 公网只给压测机和应用机：按流量计费的小带宽即可，用来 SSH 和看 Grafana；另外两台从这两台跳过去。

**费用量级**（粗略估算，未按当期报价核实，以控制台为准）：

| 计费方式 | 四台合计 | 说明 |
| --- | --- | --- |
| 按量付费 | 约 10 元 / 小时上下 | 稳定，不会被回收 |
| 抢占式实例 | 约为按量的 1–3 折 | 可能被回收，回收后数据一起没了 |

整轮基线（部署、造数、组件单测、拐点、扩展效率）预计一到两个工作日的机时，按量付费大概一两百元，抢占式几十元。

**省钱的做法**：

- PostgreSQL 机用按量付费，其余三台用抢占式：PostgreSQL 上有灌好的 1000 万订单，被回收要重灌；另外三台重建成本低。
- 中途休息时停机：阿里云按量实例支持「停机不收费」（只收磁盘钱），腾讯云有类似选项，过夜不用释放。
- 先用小规格走通流程：第一次多机启动大概率要排错，可以先租四台 2 核 4 G 把部署和连通性跑通，再换成正式规格。
- 测完立刻释放，并确认云盘和公网 IP 一起释放了，这两样会单独持续计费。

## 改了什么

单机一键启动的行为不变，下面这些都是新增的可选变量（默认值与原来相同）：

| 变量 | 默认 | 作用 |
| --- | --- | --- |
| `APP_PG_ADDR` / `APP_REDIS_HOST` / `APP_REDIS_PORT` / `APP_KAFKA_BOOTSTRAP` | `postgres:5432` / `redis` / `6379` / `kafka:9092` | app 和 exporter 连接的中间件地址 |
| `KAFKA_EXTERNAL_HOST` / `KAFKA_EXTERNAL_PORT` | `localhost` / `9094` | Kafka 给 compose 网络外的客户端用的对外地址 |
| `APP_JAVA_OPTS` | `-Xms256m -Xmx512m ...` | app 的 JVM 参数 |
| `PG_MAX_CONNECTIONS` / `PG_SHARED_BUFFERS` / `PG_EFFECTIVE_CACHE_SIZE` / `PG_MAX_WAL_SIZE` | `100` / `128MB` / `4GB` / `1GB` | PostgreSQL 容量参数（`POSTGRES_PASSWORD` 为密码，宿主端口 `PG_PORT` 默认 5433） |
| `APP_CPUS` | `2` | 每个 app 副本的 CPU 上限（只在 `docker-compose.bench-app.yml` 里生效） |

Kafka 现在有两个监听器：compose 网络内的客户端仍然用 `kafka:9092`，网络外的客户端用 `<KAFKA_EXTERNAL_HOST>:9094`。

## 部署步骤

三台被测机都装好 Docker（带 compose 插件，2.24 以上），拉下本仓库，并准备同一份环境变量文件：

```bash
cp deploy/bench.env.example deploy/bench.env
# 编辑 deploy/bench.env：换成实际的内网 IP 和密码
```

**PostgreSQL 机**（数据卷为空时首次启动自动导入 `hmdp.sql`）：

```bash
docker compose --env-file deploy/bench.env -f deploy/docker-compose.yml up -d postgres
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
docker run --rm -v "$PWD":/w -v hmdp-m2:/root/.m2 -w /w maven:3.9-eclipse-temurin-21 mvn -q clean package -DskipTests

BENCH="--env-file deploy/bench.env -f deploy/docker-compose.yml -f deploy/docker-compose.monitoring.yml -f deploy/docker-compose.bench-app.yml --profile resources"
docker compose $BENCH up -d --build --scale app=1
curl http://localhost:8081/shop/1        # 返回店铺 JSON 即三台机器已连通
```

`docker-compose.bench-app.yml` 让应用机不启动 postgres / redis / kafka，并给每个 app 副本加 CPU 上限。

## 造数与压测（在压测机上）

压测机需要 Docker、Python 3.12+、JDK 和 JMeter 5.5+。先把被测地址导出成环境变量，后面的脚本都认这一组：

```bash
export NET=host PG_HOST=10.0.0.11 PG_PORT=5432 PG_PASSWORD=<密码> REDIS_HOST=10.0.0.12 KAFKA_BOOTSTRAP=10.0.0.12:9094

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

副本数 × `DB_POOL_MAX` 要明显小于 `PG_MAX_CONNECTIONS`（PG 每个连接一个后端进程，连接多了内存和调度开销都上来）；示例配置是 6 × 30 = 180，上限 500。

## 预算有限：两台机器

压测机 + 一台 16 核 32 G 的被测机。被测机直接用单机编排，只借用 `bench.env` 里的容量参数：
把 `APP_*` 地址和 `KAFKA_EXTERNAL_HOST` 那几行删掉（回到容器服务名），`PG_SHARED_BUFFERS` 改成 `6GB`，然后

```bash
docker compose --env-file deploy/bench.env -f deploy/docker-compose.yml -f deploy/docker-compose.monitoring.yml up -d --build
```

这样单实例拐点可信，但所有组件抢同一台机器的 CPU，扩展效率偏低估，瓶颈归因也不如分机清楚。

## 完全不想租服务器：本机基线

压测客户端和整套服务跑在同一台开发机上。**结论只能当该机器的下限**：绝对 QPS 不能与分机基线横比，
`--scale` 也不会增加总容量，所以第 3 步（扩展效率）不做，只做组件单测 + 单实例拐点 + 混合场景。

```bash
# 1. 资源隔离：给 WSL 限线程和内存，把剩下的核留给 Windows 上的 JMeter
#    C:\Users\<user>\.wslconfig: memory=20GB / processors=16，然后 wsl --shutdown

# 2. 用本机专用的环境变量启动（端口、密码、缓冲池、堆大小都在里面）
cp deploy/bench.local.env.example deploy/bench.local.env
docker compose --env-file deploy/bench.local.env \
  -f deploy/docker-compose.yml -f deploy/docker-compose.monitoring.yml --profile resources up -d --build

# 3. 1/10 规模造数（约 150 MB，几十秒），然后重启 app 让布隆过滤器预热
python perf/baseline/seed_data.py --users 100000 --shops 10000 --orders 1000000 \
    --feed-users 10000
docker compose --env-file deploy/bench.local.env -f deploy/docker-compose.yml restart app frontend

# 4. 组件单测 + 单实例拐点
bash perf/baseline/component-bench.sh redis pg kafka
python perf/baseline/baseline.py knee --jmx perf/shop-detail-bench.jmx \
    --threads 50,100,200,400 --duration 30 --prop SHOP_MAX=10000
python perf/baseline/baseline.py knee --jmx perf/mixed-bench.jmx --token-file token.txt \
    --threads 50,100,200,400 --duration 30 --prop SHOP_MAX=10000 \
    --before-each "bash perf/baseline/reset-seckill.sh"
```

注意事项：

- **JMeter 在 Windows 宿主上跑**（如 `D:\apache-jmeter-5.6.3\bin`），不要放进 WSL，否则和被测服务抢同一个 cgroup；
- 部署时如果 `auth.docker.io` 超时，可从镜像站拉基础镜像再打回原名，构建加 `--pull=false`；
- `shared_buffers` 由启动参数 `PG_SHARED_BUFFERS` 决定（本机示例 4GB），改了要重启 postgres；`seed_data.py --shared-buffers-gb` 用 `ALTER SYSTEM` 改，同样要重启才生效；
- 结果填进报告的「本机基线」一节，并注明「被测机 = 压测机」。

## 收尾与退租

测完先留证据，再释放资源，否则云盘 / EIP 会继续计费：

1. **拷回结果**：`perf/baseline/results/` 下的 `summary.md`、`results.json`、`component.md`，以及各面板的 Grafana 截图；
   填进 [capacity-baseline-report.md](capacity-baseline-report.md)；
2. **保留配置**：`deploy/bench.env`（含内网 IP 与密码）拷回本地；业务数据不用导，`seed_data.py` 可以按目标规模重新生成；
3. **云控制台逐项释放**：实例 → 弹性公网 IP（EIP）→ 云盘 → 快照 → 带宽包 / 负载均衡。
   **只释放实例的话，独立云盘和 EIP 仍会按天计费**；
4. **以后可能回来**：先给系统盘打快照再释放实例，快照按存储量收费；或者下次直接按量付费几小时重跑；
5. **本机基线环境**（如果用的是「完全不想租服务器」那套）：

   ```bash
   docker compose --env-file deploy/bench.local.env \
     -f deploy/docker-compose.yml -f deploy/docker-compose.monitoring.yml --profile resources down
   ```

   数据卷会保留，下次 `up -d` 继续用；`C:\Users\<user>\.wslconfig` 里的 WSL 资源限制不需要时删掉，再 `wsl --shutdown`。
