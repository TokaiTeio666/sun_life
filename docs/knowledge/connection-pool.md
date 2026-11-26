# 连接池：MySQL 与 Redis

> 背景：2026-11-24 在 i5 本机压测时发现，热门笔记与关注 Feed 并发越高 QPS 越低，根因是 Redis 连接池 `max-idle` 小于 `max-active`，
> 导致连接被反复新建。修复记录见 [behavior-changes.md](../behavior-changes.md) 第 3 节。本文整理背后的原理与调参方法。

## 1. 为什么需要连接池

应用访问 MySQL / Redis 都要经过一条 TCP 连接。新建一条连接的成本远高于执行一条命令：

1. TCP 三次握手；
2. 协议初始化：MySQL 握手与认证、选库、设置字符集；Redis 的 `HELLO` / `AUTH` / `SELECT`；
3. 关闭时四次挥手，主动关闭的一方还要在 `TIME_WAIT` 状态停留一段时间，占着本地端口。

一条 Redis `GET` 只要几微秒，建一次连接是它的几十到几百倍。每个请求现建现关，CPU 就主要耗在建连接上。

连接池的做法是**预先建好一批连接，反复使用**：

```
请求到达 → 从池里「借」一条空闲连接 → 用它执行命令 → 用完「还」回池里
                ↑ 没有空闲连接？
                ├─ 总数没到上限 → 新建一条
                └─ 已到上限     → 排队等别人归还（超过等待时间就报错）
```

## 2. MySQL 与 Redis 的关键区别：一条连接能否同时服务多个请求

### MySQL：一条连接同一时间只能执行一条 SQL

```
连接1: [请求A 的 SQL ......等结果......] [请求B 的 SQL ...]
```

- MySQL 协议是一问一答：发出 SQL 后必须等结果返回，这条连接才能执行下一条；
- 连接上带有状态：事务、会话变量、字符集。一个事务从开始到提交必须一直占着同一条连接；
- 所以**每个正在查库的请求都要独占一条连接**，池子大小直接决定了同时能有多少个请求在查库。

### Redis（Lettuce 客户端）：一条连接可以给很多请求共用

```
连接1: →A的GET →B的GET →C的HGETALL   ←A的结果 ←B的结果 ←C的结果
```

- Lettuce 基于 Netty 异步 IO：多个线程的命令可以连续写进同一条连接，不必等前一条返回。
  Redis 按顺序处理、按顺序返回，Lettuce 再把结果按顺序交还给各个调用方；
- 因此 Spring Data Redis 默认让**所有普通命令共用一条连接**（`shareNativeConnection=true`），这时完全用不到连接池；
- 只有以下情况必须独占一条连接，才会从池里借：
  1. **pipeline**（`executePipelined`）：一批命令要连续发出，不能和其他请求的命令混在一起；
  2. **事务**（`MULTI` / `EXEC`）；
  3. **阻塞命令**（如 `BLPOP`）：会一直占着连接等数据。

> 本项目里绝大多数 Redis 调用走共享连接，只有用了 pipeline 的接口（热门笔记、Feed 的批量 `GETBIT`、点赞 UV 等）会借池里的连接。
> 所以 `max-idle` 问题只出现在这两个接口上，店铺详情（命中本地缓存、不访问 Redis）完全不受影响。

## 3. 两个连接池的实现差异

| | MySQL：HikariCP | Redis：commons-pool2（Lettuce 使用） |
| --- | --- | --- |
| 多余的空闲连接何时关闭 | 空闲超过 `idle-timeout`（项目里 10 分钟）才关闭 | **归还时空闲数超过 `max-idle` 就立刻关闭** |
| 维持最少空闲连接 | 自动补足 | 要靠定时巡检（`time-between-eviction-runs`）才会补 |
| 连接最长使用时间 | `max-lifetime` 到期后换新连接 | 无对应参数 |

因此：

- HikariCP 里 `minimum-idle: 10` 小于 `maximum-pool-size: 30` 没有问题：高峰过后多出的连接要空闲 10 分钟才关，高峰期间不会反复新建；
- commons-pool2 里 `max-idle` 一旦小于 `max-active`，高并发时就会出现"归还即关闭、下次再新建"的循环。

### 案例：`max-idle: 20` / `max-active: 50`

1. 高并发时同时借出约 45 条连接；
2. 请求结束归还，池子发现空闲已有 20 条，多出的连接**直接关闭**；
3. 下一批请求又需要连接，池里不够，只好**重新新建**；
4. 循环往复，实测每秒新建约 1,100 个 Redis 连接，app 与 Redis 的 CPU 都耗在建连上。

把 `max-idle` 改为 50 后（i5-1340P 本机，wrk 压测，每档 20 秒）：

| 场景 | 修复前 QPS / P99 | 修复后 QPS / P99 |
| --- | --- | --- |
| 热门笔记 100 并发 | 3,825 / 553 ms | 10,399 / 27 ms |
| 热门笔记 200 并发 | 2,238 / 1.36 s（有超时） | 9,903 / 67 ms |
| Feed 100 并发 | 3,811 / 517 ms | 8,354 / 33 ms |
| Feed 200 并发 | 3,191 / 1.29 s（有超时） | 7,565 / 101 ms |
| Redis 新建连接 | 约 1,100 个/秒 | 2 分钟内共 72 个 |

## 4. 参数怎么定

### 通用估算方法：Little 定律

> 同时在用的连接数 ≈ 每秒借用次数 × 每次占用时间

例：每秒 2,000 次查库、每次占用 5 ms → 平均 10 条连接在用；再留 2–3 倍余量应付高峰。

### MySQL（HikariCP，`spring.datasource.hikari.*`）

| 参数 | 项目当前值 | 怎么定 |
| --- | --- | --- |
| `maximum-pool-size` | `${DB_POOL_MAX:30}` | **不是越大越好**。按 Little 定律估算再留余量；连接过多会让 MySQL 在线程切换、锁竞争上消耗更多。硬约束：**所有副本的连接数之和 < MySQL `max_connections`**（默认 151；30 × 4 副本 = 120 已接近上限） |
| `minimum-idle` | 10 | 长期高流量的服务，HikariCP 官方建议等于最大值（固定大小池）；流量起伏大时可以小一些以节省连接 |
| `connection-timeout` | `${DB_CONN_TIMEOUT_MS:800}` | 借不到连接时最多等多久。设短一些让过载时快速失败（配合 DbGate 返回 503），避免请求越积越多 |
| `max-lifetime` | 1800000（30 分钟） | 必须**短于** MySQL `wait_timeout`（默认 8 小时）以及中间网络设备的空闲断连时间，否则会拿到已被对端断开的连接 |
| `idle-timeout` | 600000（10 分钟） | 只在 `minimum-idle` 小于最大值时生效 |

### Redis（Lettuce + commons-pool2，`spring.data.redis.lettuce.pool.*`）

| 参数 | 项目当前值 | 怎么定 |
| --- | --- | --- |
| `max-active` | 50 | 只按 **pipeline / 事务 / 阻塞命令**的并发估算，普通命令不占池。例：每秒 1 万次 pipeline × 每次约 0.3 ms ≈ 平均 3 条，50 条余量充足。总数约束：副本数 × 50 远小于 Redis `maxclients`（默认 10,000） |
| `max-idle` | 50 | **必须等于 `max-active`**，否则归还时会关闭连接（见第 3 节案例） |
| `min-idle` | 5 | 预热用；需要 `time-between-eviction-runs`（项目里 10s）才会生效 |
| `max-wait` | 2000ms | 借不到连接时最多等多久，建议短一些，快速失败 |
| `spring.data.redis.timeout` | 2000ms | **单条命令的超时**，不是借连接的超时。Redis 命令正常是亚毫秒级，2 秒已经很宽松 |

## 5. 怎么判断参数是否合适

### MySQL

看 Grafana「JVM 与连接池」面板（指标说明见 [monitoring.md](../monitoring.md)）：

| 现象 | 含义 |
| --- | --- |
| `hikaricp_connections_pending` 长期 > 0 | 连接不够用，或 SQL 太慢占着连接不放 |
| `hikaricp_connections_timeout_total` 在增长 | 已经有请求借不到连接而报错 |
| 活跃连接数长期远低于上限 | 可以调小，把 MySQL 的连接名额留给其他副本 |

### Redis

```bash
docker exec hmdp-redis redis-cli INFO stats   | grep total_connections_received   # 应基本不涨
docker exec hmdp-redis redis-cli INFO clients | grep connected_clients            # 应稳定在 副本数 × 池大小 附近
```

压测前后各取一次 `total_connections_received`，相减再除以压测秒数，就是每秒新建连接数。
正常应接近 0；明显大于 0（例如每秒几十上百）说明连接池在反复建连。

> 目前 Grafana 与告警规则里还没有"Redis 新建连接速率"，可以基于 redis-exporter 的
> `redis_connections_received_total` 加一个 `rate(...[1m])` 面板与告警。

## 6. 速查

- MySQL 连接**独占**，池大小 = 同时查库的并发上限，要和 `max_connections` 一起算；
- Redis（Lettuce）普通命令**共享一条连接**，池只给 pipeline / 事务 / 阻塞命令用；
- commons-pool2 的 `max-idle` **必须等于** `max-active`；
- 等待超时（`connection-timeout` / `max-wait`）宜短，过载时快速失败；
- 判断依据：Hikari 的 pending / timeout 指标，Redis 的 `total_connections_received` 增速。
