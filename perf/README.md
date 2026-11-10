# 压测复现指南

本文记录 `perf/` 下两组压测的完整步骤与口径。所有数字均来自本机实跑，
环境为 Windows 11 + Docker Desktop（应用 / MySQL 8 / Redis 7 / RocketMQ 同机）。

## 0. 前置

```powershell
# 启动整套服务（首次会自动建库）
docker compose -f deploy/docker-compose.yml up -d --build
```

如果宿主机 8081 被占用（Windows 端口保留范围也会导致绑定失败），可换宿主端口：

```powershell
$env:APP_PORT='18081'
docker compose -f deploy/docker-compose.yml up -d
# 下文 JMeter 的 -JSERVER_PORT 也要改成 18081
```

## 1. 准备压测数据

```powershell
# 秒杀券 id=21，库存 100000（幂等）
docker exec -i hmdp-mysql mysql -uroot -p123456 hmdp < perf/bench-voucher.sql

# 应用不会重新预热已存在的 key，手动同步 Redis 库存
docker exec hmdp-redis redis-cli SET seckill:stock:21 100000
docker exec hmdp-redis redis-cli DEL seckill:order:21

# 生成 1000 个登录 token：写入 Redis 并输出 token.txt
pwsh perf/gen_tokens.ps1 -Count 1000
```

每轮压测前重置：

```powershell
docker exec hmdp-mysql mysql -uroot -p123456 hmdp -e "DELETE FROM tb_voucher_order WHERE voucher_id=21; UPDATE tb_seckill_voucher SET stock=100000 WHERE voucher_id=21;"
docker exec hmdp-redis redis-cli SET seckill:stock:21 100000
docker exec hmdp-redis redis-cli DEL seckill:order:21
```

## 2. 秒杀接口

```powershell
jmeter -n -t perf/seckill-bench.jmx `
  -JTHREADS=500 `
  -JTOKEN_FILE=D:/work/sun_life/token.txt `
  -JSERVER_PORT=8081 `
  -l perf/500.jtl -e -o perf/report-500
```

- `THREADS` 可取 200 / 500 / 1000；
- 脚本会循环请求，1000 个 token 会被全部消费，因此每轮最终都是 1000 个用户各成功下单一次；
- 结果看 `perf/report-*/statistics.json` 的 `pct2ResTime`（P95）、`pct3ResTime`（P99）。

实测（2026-11-10）：

| 并发 | 请求数 | QPS | 平均 RT | P95 | P99 | 错误率 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 200 | 185,515 | 6,247 | 31.5 ms | 39 ms | 42 ms | 0% |
| 500 | 176,534 | 5,983 | 82.8 ms | 92 ms | 98 ms | 0.04% |
| 1000 | 180,192 | 6,060 | 162.1 ms | 175 ms | 181 ms | 1.06%（TCP 拒绝） |

每轮结束校验：

```powershell
docker exec hmdp-mysql mysql -uroot -p123456 hmdp -e "SELECT COUNT(*) orders, COUNT(DISTINCT user_id) users FROM tb_voucher_order WHERE voucher_id=21; SELECT stock FROM tb_seckill_voucher WHERE voucher_id=21;"
docker exec hmdp-redis redis-cli GET seckill:stock:21
```

## 3. 商铺详情缓存 A/B

开启缓存（默认）：

```powershell
curl http://127.0.0.1:8081/shop/1     # 预热
jmeter -n -t perf/shop-detail-bench.jmx -JTHREADS=200 -JSERVER_PORT=8081 `
  -l perf/shop-cache.jtl -e -o perf/report-shop-cache
```

关闭缓存：用同一套 compose 重建 app，注入 `HMDP_CACHE_ENABLED=false`：

```powershell
$env:HMDP_CACHE_ENABLED='false'
docker compose -f deploy/docker-compose.yml up -d app
```

再跑同一个 JMX（输出到 `report-shop-db`），结束后恢复：

```powershell
$env:HMDP_CACHE_ENABLED='true'
docker compose -f deploy/docker-compose.yml up -d app
```

MySQL 查询量用 `SHOW GLOBAL STATUS LIKE 'Questions'` 在压测前后各取一次差值。

实测（200 并发 / 30s）：

| 指标 | 缓存 + 布隆 | 直查 MySQL |
| --- | ---: | ---: |
| QPS | 10,494 | 8,367 |
| 平均 RT | 18.6 ms | 23.5 ms |
| P95 | 51 ms | 62 ms |
| P99 | 77 ms | 86 ms |
| MySQL 查询量 | 7 | 250,550 |

## 4. BitMap 与 HyperLogLog 微基准

```powershell
# 100k 用户：BitMap 16,448 B vs ZSet 8,569,376 B
docker exec hmdp-redis redis-cli EVAL "for i=ARGV[1],ARGV[2] do redis.call('SETBIT', KEYS[1], i, 1) end" 1 test:bitmap:100k 0 99999
docker exec hmdp-redis redis-cli EVAL "for i=ARGV[1],ARGV[2] do redis.call('ZADD', KEYS[1], i, 'u'..i) end" 1 test:zset:100k 0 99999
docker exec hmdp-redis redis-cli MEMORY USAGE test:bitmap:100k
docker exec hmdp-redis redis-cli MEMORY USAGE test:zset:100k

# 清理
docker exec hmdp-redis redis-cli DEL test:bitmap:100k test:zset:100k
```

HLL 误差：10 轮、每轮 5 万个不同用户，平均绝对误差 **0.53%**（理论 0.81%）。

## 5. 分布式锁主从切换失效复现

```powershell
pwsh perf/lock-failover-demo.ps1
```

脚本会启动一对临时 Redis，断开复制后由客户端 A 在主库加锁，再停主、提升从库，
最后让客户端 B 在新主上申请同一把锁。预期 B 返回 `OK`，说明仅靠 Redis 锁在主从切换时并不可靠；
应用代码用「Redis 锁 + 数据库唯一索引/条件更新」做第二道防线。
