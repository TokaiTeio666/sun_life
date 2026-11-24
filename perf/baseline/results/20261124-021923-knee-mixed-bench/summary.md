# 单实例拐点：mixed-bench.jmx

- 时间：2026-11-24 02:22；每级 30s；目标 127.0.0.1:8081
- 判定：P99 ≤ 100ms 且错误率 ≤ 0.10%；另外 QPS 比上一级增长 < 5% 视为已饱和（并发增加只在排队）

| 并发 | QPS | 平均 RT ms | P95 ms | P99 ms | 错误率 | QPS 增幅 | 判定 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 50 | 13593 | 3.3 | 7 | 10 | 0.00% | — | ok |
| 100 | 14977 | 6.1 | 16 | 23 | 0.00% | +10% | ok |
| 200 | 14039 | 13.1 | 35 | 188 | 0.00% | -6% | P99 188ms > SLO 100ms |

**拐点**：并发 100，QPS **14977**（P99 23ms）。
**安全容量**（拐点 × 60–70%）：**8986 – 10484 QPS**。

拐点 QPS 填入 Grafana 变量 `single_qps`；安全容量填入容量模型 JSON 的 `capacity.app_safe_qps`。

按 Little 定律自检：QPS ≈ 并发 ÷ 平均 RT。实测 QPS 远低于该值说明压测机本身成了瓶颈。

## 分接口（每级）

**threads = 50**

| 接口 | QPS | P99 ms | 错误率 |
| --- | ---: | ---: | ---: |
| GET /shop/{id} | 9516 | 6 | 0.00% |
| GET /blog/hot | 2042 | 9 | 0.00% |
| GET /blog/of/follow | 1361 | 13 | 0.00% |
| POST /voucher-order/seckill/{id} | 682 | 8 | 0.00% |

**threads = 100**

| 接口 | QPS | P99 ms | 错误率 |
| --- | ---: | ---: | ---: |
| GET /shop/{id} | 10484 | 12 | 0.00% |
| GET /blog/hot | 2249 | 18 | 0.00% |
| GET /blog/of/follow | 1500 | 31 | 0.00% |
| POST /voucher-order/seckill/{id} | 750 | 17 | 0.00% |

**threads = 200**

| 接口 | QPS | P99 ms | 错误率 |
| --- | ---: | ---: | ---: |
| GET /shop/{id} | 9829 | 21 | 0.00% |
| GET /blog/hot | 2109 | 251 | 0.00% |
| GET /blog/of/follow | 1406 | 335 | 0.00% |
| POST /voucher-order/seckill/{id} | 703 | 55 | 0.00% |
