# 单实例拐点：mixed-bench.jmx

- 时间：2026-11-24 23:08；每级 30s；目标 127.0.0.1:8081
- 判定：P99 ≤ 100ms 且错误率 ≤ 0.10%；另外 QPS 比上一级增长 < 5% 视为已饱和（并发增加只在排队）

| 并发 | QPS | 平均 RT ms | P95 ms | P99 ms | 错误率 | QPS 增幅 | 判定 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 50 | 13697 | 3.3 | 7 | 10 | 0.00% | — | ok |
| 100 | 16460 | 5.5 | 13 | 18 | 0.00% | +20% | ok |
| 200 | 16651 | 11.0 | 26 | 40 | 0.00% | +1% | 饱和（QPS 不再增长） |
| 400 | 17156 | 21.4 | 62 | 106 | 0.00% | +3% | P99 106ms > SLO 100ms |

**拐点**：并发 100，QPS **16460**（P99 18ms）。
**安全容量**（拐点 × 60–70%）：**9876 – 11522 QPS**。

拐点 QPS 填入 Grafana 变量 `single_qps`；安全容量填入容量模型 JSON 的 `capacity.app_safe_qps`。

按 Little 定律自检：QPS ≈ 并发 ÷ 平均 RT。实测 QPS 远低于该值说明压测机本身成了瓶颈。

## 分接口（每级）

**threads = 50**

| 接口 | QPS | P99 ms | 错误率 |
| --- | ---: | ---: | ---: |
| GET /shop/{id} | 9588 | 6 | 0.00% |
| GET /blog/hot | 2057 | 8 | 0.00% |
| GET /blog/of/follow | 1372 | 14 | 0.00% |
| POST /voucher-order/seckill/{id} | 686 | 10 | 0.00% |

**threads = 100**

| 接口 | QPS | P99 ms | 错误率 |
| --- | ---: | ---: | ---: |
| GET /shop/{id} | 11523 | 10 | 0.00% |
| GET /blog/hot | 2472 | 14 | 0.00% |
| GET /blog/of/follow | 1647 | 27 | 0.00% |
| POST /voucher-order/seckill/{id} | 824 | 15 | 0.00% |

**threads = 200**

| 接口 | QPS | P99 ms | 错误率 |
| --- | ---: | ---: | ---: |
| GET /shop/{id} | 11657 | 20 | 0.00% |
| GET /blog/hot | 2500 | 30 | 0.00% |
| GET /blog/of/follow | 1667 | 54 | 0.00% |
| POST /voucher-order/seckill/{id} | 833 | 30 | 0.00% |

**threads = 400**

| 接口 | QPS | P99 ms | 错误率 |
| --- | ---: | ---: | ---: |
| GET /shop/{id} | 12012 | 33 | 0.00% |
| GET /blog/hot | 2580 | 96 | 0.00% |
| GET /blog/of/follow | 1720 | 167 | 0.00% |
| POST /voucher-order/seckill/{id} | 859 | 54 | 0.00% |
