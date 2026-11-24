#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
压测期间按固定间隔抓 Prometheus 指标，落成 CSV，给《容量基线报告》的瓶颈归因用。

分机基线一般直接截 Grafana；本机基线（被测机 = 压测机）没有第二台机器看盘，
一边跑 JMeter 一边用这个脚本把关键指标记下来，跑完对着时间窗口看哪个先饱和。

用法（监控编排已启动）：
  python perf/baseline/sample_metrics.py --duration 600 --out perf/baseline/results/metrics.csv
  # 另一个终端跑 baseline.py knee / scale

列：时间、接口 QPS / P99 / 5xx、各容器 CPU、Hikari pending、MySQL 线程与行锁等待、Redis 命令数。
只用标准库，需要 Python 3.8+。
"""
import argparse
import csv
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime

# 列名 -> PromQL。容器名来自 docker-compose 的 container_name（app 多副本时是 sun_life-app-N）。
QUERIES = {
    # 接口层：HTTP QPS / 全局 P99 / 5xx 速率
    "qps": "sum(rate(http_server_requests_seconds_count[30s]))",
    "p99_ms": "1000 * histogram_quantile(0.99, sum(rate(http_server_requests_seconds_bucket[30s])) by (le))",
    "err_5xx": 'sum(rate(http_server_requests_seconds_count{status=~"5.."}[30s]))',
    # 应用与共享组件的 CPU（单位：核）
    "app_cpu": 'sum(rate(container_cpu_usage_seconds_total{name=~"sun_life-app-.*"}[30s]))',
    "redis_cpu": 'sum(rate(container_cpu_usage_seconds_total{name="hmdp-redis"}[30s]))',
    "mysql_cpu": 'sum(rate(container_cpu_usage_seconds_total{name="hmdp-mysql"}[30s]))',
    "kafka_cpu": 'sum(rate(container_cpu_usage_seconds_total{name="hmdp-kafka"}[30s]))',
    # 应用侧的排队信号：Hikari 等连接数（虚拟线程下看这个，不看 tomcat 线程）
    "hikari_pending": "sum(hikaricp_connections_pending)",
    "hikari_active": "sum(hikaricp_connections_active)",
    # MySQL：并发线程与热点行锁等待速率
    "mysql_threads_running": "mysql_global_status_threads_running",
    "row_lock_waits_s": "rate(mysql_global_status_innodb_row_lock_waits[30s])",
    "row_lock_avg_ms": "rate(mysql_global_status_innodb_row_lock_time[30s]) / clamp_min(rate(mysql_global_status_innodb_row_lock_waits[30s]), 1)",
    # Redis：命令速率
    "redis_cmds_s": "rate(redis_commands_processed_total[30s])",
    # Kafka 消费 lag（没有消息时该序列不存在，记为 0）
    "kafka_lag": "sum(kafka_consumergroup_lag)",
}


def query(base, expr):
    url = f"{base}/api/v1/query?" + urllib.parse.urlencode({"query": expr})
    with urllib.request.urlopen(url, timeout=10) as r:
        data = json.load(r)
    if data.get("status") != "success":
        raise RuntimeError(data)
    result = data["data"]["result"]
    if not result:
        return 0.0
    return float(result[0]["value"][1])


def main():
    ap = argparse.ArgumentParser(description="压测期间抓 Prometheus 指标到 CSV", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ap.add_argument("--prometheus", default=os.environ.get("PROMETHEUS_URL", "http://localhost:9090"))
    ap.add_argument("--interval", type=float, default=5.0, help="采样间隔秒数")
    ap.add_argument("--duration", type=int, default=0, help="采样总秒数，0 = 直到 Ctrl+C")
    ap.add_argument("--out", default="perf/baseline/results/metrics.csv")
    a = ap.parse_args()

    cols = ["time"] + list(QUERIES)
    os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
    started = time.time()
    with open(a.out, "w", newline="", encoding="utf-8") as f, \
         open(os.path.splitext(a.out)[0] + ".promql", "w", encoding="utf-8") as qf:
        qf.write("# 本文件由 sample_metrics.py 生成，记录每个 CSV 列的 PromQL\n")
        for k, v in QUERIES.items():
            qf.write(f"{k}\t{v}\n")
        w = csv.writer(f)
        w.writerow(cols)
        f.flush()
        print(f"采样中：{a.out}（间隔 {a.interval:g}s，Ctrl+C 结束）", flush=True)
        try:
            while True:
                row = [datetime.now().strftime("%H:%M:%S")]
                for expr in QUERIES.values():
                    try:
                        row.append(f"{query(a.prometheus, expr):.3f}")
                    except Exception as e:  # 单个查询失败不影响整行
                        row.append("")
                        print(f"[WARN] {expr}: {e}", file=sys.stderr, flush=True)
                w.writerow(row)
                f.flush()
                if a.duration and time.time() - started >= a.duration:
                    break
                time.sleep(a.interval)
        except KeyboardInterrupt:
            pass
    print(f"完成：{a.out}", flush=True)


if __name__ == "__main__":
    main()
