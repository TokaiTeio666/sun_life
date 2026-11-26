#!/usr/bin/env python3
"""
容量模型（阶段 3-1.3）：业务目标 → 峰值 QPS → 各组件负载 → 所需节点数。

  平均 QPS = DAU × 人均每日请求数 ÷ 86400
  峰值 QPS = 平均 QPS × 峰值系数
  组件负载 = Σ(接口 QPS × 该接口每次请求访问该组件的次数) × (1 − 上一层缓存命中率)
  所需节点数 = 组件负载 × 冗余系数 ÷ 单节点安全容量
  所需 app 实例数 = 峰值 QPS × 冗余系数 ÷ (单实例安全容量 × 扩展效率)

用法：
  python perf/baseline/capacity_model.py perf/baseline/capacity-model.example.json
  python perf/baseline/capacity_model.py model.json --peak-qps 1000000     # 直接指定峰值做推演
  python perf/baseline/capacity_model.py model.json --dau 50000000 --md   # 输出 Markdown，贴进容量报告
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path


def ceil(x: float) -> int:
    return max(1, math.ceil(x - 1e-9))


def evaluate(m: dict, peak_override: float | None = None) -> dict:
    b, cap, red = m["business"], m["capacity"], m["redundancy"]
    hit = m.get("local_cache_hit", {})
    avg_qps = b["dau"] * b["requests_per_user_per_day"] / 86400
    peak_qps = peak_override if peak_override else avg_qps * b["peak_factor"]

    share_sum = sum(i["share"] for i in m["interfaces"])
    if abs(share_sum - 1) > 1e-6:
        raise SystemExit(f"interfaces.share 之和应为 1，当前 {share_sum}")

    per_api = [{"name": i["name"], "qps": peak_qps * i["share"]} for i in m["interfaces"]]

    def load(key: str) -> float:
        raw = sum(peak_qps * i["share"] * i.get(key, 0) for i in m["interfaces"])
        return raw * (1 - hit.get(key, 0.0))

    redis_ops = load("redis")
    pg_read = load("pg_read")
    pg_write = load("pg_write")
    kafka_msgs = load("kafka")

    app_n = ceil(peak_qps * red / (cap["app_safe_qps"] * cap["app_scale_efficiency"]))
    comps = [
        ("app 实例", peak_qps, cap["app_safe_qps"] * cap["app_scale_efficiency"], app_n, "加副本，线性扩展"),
        ("Redis 主节点", redis_ops, cap["redis_ops"], ceil(redis_ops * red / cap["redis_ops"]), "Cluster：N 主 ≈ N × 单节点"),
        ("PostgreSQL 主库（写）", pg_write, cap["pg_write_tps"], ceil(pg_write * red / cap["pg_write_tps"]), "分库后 ≈ 库数 × 单库"),
        ("PostgreSQL 从库（读）", pg_read, cap["pg_read_qps"], ceil(pg_read * red / cap["pg_read_qps"]), "加从库"),
        ("Kafka 分区", kafka_msgs, cap["kafka_partition_msgs"], ceil(kafka_msgs * red / cap["kafka_partition_msgs"]), "瓶颈通常在消费端写库"),
    ]

    warnings = []
    sk = b.get("seckill_peak_qps", 0)
    if sk and sk > cap["redis_lua_single_key"]:
        warnings.append(
            f"秒杀单券峰值 {sk:,.0f}/s 超过单 key Lua 上限 {cap['redis_lua_single_key']:,.0f}/s。"
            "单张券的 key 只在一个分片上，加 Redis 节点无效：需要售罄本地标记、库存分段（3-2 批次 B / D）。")
    conns = app_n * cap["db_pool_per_app"]
    if conns > cap["pg_max_connections"] * 0.8:
        warnings.append(
            f"{app_n} 个 app × 连接池 {cap['db_pool_per_app']} = {conns} 个连接，超过 PostgreSQL max_connections "
            f"{cap['pg_max_connections']} 的 80%：调大 max_connections、调小 DB_POOL_MAX，或读写分离分摊。")
    redis_nodes = comps[1][3]
    if redis_nodes > 1:
        token_ops = peak_qps * (1 - hit.get("redis", 0.0))
        warnings.append(
            f"Redis 需要 {redis_nodes} 个主节点，其中仅登录态 HGETALL 就有 {token_ops:,.0f} ops/s："
            "登录态做本地短缓存或改无状态 token 可直接减掉这一部分（3-2 批次 B）。")

    return {"avg_qps": avg_qps, "peak_qps": peak_qps, "per_api": per_api, "components": comps, "warnings": warnings}


def render(r: dict, m: dict, markdown: bool) -> str:
    b = m["business"]
    out = []
    h = (lambda s: f"\n## {s}\n") if markdown else (lambda s: f"\n== {s} ==")
    out.append(h("业务目标 → QPS"))
    out.append(f"- DAU {b['dau']:,}，人均每日 {b['requests_per_user_per_day']} 次，峰值系数 {b['peak_factor']}")
    out.append(f"- 平均 QPS：**{r['avg_qps']:,.0f}**")
    out.append(f"- 峰值 QPS：**{r['peak_qps']:,.0f}**")
    out.append(h("峰值时各接口 QPS"))
    out.append("| 接口 | QPS |\n| --- | ---: |" if markdown else "")
    for a in r["per_api"]:
        out.append(f"| {a['name']} | {a['qps']:,.0f} |" if markdown else f"  {a['name']:<40} {a['qps']:>12,.0f}")
    out.append(h(f"组件负载与所需节点数（冗余系数 {m['redundancy']}）"))
    if markdown:
        out.append("| 组件 | 峰值负载 /s | 单节点安全容量 /s | 所需数量 | 扩展方式 |\n| --- | ---: | ---: | ---: | --- |")
    for name, load, cap, n, how in r["components"]:
        out.append(f"| {name} | {load:,.0f} | {cap:,.0f} | **{n}** | {how} |" if markdown
                   else f"  {name:<16} 负载 {load:>12,.0f}  单节点 {cap:>10,.0f}  需要 {n:>4}  （{how}）")
    if r["warnings"]:
        out.append(h("需要注意"))
        out += [f"- {w}" for w in r["warnings"]]
    return "\n".join(x for x in out if x != "") + "\n"


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("model", help="模型 JSON，参考 capacity-model.example.json")
    p.add_argument("--dau", type=int, help="覆盖 business.dau")
    p.add_argument("--peak-qps", type=float, help="直接指定峰值 QPS（忽略 DAU 与峰值系数）")
    p.add_argument("--md", action="store_true", help="输出 Markdown 表格")
    a = p.parse_args()
    m = json.loads(Path(a.model).read_text(encoding="utf-8"))
    if a.dau:
        m["business"]["dau"] = a.dau
    print(render(evaluate(m, a.peak_qps), m, a.md))


if __name__ == "__main__":
    main()
