#!/usr/bin/env python3
"""
容量基线压测驱动（阶段 3-1.2 第 2–4 步）。

  knee   单实例逐级加并发找拐点：P99 超过 SLO 或错误率超标的前一级即拐点，安全容量取拐点 QPS 的 60–70%
  scale  1 / 2 / 4 / 8 副本各压一轮，计算扩展效率 = N 副本 QPS ÷ (N × 单副本 QPS)；低于 0.7 说明共享组件到顶
  report 重新汇总已有结果目录（不压测）

混合场景（第 4 步）= 用 perf/mixed-bench.jmx 跑 knee / scale。

示例：
  # 单实例拐点（先 docker compose ... up -d --scale app=1）
  python perf/baseline/baseline.py knee --jmx perf/shop-detail-bench.jmx --threads 50,100,200,400,800

  # 扩展效率：脚本自动 --scale app=N 并重启网关
  python perf/baseline/baseline.py scale --jmx perf/mixed-bench.jmx --threads 800 --replicas 1,2,4,8 \
      --token-file token.txt --compose-files deploy/docker-compose.yml,deploy/docker-compose.monitoring.yml

  # 压测机和被测服务分开部署时：只在压测机跑 jmeter，副本数在被测机手动调整
  python perf/baseline/baseline.py scale --host 10.0.0.5 --manual-scale ...

只依赖 Python 3.8+ 标准库；需要 jmeter 在 PATH 中（或用 --jmeter 指定）。
"""
from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RESULTS = ROOT / "perf" / "baseline" / "results"


# ---------------------------------------------------------------- JMeter

def run_jmeter(args, jmx: Path, threads: int, out_dir: Path, extra_props: dict) -> dict:
    """跑一轮 JMeter，返回 statistics.json 中 Total 一行的关键字段。"""
    jtl = out_dir / "run.jtl"
    report = out_dir / "report"
    out_dir.mkdir(parents=True, exist_ok=True)
    props = {
        "THREADS": threads,
        "DURATION": args.duration,
        "SERVER_HOST": args.host,
        "SERVER_PORT": args.port,
        **({"TOKEN_FILE": str(Path(args.token_file).resolve())} if args.token_file else {}),
        **extra_props,
    }
    cmd = [args.jmeter, "-n", "-t", str(jmx.resolve()), "-l", str(jtl), "-e", "-o", str(report), "-j", str(out_dir / "jmeter.log")]
    cmd += [f"-J{k}={v}" for k, v in props.items()]
    print(f"    $ {' '.join(shlex.quote(c) for c in cmd)}", flush=True)
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, shell=(os.name == "nt"))
    return parse_statistics(report / "statistics.json")


def parse_statistics(path: Path) -> dict:
    stats = json.loads(path.read_text(encoding="utf-8"))
    total = stats["Total"]
    per_api = {k: v for k, v in stats.items() if k != "Total"}
    return {
        "qps": total["throughput"],
        "samples": total["sampleCount"],
        "mean_ms": total["meanResTime"],
        "p90_ms": total["pct1ResTime"],
        "p95_ms": total["pct2ResTime"],
        "p99_ms": total["pct3ResTime"],
        "error_ratio": total["errorPct"] / 100.0,
        "per_api": {k: {"qps": v["throughput"], "p99_ms": v["pct3ResTime"], "error_ratio": v["errorPct"] / 100.0}
                    for k, v in per_api.items()},
    }


def run_hook(cmd: str | None):
    if cmd:
        print(f"    hook: {cmd}", flush=True)
        subprocess.run(cmd, shell=True, check=True, cwd=ROOT)


def jmeter_props(args) -> dict:
    props = {}
    for kv in args.prop or []:
        k, _, v = kv.partition("=")
        props[k] = v
    return props


# ---------------------------------------------------------------- docker compose

def compose(args, *extra) -> list[str]:
    cmd = ["docker", "compose"]
    for f in args.compose_files.split(","):
        cmd += ["-f", str(ROOT / f.strip())]
    return cmd + list(extra)


def scale_app(args, n: int):
    print(f"  -> 调整 app 副本数为 {n}", flush=True)
    subprocess.run(compose(args, "up", "-d", "--no-build", "--no-recreate", "--scale", f"app={n}"), check=True, cwd=ROOT)
    deadline = time.time() + args.ready_timeout
    while time.time() < deadline:
        out = subprocess.run(compose(args, "ps", "app", "--format", "{{.Health}}"),
                             capture_output=True, text=True, cwd=ROOT).stdout.split()
        if len(out) == n and all(h == "healthy" for h in out):
            break
        time.sleep(3)
    else:
        raise SystemExit(f"等待 {n} 个 app 副本 healthy 超时")
    # nginx 只在启动时解析 app 的副本 IP，扩缩容后必须重启网关
    subprocess.run(compose(args, "restart", "frontend"), check=True, cwd=ROOT)
    time.sleep(5)


# ---------------------------------------------------------------- 判定与报告

def verdict(r: dict, args) -> str:
    if r["p99_ms"] > args.slo_p99_ms:
        return f"P99 {r['p99_ms']:.0f}ms > SLO {args.slo_p99_ms:.0f}ms"
    if r["error_ratio"] > args.max_error:
        return f"错误率 {r['error_ratio']:.2%} > {args.max_error:.2%}"
    return "ok"


def fmt_row(*cells) -> str:
    return "| " + " | ".join(str(c) for c in cells) + " |"


def knee_report(rows: list[dict], args) -> str:
    lines = [
        f"# 单实例拐点：{Path(args.jmx).name}",
        "",
        f"- 时间：{datetime.now():%Y-%m-%d %H:%M}；每级 {args.duration}s；目标 {args.host}:{args.port}",
        f"- 判定：P99 ≤ {args.slo_p99_ms:.0f}ms 且错误率 ≤ {args.max_error:.2%}；"
        f"另外 QPS 比上一级增长 < {args.flat_gain:.0%} 视为已饱和（并发增加只在排队）",
        "",
        fmt_row("并发", "QPS", "平均 RT ms", "P95 ms", "P99 ms", "错误率", "QPS 增幅", "判定"),
        fmt_row("---:", "---:", "---:", "---:", "---:", "---:", "---:", "---"),
    ]
    knee = None
    passed = True   # 第一次不达标之后的级别只展示，不再参与拐点判定
    prev_qps = None
    for r in rows:
        gain = (r["qps"] / prev_qps - 1) if prev_qps else None
        v = r["verdict"]
        if v == "ok" and gain is not None and gain < args.flat_gain:
            v = "饱和（QPS 不再增长）"
        if v != "ok":
            passed = False
        elif passed:
            knee = r
        lines.append(fmt_row(r["threads"], f"{r['qps']:.0f}", f"{r['mean_ms']:.1f}", f"{r['p95_ms']:.0f}",
                             f"{r['p99_ms']:.0f}", f"{r['error_ratio']:.2%}",
                             "—" if gain is None else f"{gain:+.0%}", v))
        prev_qps = r["qps"]
    lines.append("")
    if knee and passed:
        lines += [
            f"**尚未到拐点**：所有级别都达标，最高为并发 {knee['threads']}、QPS {knee['qps']:.0f}（P99 {knee['p99_ms']:.0f}ms）。",
            "继续加大 --threads 直到出现超 SLO / 错误率超标 / QPS 不再增长的级别。",
        ]
    elif knee:
        lines += [
            f"**拐点**：并发 {knee['threads']}，QPS **{knee['qps']:.0f}**（P99 {knee['p99_ms']:.0f}ms）。",
            f"**安全容量**（拐点 × 60–70%）：**{knee['qps'] * 0.6:.0f} – {knee['qps'] * 0.7:.0f} QPS**。",
            "",
            "拐点 QPS 填入 Grafana 变量 `single_qps`；安全容量填入容量模型 JSON 的 `capacity.app_safe_qps`。",
        ]
    else:
        lines.append("第一级就未达标：降低起始并发重新测。")
    lines += ["", "按 Little 定律自检：QPS ≈ 并发 ÷ 平均 RT。实测 QPS 远低于该值说明压测机本身成了瓶颈。"]
    return "\n".join(lines) + "\n"


def scale_report(rows: list[dict], args) -> str:
    base = rows[0]["qps"] / rows[0]["replicas"]
    lines = [
        f"# 扩展效率：{Path(args.jmx).name}",
        "",
        f"- 时间：{datetime.now():%Y-%m-%d %H:%M}；并发 {args.threads}；每轮 {args.duration}s",
        f"- 扩展效率 = N 副本 QPS ÷ (N × 单副本 QPS {base:.0f})；低于 {args.min_efficiency} 的那一级之后，瓶颈在共享组件",
        "",
        fmt_row("副本数", "QPS", "平均 RT ms", "P99 ms", "错误率", "扩展效率", "判定"),
        fmt_row("---:", "---:", "---:", "---:", "---:", "---:", "---"),
    ]
    first_bad = None
    for r in rows:
        eff = r["qps"] / (r["replicas"] * base)
        r["efficiency"] = eff
        flag = "ok" if eff >= args.min_efficiency else "共享组件到顶"
        if flag != "ok" and first_bad is None:
            first_bad = r
        lines.append(fmt_row(r["replicas"], f"{r['qps']:.0f}", f"{r['mean_ms']:.1f}", f"{r['p99_ms']:.0f}",
                             f"{r['error_ratio']:.2%}", f"{eff:.2f}", flag))
    lines.append("")
    if first_bad:
        lines += [
            f"**{first_bad['replicas']} 副本时效率降到 {first_bad['efficiency']:.2f}**。对照同一时间段的 Grafana：",
            "Redis 主线程 CPU 接近 100%、PostgreSQL 锁等待会话 / 连接数、Kafka lag、Hikari pending，",
            "先饱和的那个组件就是 3-2 的第一优先级。",
        ]
    else:
        lines.append("所有副本数下效率都达标：当前副本数范围内共享组件还没有到顶。")
    lines += ["", "注意：压测机必须与被测服务分开部署，否则副本越多压测机越抢不到 CPU，效率被低估。"]
    return "\n".join(lines) + "\n"


def api_breakdown(rows: list[dict], key: str) -> str:
    lines = ["", "## 分接口（每级）", ""]
    for r in rows:
        lines.append(f"**{key} = {r[key]}**")
        lines.append("")
        lines.append(fmt_row("接口", "QPS", "P99 ms", "错误率"))
        lines.append(fmt_row("---", "---:", "---:", "---:"))
        for name, a in sorted(r["per_api"].items(), key=lambda kv: -kv[1]["qps"]):
            lines.append(fmt_row(name, f"{a['qps']:.0f}", f"{a['p99_ms']:.0f}", f"{a['error_ratio']:.2%}"))
        lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------- 子命令

def cmd_knee(args):
    out = Path(args.out or RESULTS / f"{datetime.now():%Y%m%d-%H%M%S}-knee-{Path(args.jmx).stem}")
    rows = []
    for t in [int(x) for x in args.threads.split(",")]:
        print(f"  -> 并发 {t}", flush=True)
        run_hook(args.before_each)
        r = run_jmeter(args, Path(args.jmx), t, out / f"threads-{t}", jmeter_props(args))
        r.update(threads=t, verdict=verdict(r, args))
        rows.append(r)
        print(f"     QPS {r['qps']:.0f}  P99 {r['p99_ms']:.0f}ms  err {r['error_ratio']:.2%}  {r['verdict']}", flush=True)
        if r["verdict"] != "ok" and not args.keep_going:
            break
        time.sleep(args.cooldown)
    save(out, "knee", rows, knee_report(rows, args) + api_breakdown(rows, "threads"), args)


def cmd_scale(args):
    out = Path(args.out or RESULTS / f"{datetime.now():%Y%m%d-%H%M%S}-scale-{Path(args.jmx).stem}")
    rows = []
    for n in [int(x) for x in args.replicas.split(",")]:
        if args.manual_scale:
            input(f"  -> 请把被测环境调整为 {n} 个 app 副本并重启网关，完成后回车 ...")
        else:
            scale_app(args, n)
        run_hook(args.before_each)
        # 预热：JIT、连接池、本地缓存
        run_jmeter(args, Path(args.jmx), min(args.threads, 50), out / f"replicas-{n}-warmup",
                   {**jmeter_props(args), "DURATION": args.warmup})
        r = run_jmeter(args, Path(args.jmx), args.threads, out / f"replicas-{n}", jmeter_props(args))
        r.update(replicas=n, threads=args.threads, verdict=verdict(r, args))
        rows.append(r)
        print(f"     {n} 副本：QPS {r['qps']:.0f}  P99 {r['p99_ms']:.0f}ms  err {r['error_ratio']:.2%}", flush=True)
        time.sleep(args.cooldown)
    save(out, "scale", rows, scale_report(rows, args) + api_breakdown(rows, "replicas"), args)


def cmd_report(args):
    out = Path(args.dir)
    data = json.loads((out / "results.json").read_text(encoding="utf-8"))
    ns = argparse.Namespace(**{**vars(args), **data["args"]})
    rows = data["rows"]
    if data["kind"] == "knee":
        text = knee_report(rows, ns) + api_breakdown(rows, "threads")
    else:
        text = scale_report(rows, ns) + api_breakdown(rows, "replicas")
    (out / "summary.md").write_text(text, encoding="utf-8")
    print(text)


def save(out: Path, kind: str, rows: list[dict], text: str, args):
    out.mkdir(parents=True, exist_ok=True)
    keep = {k: v for k, v in vars(args).items() if k != "func"}
    (out / "results.json").write_text(json.dumps({"kind": kind, "args": keep, "rows": rows}, ensure_ascii=False, indent=2),
                                      encoding="utf-8")
    (out / "summary.md").write_text(text, encoding="utf-8")
    print("\n" + text)
    print(f"结果目录：{out}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(sp):
        sp.add_argument("--jmx", required=True, help="JMeter 脚本，如 perf/mixed-bench.jmx")
        sp.add_argument("--host", default="127.0.0.1", help="网关地址（压测机与被测服务分开部署时填被测机 IP）")
        sp.add_argument("--port", default="8081")
        sp.add_argument("--duration", type=int, default=60, help="每级持续秒数")
        sp.add_argument("--token-file", help="gen_tokens 生成的 token.txt（秒杀 / 混合场景需要）")
        sp.add_argument("--jmeter", default="jmeter")
        sp.add_argument("--prop", action="append", help="额外 JMeter 属性，如 --prop PCT_SECKILL=10")
        sp.add_argument("--slo-p99-ms", type=float, default=100.0)
        sp.add_argument("--max-error", type=float, default=0.001, help="错误率上限，默认 0.1%%")
        sp.add_argument("--before-each", help="每轮压测前执行的命令，如重置秒杀库存")
        sp.add_argument("--cooldown", type=int, default=10, help="两轮之间的间隔秒数")
        sp.add_argument("--out", help="结果目录，默认 perf/baseline/results/<时间>-<类型>-<脚本名>")

    k = sub.add_parser("knee", help="单实例逐级加并发找拐点")
    common(k)
    k.add_argument("--threads", default="50,100,200,400,800,1200")
    k.add_argument("--flat-gain", type=float, default=0.05, help="QPS 增幅低于该值视为饱和")
    k.add_argument("--keep-going", action="store_true", help="超过 SLO 后继续跑完剩余级别")
    k.set_defaults(func=cmd_knee)

    s = sub.add_parser("scale", help="多副本扩展效率")
    common(s)
    s.add_argument("--threads", type=int, required=True, help="固定并发，取能压满最大副本数的值")
    s.add_argument("--replicas", default="1,2,4,8")
    s.add_argument("--min-efficiency", type=float, default=0.7)
    s.add_argument("--warmup", type=int, default=15, help="每轮正式压测前的预热秒数")
    s.add_argument("--compose-files", default="deploy/docker-compose.yml,deploy/docker-compose.monitoring.yml")
    s.add_argument("--ready-timeout", type=int, default=300)
    s.add_argument("--manual-scale", action="store_true", help="不自动扩缩容，每轮前提示手动调整")
    s.set_defaults(func=cmd_scale)

    r = sub.add_parser("report", help="重新汇总已有结果目录")
    r.add_argument("dir")
    r.set_defaults(func=cmd_report)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    sys.exit(main())
