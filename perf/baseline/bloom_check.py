#!/usr/bin/env python3
"""
布隆过滤器检查：存在的店铺是否全部放行、不存在的 id 误判率是多少。

  python perf/baseline/bloom_check.py --shops 100000                  # 默认访问 http://localhost:8081
  python perf/baseline/bloom_check.py --shops 100000 --samples 20000   # 抽样（存在 / 不存在各 2 万个）

做法：
  1. 请求 /shop/{id}（id 取 1..--shops，或其中的随机样本），统计返回「不存在」的个数，应为 0；
  2. 请求同样数量的不存在的 id（大于 --shops 的随机数），然后到 Redis 里数这些 id 的空值缓存：
     被布隆过滤器拦下的请求不会碰缓存，放行的请求查库落空后会写一条空值 cache:shop:{id}，所以
     误判率 = 留下空值缓存的 id 数 ÷ 请求数。
前提：应用开着缓存（HMDP_CACHE_ENABLED=true）；Redis 通过 docker exec 访问。
空值缓存约 2 分钟过期，所以两次检查之间至少隔 3 分钟，或换一个 --seed。
"""
import argparse
import http.client
import random
import subprocess
import sys
import threading
from urllib.parse import urlparse


def count_null_cache(container, ids):
    """这些 id 里有多少个留下了空值缓存 cache:shop:{id}。"""
    cmds = "".join(f"EXISTS cache:shop:{i}\n" for i in ids)
    out = subprocess.run(["docker", "exec", "-i", container, "redis-cli"],
                         input=cmds, capture_output=True, text=True, check=True).stdout
    return sum(1 for line in out.split() if line == "1")


def fetch(base, ids, threads):
    """并发请求 /shop/{id}，返回 (成功数, 「不存在」数, 其他失败数)。"""
    u = urlparse(base)
    counts = [0, 0, 0]
    lock = threading.Lock()
    chunks = [ids[i::threads] for i in range(threads)]

    def worker(chunk):
        ok = missing = other = 0
        conn = http.client.HTTPConnection(u.hostname, u.port or 80, timeout=10)
        for shop_id in chunk:
            try:
                conn.request("GET", f"{u.path.rstrip('/')}/shop/{shop_id}")
                resp = conn.getresponse()
                body = resp.read()
                if resp.status == 200 and b'"success":true' in body:
                    ok += 1
                elif resp.status == 200:
                    missing += 1
                else:
                    other += 1
            except Exception:
                other += 1
                conn.close()
                conn = http.client.HTTPConnection(u.hostname, u.port or 80, timeout=10)
        with lock:
            counts[0] += ok
            counts[1] += missing
            counts[2] += other

    ts = [threading.Thread(target=worker, args=(c,)) for c in chunks if c]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    return counts


def main():
    ap = argparse.ArgumentParser(description="布隆过滤器检查：存在的 id 全部放行，不存在的 id 误判率")
    ap.add_argument("--base", default="http://localhost:8081", help="后端地址")
    ap.add_argument("--shops", type=int, required=True, help="店铺数（tb_shop 的最大 id，id 连续）")
    ap.add_argument("--samples", type=int, default=None, help="存在 / 不存在各测多少个，默认等于 --shops")
    ap.add_argument("--threads", type=int, default=32)
    ap.add_argument("--redis-container", default="hmdp-redis")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--max-fpp", type=float, default=0.02, help="误判率超过该值时退出码非 0")
    a = ap.parse_args()

    rnd = random.Random(a.seed)
    n = min(a.samples or a.shops, a.shops)
    existing = list(range(1, a.shops + 1)) if n == a.shops else rnd.sample(range(1, a.shops + 1), n)
    absent = [rnd.randrange(a.shops + 1, 2 ** 40) for _ in range(n)]

    ok, missing, other = fetch(a.base, existing, a.threads)
    print(f"存在的 id：{n} 个，放行 {ok}，被判为不存在 {missing}，请求失败 {other}")

    ok2, missing2, other2 = fetch(a.base, absent, a.threads)
    passed = count_null_cache(a.redis_container, absent)
    fpp = passed / n
    print(f"不存在的 id：{n} 个，返回不存在 {missing2}，返回成功 {ok2}，请求失败 {other2}")
    print(f"  其中穿过布隆过滤器 {passed} 个，误判率 {fpp:.2%}（上限 {a.max_fpp:.0%}）")

    bad = missing or other or ok2 or other2 or fpp > a.max_fpp
    print("结果：" + ("不通过" if bad else "通过"))
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
