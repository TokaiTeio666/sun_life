# -*- coding: utf-8 -*-
"""
生成 N 个模拟登录 token，与项目 hmdp 登录逻辑保持一致：
  1. 从 tb_user 表按 id 升序取 N 个用户
  2. 为每个用户生成 UUID token（模拟 UserServiceImpl.login 的 token 生成）
  3. 写入 Redis 6379：key = login:token:{uuid}，Hash = {id, nickName, icon}
     TTL 默认 36000 分钟（25 天，与 RedisConstants.LOGIN_USER_TTL 一致，每次请求还会自动续期）
  4. 输出 token.txt：每行 "token,userId"（逗号分隔，无表头），供 JMeter CSV Data Set Config 使用

依赖：pip install "psycopg[binary]" redis（建议装进 .venv-token）
数据库连接可用环境变量覆盖：PG_HOST / PG_PORT（默认 127.0.0.1:5433）/ PG_USER / PG_PASSWORD / PG_DB

用法（在项目根目录执行）：
  .venv-token\\Scripts\\python gen_tokens.py                  # 默认 1000 个
  .venv-token\\Scripts\\python gen_tokens.py --count 500     # 指定数量
  .venv-token\\Scripts\\python gen_tokens.py --ttl -1        # 永不过期
  .venv-token\\Scripts\\python gen_tokens.py --flush         # 先清空已有 login:token:* 再生成
"""
import argparse
import os
import uuid

import psycopg
import redis

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 默认连 compose 暴露的 PostgreSQL（宿主机端口 5433）；可用环境变量覆盖
DB = dict(host=os.environ.get('PG_HOST', '127.0.0.1'), port=int(os.environ.get('PG_PORT', '5433')),
          user=os.environ.get('PG_USER', 'postgres'), password=os.environ.get('PG_PASSWORD', '123456'),
          dbname=os.environ.get('PG_DB', 'hmdp'))
REDIS = dict(host='127.0.0.1', port=6379, db=0, protocol=2)
LOGIN_USER_KEY = "login:token:"
LOGIN_USER_TTL = 36000  # 分钟，与项目 RedisConstants.LOGIN_USER_TTL 一致


def main():
    ap = argparse.ArgumentParser(description='生成模拟登录 token 并写入 Redis')
    ap.add_argument('--count', type=int, default=1000, help='生成 token 数量（默认1000）')
    ap.add_argument('--ttl', type=int, default=LOGIN_USER_TTL,
                    help='TTL 分钟数，-1 表示永不过期（默认36000，与项目一致）')
    ap.add_argument('--flush', action='store_true', help='先清空已有 login:token:* 再生成')
    args = ap.parse_args()

    # 1. 从数据库取用户
    with psycopg.connect(**DB) as conn, conn.cursor() as cur:
        cur.execute("SELECT id, nick_name, icon FROM tb_user ORDER BY id LIMIT %s", (args.count,))
        users = cur.fetchall()
    if not users:
        print("[ERROR] tb_user 表为空，无法生成 token")
        return
    if len(users) < args.count:
        print(f"[WARN] tb_user 表仅有 {len(users)} 个用户，实际按 {len(users)} 生成")

    # 2. 连接 Redis
    r = redis.Redis(**REDIS, decode_responses=True)
    r.ping()

    # 3. 可选：清空旧 token
    if args.flush:
        n = 0
        for k in r.scan_iter(match=LOGIN_USER_KEY + "*", count=1000):
            r.delete(k)
            n += 1
        print(f"[INFO] 已清空 {n} 个旧 login:token:*")

    # 4. 生成 token 并写入 Redis（pipeline 批量）
    lines = []
    pipe = r.pipeline(transaction=False)
    cnt = 0
    for uid, nick, icon in users:
        token = str(uuid.uuid4())
        key = LOGIN_USER_KEY + token
        mapping = {"id": str(uid), "nickName": nick or "", "icon": icon or ""}
        for field, value in mapping.items():  # 老版本 Redis(3.x) 不支持 HSET 多字段语法，逐字段写入
            pipe.hset(key, field, value)
        if args.ttl and args.ttl > 0:
            pipe.expire(key, args.ttl * 60)
        lines.append(f"{token},{uid}")
        cnt += 1
        if cnt % 200 == 0:  # 每 200 条提交一次，避免单管道过大
            pipe.execute()
            pipe = r.pipeline(transaction=False)
    pipe.execute()

    # 5. 输出 token.txt 到项目根目录
    out_path = os.path.join(BASE_DIR, "token.txt")
    with open(out_path, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines) + "\n")

    print(f"[OK] 已生成 {cnt} 个 token")
    print(f"[OK] 已写入 Redis 127.0.0.1:6379，key 前缀: {LOGIN_USER_KEY}，TTL: {args.ttl} 分钟")
    print(f"[OK] 已输出: {out_path}")


if __name__ == "__main__":
    main()
