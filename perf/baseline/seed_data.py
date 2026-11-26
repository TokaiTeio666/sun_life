#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
压测造数：把数据量灌到接近上线的规模（默认 100 万用户 / 10 万店铺 / 1000 万订单）。

课程自带的 hmdp.sql 只有十几家店、一千个用户，全部数据都在内存里，索引只有一两层，
在这上面测出的读性能和秒杀落库性能都偏乐观。容量基线（阶段 3-1）要在灌好数据的库上测。

用法（在项目根目录；被测环境已用 docker compose 启动）：
  python perf/baseline/seed_data.py                                  # 默认规模
  python perf/baseline/seed_data.py --users 100000 --shops 10000 --orders 1000000   # 1/10 规模试跑
  python perf/baseline/seed_data.py --steps order                    # 只跑某几步（逗号分隔）
  python perf/baseline/seed_data.py --shared-buffers-gb 4            # 把 shared_buffers 调到 4G（需重启 postgres，见下）
  python perf/baseline/seed_data.py --steps tokens --tokens 2000     # 只生成登录 token（写 Redis + token.txt）

数据全部由 PostgreSQL 服务端用 INSERT ... SELECT ... generate_series(...) 生成，不经过网络传输；客户端以一次性
容器运行（docker run --rm，用 postgres:16.4 镜像里的 psql），连接方式与 component-bench.sh 相同，环境变量也通用：
  NET=host PG_HOST=10.0.0.5 PG_PORT=5433 REDIS_HOST=10.0.0.6 REDIS_PORT=6380 \
      python perf/baseline/seed_data.py

可重复执行、可中断续跑：每一步的进度记在 seed_progress 表里，和该批数据在同一个事务提交。
把目标调大后重跑只会补差额；目标比现有数据小则什么都不做（不会删数据）。
注意：订单的时间和 id 是按 --orders 总数均匀铺开的。调大 --orders 后补的那部分订单会重新铺满整个时间段，
id 插在已有订单之间（不冲突，但不再是主键顺序写入，时间分布也和一次灌满的不同）。
要和别的基线对比，就在空库上一次灌到目标规模。

生成的数据：
  tb_user / tb_user_info   id 接着现有最大值往后排到 --users；手机号 19 开头，不与现有数据冲突
  tb_shop                  id 连续排到 --shops（JMeter 用 -JSHOP_MAX=<店铺数> 随机访问）；坐标落在杭州范围
  tb_voucher               id 从 1001 起（避开压测券 21 和 999999），每家店 --vouchers ÷ --shops 张；
                           其中 1/10 是秒杀券，活动都已结束（不会被应用启动时预热进 Redis）
  tb_blog                  作者集中在前 10% 的用户（创作者），点赞数长尾分布
  tb_follow                每个用户关注 --follows-per-user 个创作者，头部创作者粉丝数可达数万（大 V）；
                           tb_user_info 的 fans / followee 按关注关系回填
  tb_voucher_order         均匀分布在最近 --order-days 天；状态只有 已支付 / 已核销 / 已取消 / 已退款，
                           没有「未支付」——否则超时关单任务会把它们全部关掉并回补库存
  Redis shop:geo:{typeId}  店铺坐标（附近商铺查询用）
  Redis feed:{userId}      前 --feed-users 个用户的关注收件箱（最近 --feed-days 天的笔记）

灌完之后：
  1. 重启 app（布隆过滤器在启动时预热；不重启的话要等定时增量同步，默认 1 分钟一轮，期间新店铺会被判为「不存在」）；
  2. 重新生成 token：加 --tokens <N>（或单独跑 --steps tokens），PostgreSQL / Redis 不在本机时也能用；
  3. 压测加 --prop SHOP_MAX=<店铺数>。

灌数时会话里设 synchronous_commit = off：提交时不等 WAL 刷盘，批量写入快很多。进度和数据在同一个事务里，
断电最多丢最后几个事务，重跑会从 seed_progress 记录的位置补上，不会出现数据与进度不一致。
（PostgreSQL 没有「关 binlog」这种会话级开关，WAL 必须写；原 --keep-binlog 选项已去掉。）

关于 shared_buffers（MySQL 的 innodb_buffer_pool_size 运行中能改，shared_buffers 不行）：
--shared-buffers-gb 只会执行 ALTER SYSTEM SET shared_buffers（写入 postgresql.auto.conf）然后提示退出；
需要自己重启 postgres（docker restart hmdp-postgres）后再重新运行本脚本。
若 compose 里用 command: -c shared_buffers=... 启动，命令行参数优先于 auto.conf，要改 compose 文件。
（旧名 --buffer-pool-gb 仍可用，是同一个选项。）

需要 Python 3.12+（只用标准库）和 docker。
"""
import argparse
import os
import re
import subprocess
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

VOUCHER_BASE = 1000          # 造数的券 id 从 VOUCHER_BASE + 1 开始
VOUCHER_LIMIT = 999998       # 999999 是 component-bench.sh 的压测券
ORDER_SEED_FLAG = 0x80000000  # 造数订单 id 的序列号最高位置 1，与 RedisIdWorker 生成的 id 永不冲突
ALL_STEPS = ["user", "user_info", "shop", "voucher", "blog", "follow", "order", "redis-geo", "redis-feed"]

# 造数与基线依赖的二级索引（db/hmdp.sql）。手工建的库可能缺索引：
# 缺 idx_* 时读 SQL 会退化成全表扫描，缺 uk_user_follow 时关注步骤的 ON CONFLICT DO NOTHING 不会去重。
# 只检查本次要跑的步骤对应的索引。
REQUIRED_INDEXES = [
    ("tb_blog", "idx_liked", "blog"),
    ("tb_blog", "idx_user_id", "blog"),
    ("tb_follow", "uk_user_follow", "follow"),
    ("tb_follow", "idx_follow_user", "follow"),
    ("tb_voucher_order", "idx_voucher_order_unpaid_ctime", "order"),
    ("tb_voucher_order", "idx_user_voucher", "order"),
    ("tb_voucher", "idx_shop_id", "voucher"),
]


def env(name, default):
    return os.environ.get(name, default)


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def die(msg):
    print(f"[ERROR] {msg}", file=sys.stderr, flush=True)
    sys.exit(1)


class Seeder:
    def __init__(self, args):
        self.a = args
        self.now = int(time.time())
        # 客户端编码与时区：timestamp 列不带时区，统一按东八区写入，与应用的 LocalDateTime 一致
        self.base = "SET client_encoding TO 'UTF8';\nSET timezone TO 'Asia/Shanghai';\n"
        # 灌数不等 WAL 刷盘：省掉每个事务提交时的 fsync（进度与数据同事务，见文件头说明）
        self.prefix = self.base + "SET synchronous_commit = off;\n"

    # ---------- 客户端 ----------

    def psql(self, sql, with_prefix=True):
        a = self.a
        # -A -t -F：无对齐、无表头、制表符分隔，便于下面按行 / 按制表符解析
        cmd = ["docker", "run", "--rm", "-i", "--network", a.net, "-e", f"PGPASSWORD={a.pg_password}",
               a.pg_image, "psql", "-v", "ON_ERROR_STOP=1", "-X", "-q", "-A", "-t", "-F", "\t",
               "-h", a.pg_host, "-p", str(a.pg_port), "-U", a.pg_user, "-d", a.pg_db, "-f", "-"]
        body = (self.prefix if with_prefix else self.base) + sql
        r = subprocess.run(cmd, input=body.encode("utf-8"), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if r.returncode != 0:
            err = r.stderr.decode("utf-8", "replace").strip()
            die(f"PostgreSQL 执行失败：{err}\n--- SQL（前 600 字符）---\n{sql[:600]}")
        return r.stdout.decode("utf-8", "replace")

    def scalar(self, sql):
        out = self.psql(sql, with_prefix=False).strip()
        return out.splitlines()[-1] if out else ""

    def redis_pipe(self, payload):
        """payload 是 RESP 协议字节串，经 redis-cli --pipe 批量写入；返回回复数。"""
        if not payload:
            return 0
        a = self.a
        cmd = ["docker", "run", "--rm", "-i", "--network", a.net]
        if a.redis_password:
            cmd += ["-e", f"REDISCLI_AUTH={a.redis_password}"]
        cmd += [a.redis_image, "redis-cli", "-h", a.redis_host, "-p", str(a.redis_port), "--pipe"]
        r = subprocess.run(cmd, input=payload, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out = r.stdout.decode("utf-8", "replace")
        m = re.search(r"errors:\s*(\d+),\s*replies:\s*(\d+)", out)
        if r.returncode != 0 or not m or int(m.group(1)) != 0:
            die(f"Redis 批量写入失败：{out.strip()} {r.stderr.decode('utf-8', 'replace').strip()}")
        return int(m.group(2))

    # ---------- 进度与分批 ----------

    def prepare(self):
        self.psql(
            "CREATE TABLE IF NOT EXISTS seed_progress ("
            " step varchar(32) PRIMARY KEY, done bigint NOT NULL,"
            " update_time timestamp NOT NULL DEFAULT now());\n"
            "COMMENT ON TABLE seed_progress IS 'seed_data.py 的进度，可删除（删除后各步从现有最大 id 续跑）';\n")

    def progress(self, step, init_sql):
        """读取某一步的进度；第一次运行时用 init_sql 的结果初始化。"""
        self.psql(f"INSERT INTO seed_progress (step, done) SELECT '{step}', ({init_sql}) ON CONFLICT (step) DO NOTHING;\n")
        return int(self.scalar(f"SELECT done FROM seed_progress WHERE step = '{step}'"))

    def fix_identity(self, table):
        """批量写入了显式 id，把 IDENTITY 序列推到当前最大 id 之后，否则应用自增插入会撞主键。"""
        self.psql(f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), COALESCE((SELECT max(id) FROM {table}), 0) + 1, false);\n",
                  with_prefix=False)

    def run_step(self, step, label, start, target, make_sql):
        """从 start 做到 target：make_sql(done, cnt) 生成「在 done 之后再做 cnt 个」的 SQL。"""
        total = target - start
        if total <= 0:
            log(f"{label}：已有 {start:,}，目标 {target:,}，跳过")
            return
        log(f"{label}：{start:,} → {target:,}（{total:,}）")
        done, t0, last = start, time.time(), 0.0
        while done < target:
            cnt = min(self.a.chunk, target - done)
            self.psql("BEGIN;\n" + make_sql(done, cnt)
                      + f"UPDATE seed_progress SET done = {done + cnt}, update_time = now() WHERE step = '{step}';\nCOMMIT;\n")
            done += cnt
            el = time.time() - t0
            if el - last >= 5 or done >= target:
                last = el
                rate = (done - start) / el if el > 0 else 0
                eta = (target - done) / rate if rate > 0 else 0
                log(f"  {label} {done - start:,}/{total:,}  {rate:,.0f}/s  剩余约 {eta:,.0f}s")

    @staticmethod
    def series(cnt):
        """生成 n = 0 .. cnt-1 的行（代替 MySQL 版的 seed_seq 序列表）。"""
        return f"generate_series(0, {cnt - 1}) AS g(n)"

    @staticmethod
    def pick(options):
        """从常量里随机取一个（代替 MySQL 的 ELT(1 + FLOOR(RAND() * N), ...)）。"""
        return f"(ARRAY[{', '.join(options)}])[1 + floor(random() * {len(options)})::int]"

    # ---------- PostgreSQL 各步 ----------

    def seed_user(self):
        start = self.progress("user", "SELECT COALESCE(MAX(id), 0) FROM tb_user")
        now = self.now

        def sql(done, cnt):
            b = done + 1
            uid = f"({b}::bigint + n)"
            return (
                "INSERT INTO tb_user (id, phone, password, nick_name, icon, create_time)\n"
                f"SELECT {uid}, '19' || lpad({uid}::text, 9, '0'), '', 'user_' || {uid}, '',\n"
                f"       to_timestamp({now} - floor(random() * 63072000))::timestamp\n"
                f"FROM {self.series(cnt)};\n")
        self.run_step("user", "用户 tb_user", start, self.a.users, sql)
        self.fix_identity("tb_user")

    def seed_user_info(self):
        start = self.progress("user_info", "SELECT 0")
        cities = ["'杭州'", "'上海'", "'北京'", "'广州'", "'深圳'", "'成都'", "'南京'", "'武汉'"]

        def sql(done, cnt):
            b = done + 1
            return (
                "INSERT INTO tb_user_info (user_id, city, introduce, fans, followee, gender, birthday, credits, level)\n"
                f"SELECT {b}::bigint + n, {self.pick(cities)},\n"
                "       '这个人很懒，什么都没有留下', 0, 0, floor(random() * 2)::int,\n"
                "       DATE '2005-01-01' - floor(random() * 10000)::int, floor(random() * 5000)::int, floor(random() * 10)::int\n"
                f"FROM {self.series(cnt)}\n"
                "ON CONFLICT (user_id) DO NOTHING;\n")
        self.run_step("user_info", "用户资料 tb_user_info", start, self.a.users, sql)

    def seed_shop(self):
        start = self.progress("shop", "SELECT COALESCE(MAX(id), 0) FROM tb_shop")
        names = ["'老街火锅'", "'江南小馆'", "'川味坊'", "'海鲜大排档'", "'日式料理'",
                 "'韩式烤肉'", "'茶餐厅'", "'量贩KTV'", "'美发沙龙'", "'健身工作室'"]
        areas = ["'大关'", "'拱宸桥/上塘'", "'运河上街'", "'北部新城'", "'武林广场'", "'西湖文化广场'", "'钱江新城'", "'滨江'"]
        roads = ["'金华路'", "'丽水路'", "'台州路'", "'湖墅南路'", "'文三路'", "'江南大道'"]

        def sql(done, cnt):
            b = done + 1
            sid = f"({b}::bigint + n)"
            images = " || ',' || ".join(f"'https://img.example.com/msmerchant/' || md5('{k}:' || {sid}) || '.jpg'" for k in (1, 2, 3))
            return (
                "INSERT INTO tb_shop (id, name, type_id, images, area, address, x, y, avg_price, sold, comments, score, open_hours)\n"
                f"SELECT {sid},\n"
                f"       {self.pick(names)} || '(' || {sid} || '号店)',\n"
                # 美食类占四成，其余类型均分：让 shop:geo:1 明显比其他类型大，接近真实分布
                "       CASE WHEN random() < 0.4 THEN 1 ELSE 1 + floor(random() * 10)::int END,\n"
                f"       {images},\n"
                f"       {self.pick(areas)},\n"
                f"       {self.pick(roads)} || (1 + floor(random() * 999)::int) || '号',\n"
                "       round((120.0 + random() * 0.4)::numeric, 6), round((30.1 + random() * 0.3)::numeric, 6),\n"
                "       20 + floor(random() * 300)::int, floor(random() * 30000)::int, floor(random() * 10000)::int, 30 + floor(random() * 21)::int, '10:00-22:00'\n"
                f"FROM {self.series(cnt)};\n")
        self.run_step("shop", "店铺 tb_shop", start, self.a.shops, sql)
        self.fix_identity("tb_shop")

    def seed_voucher(self):
        a = self.a
        start = self.progress(
            "voucher",
            f"SELECT GREATEST({VOUCHER_BASE}, COALESCE(MAX(id), 0)) FROM tb_voucher "
            f"WHERE id BETWEEN {VOUCHER_BASE + 1} AND {VOUCHER_LIMIT}")

        def sql(done, cnt):
            b = done + 1
            vid = f"({b}::bigint + n)"
            # 面值（分）：按券 id 轮流取四档。PG 的 int / int 是整数除法，对应 MySQL 的 DIV
            face = f"(ARRAY[2000, 5000, 10000, 20000])[1 + ({vid} % 4)::int]"
            return (
                "INSERT INTO tb_voucher (id, shop_id, title, sub_title, rules, pay_value, actual_value, type, status)\n"
                f"SELECT {vid}, 1 + (({vid} - {VOUCHER_BASE + 1}) % {a.shops}), ({face} / 100) || '元代金券', '周一至周日均可使用',\n"
                "       E'全场通用\\n无需预约\\n可无限叠加\\n不兑现、不找零\\n仅限堂食',\n"
                f"       {face} * (80 + ({vid} % 16)) / 100, {face}, CASE WHEN {vid} % 10 = 0 THEN 1 ELSE 0 END, 1\n"
                f"FROM {self.series(cnt)};\n"
                # 秒杀券活动全部已结束（至少 3 天前）：SeckillStockInitializer 只预热未过期的券，
                # 否则每个副本启动时要为几万张券逐个访问 Redis
                "INSERT INTO tb_seckill_voucher (voucher_id, stock, begin_time, end_time)\n"
                f"SELECT {vid}, floor(random() * 200)::int, now() - (10 + {vid} % 80) * interval '1 day', now() - (3 + {vid} % 80) * interval '1 day'\n"
                f"FROM {self.series(cnt)} WHERE {vid} % 10 = 0;\n")
        self.run_step("voucher", "优惠券 tb_voucher", start, VOUCHER_BASE + a.vouchers, sql)
        self.fix_identity("tb_voucher")

    def seed_blog(self):
        a = self.a
        start = self.progress("blog", "SELECT COALESCE(MAX(id), 0) FROM tb_blog")
        now, span, creators = self.now, a.order_days * 86400, self.creators
        titles = ["'周末探店好去处'", "'人均50吃到撑'", "'这家店我能吃一辈子'", "'约会圣地推荐'", "'打工人的深夜食堂'", "'宝藏小店分享'"]

        def sql(done, cnt):
            b = done + 1
            bid = f"({b}::bigint + n)"
            return (
                "INSERT INTO tb_blog (id, shop_id, user_id, title, images, content, liked, comments, create_time)\n"
                f"SELECT {bid}, 1 + floor(random() * {a.shops})::bigint, 1 + floor({creators} * power(random(), 2))::bigint,\n"
                f"       {self.pick(titles)} || ' #' || {bid},\n"
                f"       '/imgs/blogs/' || ({bid} % 16) || '/' || ({bid} % 7) || '/' || md5({bid}::text) || '.jpg',\n"
                "       repeat('环境很好，服务热情，菜品味道不错，分量也足，性价比很高，推荐大家周末来打卡。', 6),\n"
                # 点赞数长尾：P(点赞 ≥ x) = 1/(x+1)，约九成笔记是个位数，万分之一上万（热门榜 ORDER BY liked DESC 走 idx_liked）
                # PG 的 random() 取值 [0, 1)，1 - random() 不会为 0
                "       LEAST(100000, floor(1 / (1 - random())) - 1)::int, floor(random() * 500)::int,\n"
                f"       to_timestamp({now} - floor(random() * {span}))::timestamp\n"
                f"FROM {self.series(cnt)};\n")
        self.run_step("blog", "笔记 tb_blog", start, a.blogs, sql)
        self.fix_identity("tb_blog")

    def seed_follow(self):
        a = self.a
        start = self.progress("follow", "SELECT 0")
        now, creators = self.now, self.creators

        def sql(done, cnt):
            b = done + 1
            # 关注对象按三次方偏向头部创作者：1 号创作者拿到约 (1/创作者数)^(1/3) 的关注（10 万创作者时约 2%）。
            # 同一用户多次抽到同一人由 uk_user_follow + ON CONFLICT DO NOTHING 去重，所以实际行数略少于 用户数 × 关注数
            # 子查询里有 volatile 的 random()，PG 不会把它展开合并，u / f 每行只算一次
            one = (
                "INSERT INTO tb_follow (user_id, follow_user_id, create_time)\n"
                f"SELECT u, f, to_timestamp({now} - floor(random() * 31536000))::timestamp\n"
                f"FROM (SELECT {b}::bigint + n AS u, 1 + floor({creators} * power(random(), 3))::bigint AS f FROM {self.series(cnt)}) t\n"
                "WHERE u <> f\n"
                "ON CONFLICT DO NOTHING;\n")
            return one * a.follows_per_user
        self.run_step("follow", "关注 tb_follow（按用户）", start, a.users, sql)
        if start < a.users:
            # 粉丝数 / 关注数与 tb_follow 对齐（按全表重算，可重复执行）
            log("回填 tb_user_info 的 fans / followee")
            self.psql(
                "UPDATE tb_user_info i SET fans = t.c\n"
                "  FROM (SELECT follow_user_id AS u, count(*) AS c FROM tb_follow GROUP BY follow_user_id) t WHERE t.u = i.user_id;\n"
                "UPDATE tb_user_info i SET followee = t.c\n"
                "  FROM (SELECT user_id AS u, count(*) AS c FROM tb_follow GROUP BY user_id) t WHERE t.u = i.user_id;\n")

    def seed_order(self):
        a = self.a
        begin = self.id_begin_timestamp()
        # 订单时间均匀铺在 [t_start, now)；id 高 32 位与 RedisIdWorker 一样是「相对 BEGIN_TIMESTAMP 的秒数」，
        # 所以 t_start 不能早于 BEGIN_TIMESTAMP。id 随序号单调递增，主键是顺序插入。
        t_start = max(self.now - a.order_days * 86400, begin + 1)
        span = self.now - t_start
        if span <= 0:
            die(f"当前时间早于 RedisIdWorker.BEGIN_TIMESTAMP（{begin}），无法生成订单 id")
        if a.orders >= ORDER_SEED_FLAG:
            die("--orders 不能超过 2^31")
        start = self.progress("order", "SELECT 0")
        now, users, vouchers, total = self.now, a.users, a.vouchers, a.orders
        # 已核销 55% / 已支付 30% / 已取消 10% / 已退款 5%；不生成「未支付」（见文件头说明）
        statuses = ["3"] * 11 + ["2"] * 6 + ["4"] * 2 + ["6"]

        def sql(done, cnt):
            return (
                "INSERT INTO tb_voucher_order (id, user_id, voucher_id, pay_type, status, create_time, pay_time, use_time, refund_time, update_time)\n"
                f"SELECT ((ct - {begin}) << 32) | {ORDER_SEED_FLAG} | i, uid, vid, pt, st, to_timestamp(ct)::timestamp,\n"
                f"       CASE WHEN st IN (2, 3, 6) THEN to_timestamp(LEAST(ct + 30 + floor(r * 600), {now}))::timestamp END,\n"
                f"       CASE WHEN st = 3 THEN to_timestamp(LEAST(ct + 3600 + floor(r * 259200), {now}))::timestamp END,\n"
                f"       CASE WHEN st = 6 THEN to_timestamp(LEAST(ct + 3600 + floor(r * 86400), {now}))::timestamp END,\n"
                "       to_timestamp(ct)::timestamp\n"
                "FROM (\n"
                # i / ct 用 bigint：(序号 × 时间跨度) 会超过 int4。子查询里有 random()，PG 不会展开合并，各列每行只算一次
                f"  SELECT {done}::bigint + n AS i, {t_start}::bigint + ({done}::bigint + n) * {span} / {total} AS ct,\n"
                f"         1 + floor({users} * power(random(), 1.5))::bigint AS uid,\n"
                f"         {VOUCHER_BASE + 1} + floor({vouchers} * power(random(), 2))::bigint AS vid,\n"
                "         1 + floor(random() * 3)::int AS pt,\n"
                f"         {self.pick(statuses)} AS st,\n"
                "         random() AS r\n"
                f"  FROM {self.series(cnt)}\n"
                ") t;\n")
        self.run_step("order", "订单 tb_voucher_order", start, a.orders, sql)

    def id_begin_timestamp(self):
        src = ROOT / "src/main/java/com/hmdp/utils/RedisIdWorker.java"
        m = re.search(r"BEGIN_TIMESTAMP\s*=\s*(\d+)L", src.read_text(encoding="utf-8")) if src.exists() else None
        if not m:
            die(f"读不到 {src} 里的 BEGIN_TIMESTAMP")
        return int(m.group(1))

    # ---------- Redis 各步 ----------

    @staticmethod
    def resp(*parts):
        out = [b"*%d\r\n" % len(parts)]
        for p in parts:
            p = p.encode("utf-8") if isinstance(p, str) else p
            out.append(b"$%d\r\n%s\r\n" % (len(p), p))
        return b"".join(out)

    def seed_redis_geo(self):
        rows = self.psql("SELECT type_id, x, y, id FROM tb_shop;\n", with_prefix=False).splitlines()
        payload = b"".join(self.resp("GEOADD", f"shop:geo:{t}", x, y, i)
                           for t, x, y, i in (r.split("\t") for r in rows if r))
        n = self.redis_pipe(payload)
        log(f"Redis 店铺坐标 shop:geo:*：{n:,} 家")

    def seed_redis_feed(self):
        a = self.a
        users = min(a.feed_users, a.users)
        if users <= 0:
            return
        log(f"Redis 关注收件箱 feed:*：前 {users:,} 个用户，最近 {a.feed_days} 天的笔记")
        step, entries, t0 = 20000, 0, time.time()
        for lo in range(1, users + 1, step):
            hi = min(lo + step - 1, users)
            # timestamp 不带时区：按会话时区（东八区）换算成 epoch，与 MySQL 的 UNIX_TIMESTAMP 一致
            rows = self.psql(
                "SELECT f.user_id, b.id, (extract(epoch FROM b.create_time AT TIME ZONE current_setting('TimeZone')) * 1000)::bigint\n"
                "FROM tb_follow f JOIN tb_blog b ON b.user_id = f.follow_user_id\n"
                f"WHERE f.user_id BETWEEN {lo} AND {hi} AND b.create_time >= now() - interval '{a.feed_days} days';\n",
                with_prefix=False).splitlines()
            buf, uids = [], set()
            for r in rows:
                if not r:
                    continue
                uid, blog_id, score = r.split("\t")
                buf.append(self.resp("ZADD", f"feed:{uid}", score, blog_id))
                uids.add(uid)
            # 与 BlogServiceImpl.saveBlog 一致：每个收件箱只保留最近 1000 条
            buf.extend(self.resp("ZREMRANGEBYRANK", f"feed:{u}", "0", "-1001") for u in uids)
            self.redis_pipe(b"".join(buf))
            entries += len(rows)
            log(f"  收件箱 {hi:,}/{users:,} 用户，累计 {entries:,} 条，{time.time() - t0:,.0f}s")

    def seed_tokens(self):
        """与 gen_tokens.ps1 等价：取前 N 个用户，写 login:token:{uuid}，输出 JMeter 用的 token.txt。"""
        a = self.a
        rows = self.psql(f"SELECT id, nick_name, icon FROM tb_user ORDER BY id LIMIT {a.tokens};\n",
                          with_prefix=False).splitlines()
        if not rows:
            die("tb_user 为空，无法生成 token")
        buf, lines = [], []
        for r in rows:
            uid, nick, icon = (r.split("\t") + ["", ""])[:3]
            token = str(uuid.uuid4())
            key = f"login:token:{token}"
            buf.append(self.resp("HSET", key, "id", uid, "nickName", nick, "icon", icon))
            buf.append(self.resp("EXPIRE", key, "2160000"))   # 25 天，与 LOGIN_USER_TTL 一致
            lines.append(f"{token},{uid}")
        self.redis_pipe(b"".join(buf))
        out = Path(a.token_file).resolve()
        out.write_text("\n".join(lines) + "\n", encoding="ascii", newline="\n")
        log(f"登录 token：{len(lines):,} 个 -> {out}")

    # ---------- 主流程 ----------

    @property
    def creators(self):
        return max(1, self.a.users // 10)

    def check(self):
        a = self.a
        if a.users > 999_999_999:
            die("--users 不能超过 999999999（手机号只有 11 位）")
        if a.vouchers > VOUCHER_LIMIT - VOUCHER_BASE:
            die(f"--vouchers 不能超过 {VOUCHER_LIMIT - VOUCHER_BASE}")
        if "order" in a.steps and "voucher" not in a.steps:
            have = int(self.scalar(f"SELECT COUNT(*) FROM tb_voucher WHERE id BETWEEN {VOUCHER_BASE + 1} AND {VOUCHER_BASE + a.vouchers}") or 0)
            if have < a.vouchers:
                log(f"[WARN] 订单会引用券 {VOUCHER_BASE + 1}–{VOUCHER_BASE + a.vouchers}，目前只有 {have:,} 张（没有外键，不影响写入）")
        needed = [(t, i) for t, i, step in REQUIRED_INDEXES if step in a.steps]
        if needed:
            present = {tuple(line.split("\t")) for line in self.psql(
                "SELECT tablename, indexname FROM pg_indexes WHERE schemaname = current_schema();\n",
                with_prefix=False).splitlines() if line}
            missing = [f"{t}.{i}" for t, i in needed if (t, i) not in present]
            if missing:
                die("缺少索引：" + ", ".join(missing) +
                    "\n  建表脚本 src/main/resources/db/hmdp.sql 里已有这些索引：新建 PG 数据卷会自动执行；\n"
                    "  已有的库请手工执行其中对应的 CREATE INDEX（或 docker compose down -v 清空数据卷后重建）。\n"
                    "  缺少 uk_user_follow 时 ON CONFLICT DO NOTHING 不会去重，关注数据会出现重复行。")

    def shared_buffers(self):
        return int(self.scalar("SELECT pg_size_bytes(current_setting('shared_buffers'))") or 0)

    def tune(self):
        a = self.a
        if a.shared_buffers_gb:
            target = int(a.shared_buffers_gb * 1024 ** 3)
            if abs(self.shared_buffers() - target) > 8 * 1024 ** 2:
                # shared_buffers 只能在启动时设置，ALTER SYSTEM 写入 postgresql.auto.conf，重启后才生效
                self.psql(f"ALTER SYSTEM SET shared_buffers = '{target // 1024 ** 2}MB';\n", with_prefix=False)
                die(f"shared_buffers 已写入 postgresql.auto.conf（{a.shared_buffers_gb:g}G），但要重启 postgres 才生效：\n"
                    "    docker restart hmdp-postgres\n"
                    "  重启后重新运行本命令（已灌的数据会续跑）。若 compose 用 command: -c shared_buffers=... 启动，"
                    "命令行优先于 auto.conf，需要改 compose 文件。")
        pool = self.shared_buffers()
        if "order" in a.steps and a.orders >= 1_000_000 and pool < 1024 ** 3:
            log(f"[WARN] shared_buffers 只有 {pool / 1024 ** 2:,.0f}M：千万级订单的二级索引放不进内存，"
                "灌数会越来越慢，压测也会变成测磁盘。建议加 --shared-buffers-gb（机器内存的 25–40%，剩下的留给 OS 页缓存）。")

    def summary(self):
        tables = ["tb_user", "tb_user_info", "tb_shop", "tb_voucher", "tb_seckill_voucher", "tb_blog", "tb_follow", "tb_voucher_order"]
        self.psql("".join(f"ANALYZE {t};\n" for t in tables), with_prefix=False)
        out = self.psql(
            "".join(f"SELECT '{t}', count(*) FROM {t};\n" for t in tables)
            # 含索引与 TOAST 的总大小；只统计上面这几张业务表
            + "SELECT 'data+index MB', round(sum(pg_total_relation_size(t::regclass)) / 1048576.0)::bigint "
              f"FROM unnest(ARRAY[{', '.join(repr(t) for t in tables)}]) AS t;\n", with_prefix=False)
        log("完成。各表行数：")
        for line in out.splitlines():
            name, _, val = line.partition("\t")
            print(f"    {name:<20} {int(val):>12,}", flush=True)
        a = self.a
        print(
            "\n接下来：\n"
            "  1. 重启 app，让布隆过滤器重新预热（不重启的话要等定时增量同步，默认 1 分钟一轮）：\n"
            "       docker compose -f deploy/docker-compose.yml restart app frontend\n"
            "  2. 重新生成 token：python perf/baseline/seed_data.py --steps tokens --tokens 2000\n"
            f"  3. 压测时加 --prop SHOP_MAX={a.shops}，例如：\n"
            f"       python perf/baseline/baseline.py knee --jmx perf/shop-detail-bench.jmx --prop SHOP_MAX={a.shops}\n",
            flush=True)

    def run(self):
        a = self.a
        self.check()
        self.tune()
        pg_steps = [s for s in a.steps if not s.startswith("redis-") and s != "tokens"]
        if pg_steps:
            self.prepare()
        t0 = time.time()
        for s in a.steps:
            getattr(self, "seed_" + s.replace("-", "_"))()
        if pg_steps:
            self.summary()
        log(f"总耗时 {time.time() - t0:,.0f}s")


def main():
    if sys.version_info < (3, 12):
        die(f"需要 Python 3.12+，当前是 {sys.version.split()[0]}")
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass

    ap = argparse.ArgumentParser(description="压测造数：把 PostgreSQL / Redis 的数据量灌到接近上线的规模",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    g = ap.add_argument_group("规模")
    g.add_argument("--users", type=int, default=1_000_000, help="用户数（tb_user 的最大 id）")
    g.add_argument("--shops", type=int, default=100_000, help="店铺数（tb_shop 的最大 id）")
    g.add_argument("--orders", type=int, default=10_000_000, help="造数订单数")
    g.add_argument("--vouchers", type=int, default=None, help="优惠券数，默认 店铺数 × 2")
    g.add_argument("--blogs", type=int, default=None, help="笔记数（tb_blog 的最大 id），默认 用户数 ÷ 2")
    g.add_argument("--follows-per-user", type=int, default=3, help="每个用户关注几个创作者")
    g.add_argument("--order-days", type=int, default=60, help="订单与笔记铺在最近多少天")
    g.add_argument("--feed-users", type=int, default=100_000, help="为前多少个用户生成 Redis 关注收件箱")
    g.add_argument("--feed-days", type=int, default=7, help="收件箱包含最近多少天的笔记")
    g.add_argument("--tokens", type=int, default=None,
                   help="为前 N 个用户生成登录 token（写 Redis 并输出 token 文件）；不指定则不生成")
    g.add_argument("--token-file", default=str(ROOT / "token.txt"), help="token 输出文件，每行 token,userId")
    g = ap.add_argument_group("执行")
    g.add_argument("--steps", default=",".join(ALL_STEPS), help="要执行的步骤，逗号分隔，按给定顺序执行")
    g.add_argument("--chunk", type=int, default=250_000, help="每个事务写多少行")
    g.add_argument("--shared-buffers-gb", "--buffer-pool-gb", dest="shared_buffers_gb", type=float, default=0,
                   help="把 shared_buffers 改成这个值（0 = 不改）；ALTER SYSTEM 后需重启 postgres 再重跑")
    g = ap.add_argument_group("连接（默认值同 component-bench.sh，也可用同名环境变量）")
    g.add_argument("--net", default=env("NET", "sun_life_default"), help="docker 网络；被测组件在另一台机器时用 host")
    g.add_argument("--pg-host", default=env("PG_HOST", "postgres"))
    g.add_argument("--pg-port", default=env("PG_PORT", "5432"))
    g.add_argument("--pg-user", default=env("PG_USER", "postgres"))
    g.add_argument("--pg-password", default=env("PG_PASSWORD", "123456"))
    g.add_argument("--pg-db", default=env("PG_DB", "hmdp"))
    g.add_argument("--pg-image", default=env("PG_IMAGE", "postgres:16.4"))
    g.add_argument("--redis-host", default=env("REDIS_HOST", "redis"))
    g.add_argument("--redis-port", default=env("REDIS_PORT", "6379"))
    g.add_argument("--redis-password", default=env("REDIS_PASSWORD", ""))
    g.add_argument("--redis-image", default=env("REDIS_IMAGE", "redis:7.2"))
    args = ap.parse_args()

    args.steps = [s.strip() for s in args.steps.split(",") if s.strip()]
    unknown = [s for s in args.steps if s not in ALL_STEPS + ["tokens"]]
    if unknown:
        ap.error(f"未知步骤 {unknown}，可选：{', '.join(ALL_STEPS + ['tokens'])}")
    if args.tokens is not None and "tokens" not in args.steps:
        args.steps.append("tokens")
    if args.tokens is None:
        args.tokens = 2000
    if args.vouchers is None:
        args.vouchers = args.shops * 2
    if args.blogs is None:
        args.blogs = args.users // 2
    if min(args.users, args.shops, args.orders, args.vouchers, args.blogs, args.chunk, args.tokens) < 1:
        ap.error("规模参数与 --chunk 必须为正数")

    Seeder(args).run()


if __name__ == "__main__":
    main()
