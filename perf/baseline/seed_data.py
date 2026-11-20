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
  python perf/baseline/seed_data.py --buffer-pool-gb 4               # 灌数前把 InnoDB 缓冲池调到 4G（见下）
  python perf/baseline/seed_data.py --steps tokens --tokens 2000     # 只生成登录 token（写 Redis + token.txt）

数据全部由 MySQL 服务端用 INSERT ... SELECT 生成，不经过网络传输；客户端以一次性容器运行
（docker run --rm），连接方式与 component-bench.sh 相同，环境变量也通用：
  NET=host MYSQL_HOST=10.0.0.5 MYSQL_PORT=3307 REDIS_HOST=10.0.0.6 REDIS_PORT=6380 \
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
  2. 重新生成 token：加 --tokens <N>（或单独跑 --steps tokens），MySQL / Redis 不在本机时也能用；
  3. 压测加 --prop SHOP_MAX=<店铺数>。

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
        self.prefix = "SET NAMES utf8mb4;\n"
        if not args.keep_binlog:
            # 灌数不写 binlog：省一半以上的写入量。有从库、或托管数据库不允许时加 --keep-binlog
            self.prefix += "SET SESSION sql_log_bin = 0;\n"

    # ---------- 客户端 ----------

    def mysql(self, sql, with_prefix=True):
        a = self.a
        cmd = ["docker", "run", "--rm", "-i", "--network", a.net, "-e", f"MYSQL_PWD={a.mysql_password}",
               a.mysql_image, "mysql", "-h", a.mysql_host, "-P", str(a.mysql_port), f"-u{a.mysql_user}",
               "--default-character-set=utf8mb4", "-N", "-B", a.mysql_db]
        body = (self.prefix if with_prefix else "SET NAMES utf8mb4;\n") + sql
        r = subprocess.run(cmd, input=body.encode("utf-8"), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if r.returncode != 0:
            err = r.stderr.decode("utf-8", "replace").strip()
            hint = ""
            if "sql_log_bin" in err or "SUPER" in err or "Access denied; you need" in err:
                hint = "\n提示：当前账号不能关闭 binlog，加 --keep-binlog 重试。"
            die(f"MySQL 执行失败：{err}{hint}\n--- SQL（前 600 字符）---\n{sql[:600]}")
        return r.stdout.decode("utf-8", "replace")

    def scalar(self, sql):
        out = self.mysql(sql, with_prefix=False).strip()
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

    def prepare(self, seq_rows):
        self.mysql(
            "CREATE TABLE IF NOT EXISTS seed_progress ("
            " step VARCHAR(32) PRIMARY KEY, done BIGINT NOT NULL,"
            " update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
            ") COMMENT = 'seed_data.py 的进度，可删除（删除后各步从现有最大 id 续跑）';\n"
            "CREATE TABLE IF NOT EXISTS seed_seq (n INT PRIMARY KEY) COMMENT = 'seed_data.py 的临时序列表';\n")
        if int(self.scalar("SELECT COUNT(*) FROM seed_seq") or 0) < seq_rows:
            self.mysql(
                f"SET SESSION cte_max_recursion_depth = {seq_rows + 10};\n"
                "TRUNCATE TABLE seed_seq;\n"
                "INSERT INTO seed_seq WITH RECURSIVE s(n) AS ("
                f"SELECT 0 UNION ALL SELECT n + 1 FROM s WHERE n + 1 < {seq_rows}) SELECT n FROM s;\n")

    def progress(self, step, init_sql):
        """读取某一步的进度；第一次运行时用 init_sql 的结果初始化。"""
        self.mysql(f"INSERT IGNORE INTO seed_progress (step, done) SELECT '{step}', ({init_sql});\n")
        return int(self.scalar(f"SELECT done FROM seed_progress WHERE step = '{step}'"))

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
            self.mysql("START TRANSACTION;\n" + make_sql(done, cnt)
                       + f"UPDATE seed_progress SET done = {done + cnt} WHERE step = '{step}';\nCOMMIT;\n")
            done += cnt
            el = time.time() - t0
            if el - last >= 5 or done >= target:
                last = el
                rate = (done - start) / el if el > 0 else 0
                eta = (target - done) / rate if rate > 0 else 0
                log(f"  {label} {done - start:,}/{total:,}  {rate:,.0f}/s  剩余约 {eta:,.0f}s")

    # ---------- MySQL 各步 ----------

    def seed_user(self):
        start = self.progress("user", "SELECT COALESCE(MAX(id), 0) FROM tb_user")
        now = self.now

        def sql(done, cnt):
            b = done + 1
            return (
                "INSERT INTO tb_user (id, phone, password, nick_name, icon, create_time)\n"
                f"SELECT {b} + n, CONCAT('19', LPAD({b} + n, 9, '0')), '', CONCAT('user_', {b} + n), '',\n"
                f"       FROM_UNIXTIME({now} - FLOOR(RAND() * 63072000))\n"
                f"FROM seed_seq WHERE n < {cnt};\n")
        self.run_step("user", "用户 tb_user", start, self.a.users, sql)

    def seed_user_info(self):
        start = self.progress("user_info", "SELECT 0")

        def sql(done, cnt):
            b = done + 1
            return (
                "INSERT IGNORE INTO tb_user_info (user_id, city, introduce, fans, followee, gender, birthday, credits, level)\n"
                f"SELECT {b} + n, ELT(1 + FLOOR(RAND() * 8), '杭州', '上海', '北京', '广州', '深圳', '成都', '南京', '武汉'),\n"
                "       '这个人很懒，什么都没有留下', 0, 0, FLOOR(RAND() * 2),\n"
                "       DATE_SUB('2005-01-01', INTERVAL FLOOR(RAND() * 10000) DAY), FLOOR(RAND() * 5000), FLOOR(RAND() * 10)\n"
                f"FROM seed_seq WHERE n < {cnt};\n")
        self.run_step("user_info", "用户资料 tb_user_info", start, self.a.users, sql)

    def seed_shop(self):
        start = self.progress("shop", "SELECT COALESCE(MAX(id), 0) FROM tb_shop")

        def sql(done, cnt):
            b = done + 1
            img = "'https://img.example.com/msmerchant/', MD5(CONCAT({k}, ':', {b} + n)), '.jpg'"
            images = ", ',', ".join(img.format(k=k, b=b) for k in (1, 2, 3))
            return (
                "INSERT INTO tb_shop (id, name, type_id, images, area, address, x, y, avg_price, sold, comments, score, open_hours)\n"
                f"SELECT {b} + n,\n"
                "       CONCAT(ELT(1 + FLOOR(RAND() * 10), '老街火锅', '江南小馆', '川味坊', '海鲜大排档', '日式料理',\n"
                f"              '韩式烤肉', '茶餐厅', '量贩KTV', '美发沙龙', '健身工作室'), '(', {b} + n, '号店)'),\n"
                # 美食类占四成，其余类型均分：让 shop:geo:1 明显比其他类型大，接近真实分布
                "       IF(RAND() < 0.4, 1, 1 + FLOOR(RAND() * 10)),\n"
                f"       CONCAT({images}),\n"
                "       ELT(1 + FLOOR(RAND() * 8), '大关', '拱宸桥/上塘', '运河上街', '北部新城', '武林广场', '西湖文化广场', '钱江新城', '滨江'),\n"
                "       CONCAT(ELT(1 + FLOOR(RAND() * 6), '金华路', '丽水路', '台州路', '湖墅南路', '文三路', '江南大道'), 1 + FLOOR(RAND() * 999), '号'),\n"
                "       ROUND(120.0 + RAND() * 0.4, 6), ROUND(30.1 + RAND() * 0.3, 6),\n"
                "       20 + FLOOR(RAND() * 300), FLOOR(RAND() * 30000), FLOOR(RAND() * 10000), 30 + FLOOR(RAND() * 21), '10:00-22:00'\n"
                f"FROM seed_seq WHERE n < {cnt};\n")
        self.run_step("shop", "店铺 tb_shop", start, self.a.shops, sql)

    def seed_voucher(self):
        a = self.a
        start = self.progress(
            "voucher",
            f"SELECT GREATEST({VOUCHER_BASE}, COALESCE(MAX(id), 0)) FROM tb_voucher "
            f"WHERE id BETWEEN {VOUCHER_BASE + 1} AND {VOUCHER_LIMIT}")

        def sql(done, cnt):
            b = done + 1
            vid = f"({b} + n)"
            face = f"ELT(1 + ({vid} MOD 4), 2000, 5000, 10000, 20000)"
            return (
                "INSERT INTO tb_voucher (id, shop_id, title, sub_title, rules, pay_value, actual_value, type, status)\n"
                f"SELECT {vid}, 1 + (({vid} - {VOUCHER_BASE + 1}) MOD {a.shops}), CONCAT({face} DIV 100, '元代金券'), '周一至周日均可使用',\n"
                "       '全场通用\\n无需预约\\n可无限叠加\\n不兑现、不找零\\n仅限堂食',\n"
                f"       {face} * (80 + ({vid} MOD 16)) DIV 100, {face}, IF({vid} MOD 10 = 0, 1, 0), 1\n"
                f"FROM seed_seq WHERE n < {cnt};\n"
                # 秒杀券活动全部已结束（至少 3 天前）：SeckillStockInitializer 只预热未过期的券，
                # 否则每个副本启动时要为几万张券逐个访问 Redis
                "INSERT INTO tb_seckill_voucher (voucher_id, stock, begin_time, end_time)\n"
                f"SELECT {vid}, FLOOR(RAND() * 200), NOW() - INTERVAL (10 + ({vid} MOD 80)) DAY, NOW() - INTERVAL (3 + ({vid} MOD 80)) DAY\n"
                f"FROM seed_seq WHERE n < {cnt} AND {vid} MOD 10 = 0;\n")
        self.run_step("voucher", "优惠券 tb_voucher", start, VOUCHER_BASE + a.vouchers, sql)

    def seed_blog(self):
        a = self.a
        start = self.progress("blog", "SELECT COALESCE(MAX(id), 0) FROM tb_blog")
        now, span, creators = self.now, a.order_days * 86400, self.creators

        def sql(done, cnt):
            b = done + 1
            return (
                "INSERT INTO tb_blog (id, shop_id, user_id, title, images, content, liked, comments, create_time)\n"
                f"SELECT {b} + n, 1 + FLOOR(RAND() * {a.shops}), 1 + FLOOR({creators} * POW(RAND(), 2)),\n"
                "       CONCAT(ELT(1 + FLOOR(RAND() * 6), '周末探店好去处', '人均50吃到撑', '这家店我能吃一辈子', '约会圣地推荐',\n"
                f"              '打工人的深夜食堂', '宝藏小店分享'), ' #', {b} + n),\n"
                f"       CONCAT('/imgs/blogs/', ({b} + n) MOD 16, '/', ({b} + n) MOD 7, '/', MD5({b} + n), '.jpg'),\n"
                "       REPEAT('环境很好，服务热情，菜品味道不错，分量也足，性价比很高，推荐大家周末来打卡。', 6),\n"
                # 点赞数长尾：P(点赞 ≥ x) = 1/(x+1)，约九成笔记是个位数，万分之一上万（热门榜 ORDER BY liked DESC 走 idx_liked）
                "       LEAST(100000, FLOOR(1 / (1 - RAND())) - 1), FLOOR(RAND() * 500),\n"
                f"       FROM_UNIXTIME({now} - FLOOR(RAND() * {span}))\n"
                f"FROM seed_seq WHERE n < {cnt};\n")
        self.run_step("blog", "笔记 tb_blog", start, a.blogs, sql)

    def seed_follow(self):
        a = self.a
        start = self.progress("follow", "SELECT 0")
        now, creators = self.now, self.creators

        def sql(done, cnt):
            b = done + 1
            # 关注对象按三次方偏向头部创作者：1 号创作者拿到约 (1/创作者数)^(1/3) 的关注（10 万创作者时约 2%）。
            # 同一用户多次抽到同一人由 uk_user_follow + INSERT IGNORE 去重，所以实际行数略少于 用户数 × 关注数
            one = (
                "INSERT IGNORE INTO tb_follow (user_id, follow_user_id, create_time)\n"
                f"SELECT /*+ NO_MERGE(t) */ u, f, FROM_UNIXTIME({now} - FLOOR(RAND() * 31536000))\n"
                f"FROM (SELECT {b} + n AS u, 1 + FLOOR({creators} * POW(RAND(), 3)) AS f FROM seed_seq WHERE n < {cnt}) t\n"
                "WHERE u <> f;\n")
            return one * a.follows_per_user
        self.run_step("follow", "关注 tb_follow（按用户）", start, a.users, sql)
        if start < a.users:
            # 粉丝数 / 关注数与 tb_follow 对齐（按全表重算，可重复执行）
            log("回填 tb_user_info 的 fans / followee")
            self.mysql(
                "UPDATE tb_user_info i JOIN (SELECT follow_user_id AS u, COUNT(*) AS c FROM tb_follow GROUP BY follow_user_id) t\n"
                "  ON t.u = i.user_id SET i.fans = t.c;\n"
                "UPDATE tb_user_info i JOIN (SELECT user_id AS u, COUNT(*) AS c FROM tb_follow GROUP BY user_id) t\n"
                "  ON t.u = i.user_id SET i.followee = t.c;\n")

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

        def sql(done, cnt):
            return (
                "INSERT INTO tb_voucher_order (id, user_id, voucher_id, pay_type, status, create_time, pay_time, use_time, refund_time, update_time)\n"
                f"SELECT /*+ NO_MERGE(t) */ ((ct - {begin}) << 32) | {ORDER_SEED_FLAG} | i, uid, vid, pt, st, FROM_UNIXTIME(ct),\n"
                f"       IF(st IN (2, 3, 6), FROM_UNIXTIME(LEAST(ct + 30 + FLOOR(r * 600), {now})), NULL),\n"
                f"       IF(st = 3, FROM_UNIXTIME(LEAST(ct + 3600 + FLOOR(r * 259200), {now})), NULL),\n"
                f"       IF(st = 6, FROM_UNIXTIME(LEAST(ct + 3600 + FLOOR(r * 86400), {now})), NULL),\n"
                "       FROM_UNIXTIME(ct)\n"
                "FROM (\n"
                f"  SELECT {done} + n AS i, {t_start} + ({done} + n) * {span} DIV {total} AS ct,\n"
                f"         1 + FLOOR({users} * POW(RAND(), 1.5)) AS uid,\n"
                f"         {VOUCHER_BASE + 1} + FLOOR({vouchers} * POW(RAND(), 2)) AS vid,\n"
                "         1 + FLOOR(RAND() * 3) AS pt,\n"
                # 已核销 55% / 已支付 30% / 已取消 10% / 已退款 5%；不生成「未支付」（见文件头说明）
                "         ELT(1 + FLOOR(RAND() * 20), 3,3,3,3,3,3,3,3,3,3,3, 2,2,2,2,2,2, 4,4, 6) AS st,\n"
                "         RAND() AS r\n"
                f"  FROM seed_seq WHERE n < {cnt}\n"
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
        rows = self.mysql("SELECT type_id, x, y, id FROM tb_shop;\n", with_prefix=False).splitlines()
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
            rows = self.mysql(
                "SELECT f.user_id, b.id, UNIX_TIMESTAMP(b.create_time) * 1000 FROM tb_follow f\n"
                "JOIN tb_blog b ON b.user_id = f.follow_user_id\n"
                f"WHERE f.user_id BETWEEN {lo} AND {hi} AND b.create_time >= NOW() - INTERVAL {a.feed_days} DAY;\n",
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
        rows = self.mysql(f"SELECT id, nick_name, icon FROM tb_user ORDER BY id LIMIT {a.tokens};\n",
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

    def tune(self):
        a = self.a
        if a.buffer_pool_gb:
            self.mysql(f"SET GLOBAL innodb_buffer_pool_size = {int(a.buffer_pool_gb * 1024 ** 3)};\n", with_prefix=False)
            log(f"innodb_buffer_pool_size 已调到 {a.buffer_pool_gb:g}G（未持久化，MySQL 重启后恢复原值）")
        pool = int(self.scalar("SELECT @@innodb_buffer_pool_size") or 0)
        if "order" in a.steps and a.orders >= 1_000_000 and pool < 1024 ** 3:
            log(f"[WARN] innodb_buffer_pool_size 只有 {pool / 1024 ** 2:,.0f}M：千万级订单的二级索引放不进内存，"
                "灌数会越来越慢，压测也会变成测磁盘。建议加 --buffer-pool-gb（机器内存的 50–70%）。")

    def summary(self):
        tables = ["tb_user", "tb_user_info", "tb_shop", "tb_voucher", "tb_seckill_voucher", "tb_blog", "tb_follow", "tb_voucher_order"]
        self.mysql("".join(f"ANALYZE TABLE {t};\n" for t in tables), with_prefix=False)
        out = self.mysql(
            "".join(f"SELECT '{t}', COUNT(*) FROM {t};\n" for t in tables)
            + "SELECT 'data+index MB', ROUND(SUM(data_length + index_length) / 1048576) FROM information_schema.tables "
              "WHERE table_schema = DATABASE();\n", with_prefix=False)
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
        mysql_steps = [s for s in a.steps if not s.startswith("redis-") and s != "tokens"]
        if mysql_steps:
            self.prepare(a.chunk)
        t0 = time.time()
        for s in a.steps:
            getattr(self, "seed_" + s.replace("-", "_"))()
        if mysql_steps:
            self.mysql("DROP TABLE IF EXISTS seed_seq;\n", with_prefix=False)
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

    ap = argparse.ArgumentParser(description="压测造数：把 MySQL / Redis 的数据量灌到接近上线的规模",
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
    g.add_argument("--buffer-pool-gb", type=float, default=0, help="灌数前 SET GLOBAL innodb_buffer_pool_size（0 = 不改）")
    g.add_argument("--keep-binlog", action="store_true", help="灌数时照常写 binlog（有从库、或托管数据库不允许关闭时用）")
    g = ap.add_argument_group("连接（默认值同 component-bench.sh，也可用同名环境变量）")
    g.add_argument("--net", default=env("NET", "sun_life_default"), help="docker 网络；被测组件在另一台机器时用 host")
    g.add_argument("--mysql-host", default=env("MYSQL_HOST", "mysql"))
    g.add_argument("--mysql-port", default=env("MYSQL_PORT", "3306"))
    g.add_argument("--mysql-user", default=env("MYSQL_USER", "root"))
    g.add_argument("--mysql-password", default=env("MYSQL_PASSWORD", "123456"))
    g.add_argument("--mysql-db", default=env("MYSQL_DB", "hmdp"))
    g.add_argument("--mysql-image", default=env("MYSQL_IMAGE", "mysql:8.0"))
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
