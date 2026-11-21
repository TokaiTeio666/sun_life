#!/usr/bin/env bash
# =====================================================================
# 组件单测（阶段 3-1.2 第 1 步）：分别测出 Redis / MySQL / Kafka 各自的上限
#
# 用法（在能访问被测组件的机器上执行，Git Bash / Linux / macOS 均可）：
#   bash perf/baseline/component-bench.sh              # 全部
#   bash perf/baseline/component-bench.sh redis mysql  # 只测部分组件
#
# 压测客户端以一次性容器运行（docker run --rm），通过 compose 网络访问被测组件。
# 被测组件在另一台机器时，改用 --network host 并把 *_HOST 指向那台机器：
#   NET=host REDIS_HOST=10.0.0.5 REDIS_PORT=6380 MYSQL_HOST=10.0.0.5 MYSQL_PORT=3307 \
#   KAFKA_BOOTSTRAP=10.0.0.5:9094 bash perf/baseline/component-bench.sh
# 注意：客户端与被测组件同机时会抢 CPU，结果偏低，只能作为下限参考。
#
# 所有测试数据都写在独立的 key / 表 / topic 里，结束后清理，不影响业务数据：
#   Redis: bench:* 与 seckill:*:999999；MySQL: 券 999999 + bench_voucher_order 表；Kafka: bench.perf
# =====================================================================
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash 下不要把 /opt/... 参数改写成 Windows 路径

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
NET="${NET:-sun_life_default}"
REDIS_IMAGE="${REDIS_IMAGE:-redis:7.2}"
MYSQL_IMAGE="${MYSQL_IMAGE:-mysql:8.0}"
SYSBENCH_IMAGE="${SYSBENCH_IMAGE:-severalnines/sysbench}"
KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:3.7.0}"
REDIS_HOST="${REDIS_HOST:-redis}";   REDIS_PORT="${REDIS_PORT:-6379}"
MYSQL_HOST="${MYSQL_HOST:-mysql}";   MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:-root}";    MYSQL_PASSWORD="${MYSQL_PASSWORD:-123456}"; MYSQL_DB="${MYSQL_DB:-hmdp}"
# sysbench 1.0 自带的 MariaDB 客户端不支持 MySQL 8 默认的 caching_sha2_password，
# 脚本用上面的管理员账号临时建一个 mysql_native_password 的压测账号，结束后删除
BENCH_USER="${BENCH_USER:-bench}";   BENCH_PASSWORD="${BENCH_PASSWORD:-bench123}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"

REDIS_N="${REDIS_N:-200000}"           # 每项请求数
REDIS_CLIENTS="${REDIS_CLIENTS:-50}"   # 并发连接
MYSQL_CONCURRENCY="${MYSQL_CONCURRENCY:-50}"
MYSQL_TIME="${MYSQL_TIME:-30}"                 # 每条 SQL 压测秒数
KAFKA_RECORDS="${KAFKA_RECORDS:-1000000}"
KAFKA_RECORD_SIZE="${KAFKA_RECORD_SIZE:-200}"   # 秒杀订单消息约 80–100 字节，留余量

BENCH_VOUCHER=999999
OUT="${OUT:-$ROOT/perf/baseline/results/$(date +%Y%m%d-%H%M%S)-component}"
mkdir -p "$OUT"
REPORT="$OUT/component.md"

log() { echo "[$(date +%H:%M:%S)] $*" >&2; }
md()  { echo "$*" >> "$REPORT"; }

redis_cli()   { docker run --rm -i --network "$NET" "$REDIS_IMAGE" redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" "$@"; }
redis_bench() { docker run --rm --network "$NET" "$REDIS_IMAGE" redis-benchmark -h "$REDIS_HOST" -p "$REDIS_PORT" \
                  -n "$REDIS_N" -c "$REDIS_CLIENTS" --csv "$@"; }
mysql_exec()  { docker run --rm -i --network "$NET" -e MYSQL_PWD="$MYSQL_PASSWORD" "$MYSQL_IMAGE" \
                  mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u"$MYSQL_USER" "$MYSQL_DB" "$@"; }
# sysbench 通用参数；脚本经 stdin 写进容器，不依赖宿主机目录挂载（Windows 路径含中文时更稳）
sysbench_run() {
  local sql="$1" threads="$2"
  docker run --rm -i --network "$NET" -e BENCH_SQL="$sql" "$SYSBENCH_IMAGE" \
    sh -c 'cat > /tmp/b.lua && sysbench /tmp/b.lua "$@"' _ \
    --db-driver=mysql --mysql-host="$MYSQL_HOST" --mysql-port="$MYSQL_PORT" --mysql-user="$BENCH_USER" \
    --mysql-password="$BENCH_PASSWORD" --mysql-db="$MYSQL_DB" --threads="$threads" --time="$MYSQL_TIME" \
    --percentile=95 run < "$ROOT/perf/baseline/sql-bench.lua"
}
kafka_bin()   { docker run --rm --network "$NET" "$KAFKA_IMAGE" "/opt/kafka/bin/$1" "${@:2}"; }

# redis-benchmark --csv 输出："test","rps","avg_latency_ms","min","p50","p95","p99","max"
redis_row() {
  local label="$1"; shift
  local line
  line=$(redis_bench "$@" | grep -v '^"test"' | tail -1)
  IFS=',' read -r _ rps avg _ p50 p95 p99 _ <<<"$(echo "$line" | tr -d '"')"
  md "| $label | $rps | $avg | $p50 | $p95 | $p99 |"
  log "  $label: $rps ops/s, p99 ${p99}ms"
}

bench_redis() {
  log "Redis @ $REDIS_HOST:$REDIS_PORT（n=$REDIS_N, c=$REDIS_CLIENTS）"
  md "## Redis（$REDIS_HOST:$REDIS_PORT，n=$REDIS_N，c=$REDIS_CLIENTS）"
  md ""
  md "| 命令 | ops/s | 平均 ms | P50 | P95 | P99 |"
  md "| --- | ---: | ---: | ---: | ---: | ---: |"

  # 夹具：模拟登录态 hash、关注集合、秒杀库存
  redis_cli HSET bench:token id 1 nickName bench icon "" refreshAt 0 >/dev/null
  redis_cli SADD bench:set 1 2 3 4 5 >/dev/null
  redis_cli SET "seckill:stock:$BENCH_VOUCHER" 1000000000 >/dev/null
  redis_cli DEL "seckill:order:$BENCH_VOUCHER" "seckill:time:$BENCH_VOUCHER" >/dev/null

  redis_row "GET"                         -t get
  redis_row "HGETALL（登录态校验）"       HGETALL bench:token
  redis_row "SISMEMBER（是否关注）"       SISMEMBER bench:set 3

  local seckill_sha like_sha
  seckill_sha=$(redis_cli -x SCRIPT LOAD < "$ROOT/src/main/resources/seckill.lua" | tr -d '\r')
  # 与 BlogServiceImpl.TOGGLE_LIKE_SCRIPT 相同的逻辑；唯一区别是先 tonumber(ARGV[1])：
  # redis-benchmark 的 __rand_int__ 是补零的 12 位数字，GETBIT 不接受前导零
  like_sha=$(redis_cli SCRIPT LOAD "local uid = tonumber(ARGV[1]) local old = redis.call('getbit', KEYS[1], uid) if old == 0 then redis.call('setbit', KEYS[1], uid, 1) redis.call('zadd', KEYS[2], ARGV[2], uid) redis.call('zremrangebyrank', KEYS[2], tonumber(ARGV[3]), -1) else redis.call('setbit', KEYS[1], uid, 0) redis.call('zrem', KEYS[2], uid) end return old" | tr -d '\r')

  # __rand_int__ 被替换为随机 userId（-r 范围），模拟大量不同用户抢同一张券：单 key 热点的真实上限
  redis_row "EVALSHA seckill.lua（单券）"  -r 100000000000 EVALSHA "$seckill_sha" 0 "$BENCH_VOUCHER" __rand_int__ 1
  redis_row "EVALSHA 点赞 Lua"             -r 1000000 EVALSHA "$like_sha" 2 bench:liked:bit bench:liked:top __rand_int__ 1700000000000 1000

  redis_cli DEL bench:token bench:set bench:liked:bit bench:liked:top \
    "seckill:stock:$BENCH_VOUCHER" "seckill:order:$BENCH_VOUCHER" key:__rand_int__ >/dev/null
  md ""
}

# sysbench 输出 "transactions: N (X per sec.)" 与 "95th percentile: Y"
mysql_row() {
  local label="$1" query="$2" threads="${3:-$MYSQL_CONCURRENCY}"
  local out tps p95 avg
  out=$(sysbench_run "$query" "$threads")
  tps=$(echo "$out" | sed -n 's/.*transactions:.*(\([0-9.]*\) per sec.*/\1/p')
  avg=$(echo "$out" | sed -n 's/^ *avg: *\([0-9.]*\).*/\1/p')
  p95=$(echo "$out" | sed -n 's/.*95th percentile: *\([0-9.]*\).*/\1/p')
  md "| $label | $threads | ${tps%.*} | $avg | $p95 |"
  log "  $label: ${tps%.*} /s, p95 ${p95}ms"
}

bench_mysql() {
  log "MySQL @ $MYSQL_HOST:$MYSQL_PORT（threads=$MYSQL_CONCURRENCY, 每条 ${MYSQL_TIME}s）"
  md "## MySQL（$MYSQL_HOST:$MYSQL_PORT，sysbench 执行业务真实 SQL，每条 ${MYSQL_TIME}s）"
  md ""
  md "| SQL | 并发 | 每秒执行 | 平均 ms | P95 ms |"
  md "| --- | ---: | ---: | ---: | ---: |"

  # 压测券 end_time 设为两天前：应用的库存预热和对账采样都不会扫到它
  mysql_exec <<SQL
DELETE FROM tb_seckill_voucher WHERE voucher_id = $BENCH_VOUCHER;
INSERT INTO tb_seckill_voucher (voucher_id, stock, begin_time, end_time)
  VALUES ($BENCH_VOUCHER, 1000000000, NOW() - INTERVAL 3 DAY, NOW() - INTERVAL 2 DAY);
DROP TABLE IF EXISTS bench_voucher_order;
CREATE TABLE bench_voucher_order LIKE tb_voucher_order;
DROP USER IF EXISTS '$BENCH_USER'@'%';
CREATE USER '$BENCH_USER'@'%' IDENTIFIED WITH mysql_native_password BY '$BENCH_PASSWORD';
GRANT SELECT, INSERT, UPDATE, DELETE ON $MYSQL_DB.* TO '$BENCH_USER'@'%';
GRANT ALL ON sbtest.* TO '$BENCH_USER'@'%';
SQL

  local values="" i
  for i in $(seq 1 24); do values+="${values:+,}(UUID_SHORT(), $i, $BENCH_VOUCHER, 1)"; done

  mysql_row "主键点查（店铺详情回源）" "SELECT * FROM tb_shop WHERE id = 1"
  mysql_row "热门笔记（ORDER BY liked）" "SELECT id, user_id, title, liked FROM tb_blog ORDER BY liked DESC LIMIT 10"
  mysql_row "超时关单扫描（idx_status_ctime）" \
    "SELECT id, user_id, voucher_id FROM tb_voucher_order WHERE status = 1 AND create_time < NOW() - INTERVAL 30 MINUTE ORDER BY create_time LIMIT 500"
  mysql_row "热点行扣库存 stock = stock - 1" \
    "UPDATE tb_seckill_voucher SET stock = stock - 1 WHERE voucher_id = $BENCH_VOUCHER AND stock >= 1"
  mysql_row "热点行扣库存 stock = stock - 24（批量聚合）" \
    "UPDATE tb_seckill_voucher SET stock = stock - 24 WHERE voucher_id = $BENCH_VOUCHER AND stock >= 24"
  mysql_row "多值 INSERT 24 行" "INSERT INTO bench_voucher_order (id, user_id, voucher_id, status) VALUES $values"

  mysql_exec <<SQL
DELETE FROM tb_seckill_voucher WHERE voucher_id = $BENCH_VOUCHER;
DROP TABLE IF EXISTS bench_voucher_order;
SQL
  md ""
  md "> 热点行的两行对比即「批量聚合」的收益：每秒能执行的 UPDATE 次数接近，但每次扣 24 单。"
  md ""
  if [[ "${SYSBENCH:-0}" == "1" ]]; then bench_sysbench; fi
  mysql_exec -e "DROP USER IF EXISTS '$BENCH_USER'@'%'"
}

# 可选：sysbench 通用基准（SYSBENCH=1 开启），使用独立库 sbtest
bench_sysbench() {
  local img="$SYSBENCH_IMAGE"
  local args=(--db-driver=mysql --mysql-host="$MYSQL_HOST" --mysql-port="$MYSQL_PORT" --mysql-user="$BENCH_USER"
              --mysql-password="$BENCH_PASSWORD" --mysql-db=sbtest --tables=8 --table-size=100000
              --threads="$MYSQL_CONCURRENCY" --time=60 --report-interval=0)
  mysql_exec -e "CREATE DATABASE IF NOT EXISTS sbtest"
  docker run --rm --network "$NET" "$img" sysbench oltp_read_write "${args[@]}" prepare >/dev/null
  md "## sysbench（8 表 × 10 万行，threads=$MYSQL_CONCURRENCY，60s）"
  md ""
  md '```'
  for t in oltp_point_select oltp_write_only; do
    log "  sysbench $t"
    docker run --rm --network "$NET" "$img" sysbench "$t" "${args[@]}" run \
      | grep -E 'queries:|transactions:|95th percentile' | sed "s/^/$t /" >> "$REPORT"
  done
  md '```'
  md ""
  docker run --rm --network "$NET" "$img" sysbench oltp_read_write "${args[@]}" cleanup >/dev/null
  mysql_exec -e "DROP DATABASE IF EXISTS sbtest"
}

bench_kafka() {
  log "Kafka @ $KAFKA_BOOTSTRAP（records=$KAFKA_RECORDS, size=$KAFKA_RECORD_SIZE）"
  kafka_bin kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" --delete --topic bench.perf >/dev/null 2>&1 || true
  kafka_bin kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" --create --topic bench.perf \
    --partitions 12 --replication-factor 1 >/dev/null

  md "## Kafka（$KAFKA_BOOTSTRAP，12 分区，$KAFKA_RECORDS 条 × $KAFKA_RECORD_SIZE B）"
  md ""
  md '```'
  # 与应用生产者一致：acks=all + 幂等 + linger.ms=5
  kafka_bin kafka-producer-perf-test.sh --topic bench.perf --num-records "$KAFKA_RECORDS" \
    --record-size "$KAFKA_RECORD_SIZE" --throughput -1 \
    --producer-props bootstrap.servers="$KAFKA_BOOTSTRAP" acks=all enable.idempotence=true linger.ms=5 \
    | tail -1 | sed 's/^/producer: /' >> "$REPORT"
  kafka_bin kafka-consumer-perf-test.sh --bootstrap-server "$KAFKA_BOOTSTRAP" --topic bench.perf \
    --messages "$KAFKA_RECORDS" --timeout 60000 \
    | tail -1 | sed 's/^/consumer: /' >> "$REPORT"
  md '```'
  md ""
  md "> consumer 一行字段依次为：start, end, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, ..."
  md ""
  kafka_bin kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" --delete --topic bench.perf >/dev/null 2>&1 || true
  log "  完成"
}

{
  echo "# 组件单测结果"
  echo
  echo "- 时间：$(date '+%Y-%m-%d %H:%M:%S')"
  echo "- 网络：\`$NET\`；客户端与被测组件$( [[ "$NET" == "host" ]] && echo "分机" || echo "**同机**（结果偏低，仅作下限参考）")"
  echo
} > "$REPORT"

targets=("$@")
[[ ${#targets[@]} -eq 0 ]] && targets=(redis mysql kafka)
for t in "${targets[@]}"; do
  case "$t" in
    redis) bench_redis ;;
    mysql) bench_mysql ;;
    kafka) bench_kafka ;;
    *) echo "未知组件: $t（可选 redis / mysql / kafka）" >&2; exit 1 ;;
  esac
done
log "结果：$REPORT"
cat "$REPORT"
