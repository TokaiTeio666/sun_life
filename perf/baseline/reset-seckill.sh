#!/usr/bin/env bash
# 重置压测券（默认 21）：清订单、DB 与 Redis 库存回满、清一人一单集合。
# 供 baseline.py 的 --before-each 使用，等价于 perf/README.md「每轮压测前重置」那几条命令。
#   VOUCHER_ID=21 STOCK=100000 bash perf/baseline/reset-seckill.sh
#
# 默认通过 docker exec 操作本机的 hmdp-postgres / hmdp-redis 容器。
# 分机部署（PostgreSQL / Redis 在别的机器上）时指定地址，改用一次性客户端容器，变量与 component-bench.sh 通用：
#   NET=host PG_HOST=10.0.0.11 PG_PORT=5433 PG_PASSWORD=xxx REDIS_HOST=10.0.0.12 bash perf/baseline/reset-seckill.sh
# 券不存在时会一并创建（等价于先执行 perf/bench-voucher.sql）；已存在时只重置库存，不改活动时间。
set -euo pipefail
export MSYS_NO_PATHCONV=1
VOUCHER_ID="${VOUCHER_ID:-21}"
STOCK="${STOCK:-100000}"
PG_CONTAINER="${PG_CONTAINER:-hmdp-postgres}"
REDIS_CONTAINER="${REDIS_CONTAINER:-hmdp-redis}"
PG_PASSWORD="${PG_PASSWORD:-123456}"
NET="${NET:-host}"

PSQL_OPTS=(-v ON_ERROR_STOP=1 -X -q)
if [ -n "${PG_HOST:-}" ]; then
  pg_exec() { docker run --rm -i --network "$NET" -e PGPASSWORD="$PG_PASSWORD" "${PG_IMAGE:-postgres:16.4}" \
                psql "${PSQL_OPTS[@]}" -h "$PG_HOST" -p "${PG_PORT:-5432}" -U "${PG_USER:-postgres}" -d "${PG_DB:-hmdp}" "$@"; }
else
  pg_exec() { docker exec -i -e PGPASSWORD="$PG_PASSWORD" "$PG_CONTAINER" psql "${PSQL_OPTS[@]}" -U "${PG_USER:-postgres}" -d "${PG_DB:-hmdp}" "$@"; }
fi
if [ -n "${REDIS_HOST:-}" ]; then
  # 没有密码时不能传空的 REDISCLI_AUTH（redis-cli 会照样发 AUTH）
  redis_cli() { docker run --rm --network "$NET" ${REDIS_PASSWORD:+-e REDISCLI_AUTH="$REDIS_PASSWORD"} "${REDIS_IMAGE:-redis:7.2}" \
                  redis-cli -h "$REDIS_HOST" -p "${REDIS_PORT:-6379}" "$@"; }
else
  redis_cli() { docker exec "$REDIS_CONTAINER" redis-cli "$@"; }
fi

pg_exec -c "
  INSERT INTO tb_voucher (id, shop_id, title, sub_title, rules, pay_value, actual_value, type, status)
    VALUES ($VOUCHER_ID, 1, 'seckill bench', 'bench', 'bench only', 100, 1000, 1, 1)
    ON CONFLICT (id) DO NOTHING;
  INSERT INTO tb_seckill_voucher (voucher_id, stock, begin_time, end_time)
    VALUES ($VOUCHER_ID, $STOCK, now(), now() + interval '7 days')
    ON CONFLICT (voucher_id) DO UPDATE SET stock = EXCLUDED.stock;
  DELETE FROM tb_voucher_order WHERE voucher_id = $VOUCHER_ID;" >/dev/null
redis_cli SET "seckill:stock:$VOUCHER_ID" "$STOCK" >/dev/null
# 通知各副本清除这张券的售罄本地标记（不发的话要等标记自己过期，默认 5s）
redis_cli PUBLISH "hmdp:local-cache:evict" "soldOut|$VOUCHER_ID" >/dev/null
# 活动时间 key 不存在时 Lua 跳过时间校验（压测券可能在应用启动后才导入）
redis_cli DEL "seckill:order:$VOUCHER_ID" "seckill:time:$VOUCHER_ID" >/dev/null
echo "券 $VOUCHER_ID 已重置：库存 $STOCK"
