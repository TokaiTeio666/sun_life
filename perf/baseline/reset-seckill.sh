#!/usr/bin/env bash
# 重置压测券（默认 21）：清订单、DB 与 Redis 库存回满、清一人一单集合。
# 供 baseline.py 的 --before-each 使用，等价于 perf/README.md「每轮压测前重置」那几条命令。
#   VOUCHER_ID=21 STOCK=100000 bash perf/baseline/reset-seckill.sh
set -euo pipefail
VOUCHER_ID="${VOUCHER_ID:-21}"
STOCK="${STOCK:-100000}"
MYSQL_CONTAINER="${MYSQL_CONTAINER:-hmdp-mysql}"
REDIS_CONTAINER="${REDIS_CONTAINER:-hmdp-redis}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-123456}"

docker exec -e MYSQL_PWD="$MYSQL_PASSWORD" "$MYSQL_CONTAINER" mysql -uroot hmdp -e \
  "DELETE FROM tb_voucher_order WHERE voucher_id=$VOUCHER_ID; UPDATE tb_seckill_voucher SET stock=$STOCK WHERE voucher_id=$VOUCHER_ID;"
docker exec "$REDIS_CONTAINER" redis-cli SET "seckill:stock:$VOUCHER_ID" "$STOCK" >/dev/null
# 活动时间 key 不存在时 Lua 跳过时间校验（压测券可能在应用启动后才导入）
docker exec "$REDIS_CONTAINER" redis-cli DEL "seckill:order:$VOUCHER_ID" "seckill:time:$VOUCHER_ID" >/dev/null
echo "券 $VOUCHER_ID 已重置：库存 $STOCK"
