#!/usr/bin/env bash
# ===== sun_life 一键停止（Linux / macOS） =====
cd "$(dirname "$0")/.."
docker compose -f deploy/docker-compose.yml down
echo "服务已停止（数据卷保留，再次 start.sh 数据不丢）"
