#!/usr/bin/env bash
# ===== hm-dianping 一键启动（Linux / macOS） =====
# 前置：已安装 Docker 与 docker compose 插件、Maven（mvn 在 PATH）
set -e
cd "$(dirname "$0")/.."

echo "[1/2] 编译应用 jar ..."
mvn -q clean package -DskipTests

echo "[2/2] 构建镜像并后台启动全部服务 ..."
docker compose -f deploy/docker-compose.yml up -d --build

echo
echo "等待服务就绪后访问：http://localhost:8081"
echo "查看状态：docker compose -f deploy/docker-compose.yml ps"
echo "停止服务：deploy/stop.sh"
