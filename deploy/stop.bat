@echo off
REM ===== sun_life 一键停止（Windows） =====
cd /d %~dp0\..
docker compose -f deploy\docker-compose.yml down
echo 服务已停止（数据卷保留在 docker volumes 中，再次 start.bat 数据不丢）
