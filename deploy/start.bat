@echo off
REM ===== hm-dianping 一键启动（Windows） =====
REM 前置：已安装 Docker Desktop 并启动、已安装 Maven（mvn 在 PATH）
setlocal
cd /d %~dp0\..

echo [1/2] 编译应用 jar ...
call mvn -q clean package -DskipTests
if errorlevel 1 (
  echo 编译失败，请检查 Maven/JDK 环境
  exit /b 1
)

echo [2/2] 构建镜像并后台启动全部服务 ...
docker compose -f deploy\docker-compose.yml up -d --build

echo.
echo 等待服务就绪后访问：http://localhost:8081
echo 查看状态：docker compose -f deploy\docker-compose.yml ps
echo 停止服务：deploy\stop.bat
endlocal
