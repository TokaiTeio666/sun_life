<#
.SYNOPSIS
    复现 Redis 主从切换导致的分布式锁失效：旧持有者仍以为自己持锁，新主上同一把锁却能被第二个客户端拿到。

.DESCRIPTION
    需要一个可用的 Docker。脚本会临时启动一对 Redis（主 16379 / 从），
    执行 SETNX 加锁 → 停主 → 从库提升为主 → 第二个客户端再次 SETNX。
    预期第二次 SETNX 返回 OK，说明仅靠 Redis 单机锁在主从切换时并不安全。

.EXAMPLE
    pwsh perf/lock-failover-demo.ps1
#>
param(
    [int]$MasterPort = 16379,
    [string]$MasterName = 'hmdp-lock-master',
    [string]$ReplicaName = 'hmdp-lock-replica',
    [switch]$Keep
)

$ErrorActionPreference = 'Stop'

function Invoke-Redis {
    param([string]$Container, [string[]]$Command)
    docker exec $Container redis-cli @Command
}

docker rm -f $MasterName $ReplicaName 2>$null | Out-Null

Write-Host '== 1. 启动主从' -ForegroundColor Cyan
docker run -d --name $MasterName -p "${MasterPort}:6379" redis:7.2 | Out-Null
docker run -d --name $ReplicaName redis:7.2 redis-server --replicaof host.docker.internal $MasterPort | Out-Null
Start-Sleep -Seconds 3

# 等待首次全量同步完成，确保从库一开始是健康的
for ($i = 0; $i -lt 20; $i++) {
    $info = docker exec $ReplicaName redis-cli INFO replication
    if ($info -match 'master_link_status:up') { break }
    Start-Sleep -Milliseconds 500
}

# 模拟「复制中断/延迟窗口」：断开复制后再加锁，锁不会同步到从库
Invoke-Redis $ReplicaName @('REPLICAOF', 'NO', 'ONE') | Out-Null
Write-Host '复制已断开（模拟主从延迟窗口）'

Write-Host '== 2. 客户端 A 在主库加锁（SETNX + TTL）' -ForegroundColor Cyan
$lockResult = Invoke-Redis $MasterName @('SET', 'lock:order:1', 'holder-A', 'NX', 'EX', '60')
Write-Host "A SETNX -> $lockResult"
Write-Host "主库 GET -> $(Invoke-Redis $MasterName @('GET', 'lock:order:1'))"
Write-Host "从库 GET -> $(Invoke-Redis $ReplicaName @('GET', 'lock:order:1'))"

Write-Host '== 3. 主库宕机，从库提升为新主' -ForegroundColor Cyan
docker stop $MasterName | Out-Null
Start-Sleep -Seconds 1

Write-Host '== 4. 客户端 B 在新主上申请同一把锁' -ForegroundColor Cyan
$secondResult = Invoke-Redis $ReplicaName @('SET', 'lock:order:1', 'holder-B', 'NX', 'EX', '60')
$current = Invoke-Redis $ReplicaName @('GET', 'lock:order:1')
Write-Host "B SETNX -> $secondResult"
Write-Host "新主 GET -> $current"

if ($secondResult -eq 'OK') {
    Write-Host ''
    Write-Host '结论：主从切换后锁丢失，A 与 B 会同时认为自己持有锁。' -ForegroundColor Yellow
    Write-Host '兜底：业务侧再用数据库唯一索引/条件更新做第二道防线（见 VoucherOrderServiceImpl）。' -ForegroundColor Yellow
} else {
    Write-Host '本次未能复现（复制/提升时序可能不同），可重跑。' -ForegroundColor Yellow
}

if (-not $Keep) {
    docker rm -f $MasterName $ReplicaName 2>$null | Out-Null
}
