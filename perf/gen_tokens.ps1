<#
.SYNOPSIS
    批量生成压测登录态：从 tb_user 取用户，写入 Redis login:token:*，输出 JMeter CSV。

.DESCRIPTION
    等价于根目录 gen_tokens.py，但只依赖 PowerShell + Docker，Windows 环境无需 Python。
    默认连接 compose 中的 hmdp-mysql / hmdp-redis 容器。

.EXAMPLE
    pwsh perf/gen_tokens.ps1 -Count 1000
#>
param(
    [int]$Count = 1000,
    [string]$MysqlContainer = 'hmdp-mysql',
    [string]$RedisContainer = 'hmdp-redis',
    [string]$DbUser = 'root',
    [string]$DbPassword = '123456',
    [string]$Database = 'hmdp',
    [string]$OutFile = (Join-Path $PSScriptRoot '..\token.txt')
)

$ErrorActionPreference = 'Stop'

$sql = "SELECT id, nick_name, icon FROM tb_user ORDER BY id LIMIT $Count"
$rows = docker exec $MysqlContainer mysql -N -B "-u$DbUser" "-p$DbPassword" $Database -e $sql
if (-not $rows) {
    throw 'tb_user 表为空，无法生成 token'
}

$redisCommands = [System.Collections.Generic.List[string]]::new()
$tokenLines = [System.Collections.Generic.List[string]]::new()

foreach ($row in $rows) {
    if ([string]::IsNullOrWhiteSpace($row)) { continue }
    $parts = $row -split "`t", 3
    $userId = $parts[0].Trim()
    $nickName = if ($parts.Count -gt 1) { $parts[1] -replace '"', '\"' } else { '' }
    $icon = if ($parts.Count -gt 2) { $parts[2] -replace '"', '\"' } else { '' }

    $token = [guid]::NewGuid().ToString()
    $key = "login:token:$token"
    $redisCommands.Add("HSET $key id $userId nickName `"$nickName`" icon `"$icon`"")
    $redisCommands.Add("EXPIRE $key 2160000")   # 25 天，与 LOGIN_USER_TTL 一致
    $tokenLines.Add("$token,$userId")
}

$redisCommands -join "`n" | docker exec -i $RedisContainer redis-cli | Out-Null

$resolvedOut = [System.IO.Path]::GetFullPath($OutFile)
$tokenLines -join "`n" | Set-Content -Path $resolvedOut -Encoding ascii
Write-Host "已生成 $($tokenLines.Count) 个 token -> $resolvedOut"
