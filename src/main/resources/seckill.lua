-- 秒杀下单原子脚本
-- ARGV[1] = voucherId, ARGV[2] = userId, ARGV[3] = orderId
-- 返回：0 成功；1 库存不足；2 重复下单；3 未开始；4 已结束
local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[3]

local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId
local timeKey = 'seckill:time:' .. voucherId

-- 1. 活动时间校验（epoch 毫秒，由应用启动/新增秒杀券时预热；key 不存在则跳过，兼容旧数据）
local times = redis.call('hmget', timeKey, 'begin', 'end')
if times[1] and times[2] then
    local t = redis.call('time')
    local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
    if now < tonumber(times[1]) then
        return 3
    end
    if now > tonumber(times[2]) then
        return 4
    end
end

-- 2. 库存校验（先判空：key 不存在时 tonumber(nil)=nil，不能直接比较）
local stock = tonumber(redis.call('get', stockKey))
if (not stock or stock <= 0) then
    return 1
end
-- 3. 一人一单
if (redis.call('sismember', orderKey, userId) == 1) then
    return 2
end
-- 4. 扣库存 + 记录下单用户
redis.call('incrby', stockKey, -1)
redis.call('sadd', orderKey, userId)
-- 5. 消息投递由 Java 侧完成：Lua 返回 0 后 KafkaTemplate 异步发送订单消息，
--    发送失败时执行 seckill_rollback.lua 回滚库存与去重标记。
return 0
