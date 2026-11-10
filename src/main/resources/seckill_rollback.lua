-- Kafka 消息投递失败时的补偿脚本：回滚库存、移除去重标记
-- ARGV[1] = voucherId, ARGV[2] = userId
local voucherId = ARGV[1]
local userId = ARGV[2]

local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId

redis.call('incrby', stockKey, 1)
redis.call('srem', orderKey, userId)
return 0
