-- 1 参数列表
-- 1.1 优惠卷id
local voucherId = ARGV[1]
-- 1.2 用户id
local userId = ARGV[2]
-- 1.3 订单id
local orderId = ARGV[3]

-- 2. 数据key
-- 2.1 库存key
local stockKey = 'seckill:stock:' .. voucherId
-- 2.2 订单key
local orderKey = 'seckill:order:' .. voucherId

-- 3. 脚本业务
-- 3.1 判断库存是否充足 get stockKey（先判空：key 不存在时 tonumber(nil)=nil，不能直接比较）
local stock = tonumber(redis.call('get', stockKey))
if(not stock or stock <= 0) then
    --3.2库存不足，返回1
    return 1
end
-- 3.2 判断用户下单
if(redis.call('sismember',orderKey,userId)==1) then
    -- 3.3 存在，说明重复下单
    return 2
end
-- 3.4 扣库存
redis.call('incrby',stockKey,-1)
-- 3.5 下单（保存用户）
redis.call('sadd',orderKey,userId)
-- 3.6 消息投递由 Java 侧完成：
-- Lua 返回 0 后，VoucherOrderServiceImpl 通过 KafkaTemplate 异步发送订单消息；
-- 发送失败时执行 seckill_rollback.lua 回滚库存与去重标记。
-- （原 Redis Stream 的 xadd 已删除，避免与 Kafka 双写导致订单重复创建）
return 0
