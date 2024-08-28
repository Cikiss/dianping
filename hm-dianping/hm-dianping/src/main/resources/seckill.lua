-- 1.参数列表
-- 1.1.优惠券id
local voucherId = ARGV[1]
-- 1.2.用户id
local userId = ARGV[2]
-- 1.3.订单id
local orderId = ARGV[3]

-- 2.数据key
-- 2.1.库存key
local stockKey = 'seckill:stock:' .. voucherId
-- 2.2.订单key
local orderKey = 'seckill:order:' .. voucherId

-- 3.脚本业务
-- 3.1.判断库存是否充足 get stockKey
if tonumber(redis.call('get', stockKey)) <= 0 then
    -- 3.1.1.库存不足，返回1
    return 1
end
-- 3.2.判断用户是否下单
if redis.call('sismember', orderKey, userId) == 1 then
    -- 3.3.存在，重复下单，返回2
    return 2
end
-- 3.4.扣库存
redis.call('incrby', stockKey, -1)
-- 3.5.下单（保存用户）
redis.call('sadd', orderKey, userId)
-- 3.6.向stream.orders中添加消息
redis.call('xadd', 'stream.orders', '*', 'userId', userId, 'voucherId', voucherId, 'id', orderId)
--redis.call('xgroup', 'create', 'stream.orders', 'g1', '0', 'mkstream')
return 0