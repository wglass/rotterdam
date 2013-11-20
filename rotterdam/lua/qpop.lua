local scheduled_set, ready_set, job_pool = unpack(KEYS)
local start, cutoff = unpack(ARGV)

local unique_keys = redis.call("ZRANGEBYSCORE", scheduled_set, start, cutoff)

if next(unique_keys) == nil then
    return {}
end

redis.call("ZREM", scheduled_set, unpack(unique_keys))
redis.call("SADD", ready_set, unpack(unique_keys))

return redis.call("HMGET", job_pool, unpack(unique_keys))
