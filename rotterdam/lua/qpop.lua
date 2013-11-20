local scheduled_set, ready_set, job_pool = unpack(KEYS)
local timestamp, cutoff = unpack(ARGV)

local unique_keys = redis.call("ZRANGEBYSCORE", scheduled_set, 0, cutoff)

if next(unique_keys) == nil then
    return {}
end

local zadd_args = {}
for i, unique_key in ipairs(unique_keys) do
    zadd_args[2 * i - 1] = timestamp
    zadd_args[2 * i] = unique_key
end

redis.call("ZREMRANGEBYSCORE", scheduled_set, 0, cutoff)
redis.call("ZADD", ready_set, zadd_args)

return redis.call("HMGET", job_pool, unpack(unique_keys))
