local schedule, job_pool = unpack(KEYS)
local start, cutoff = unpack(ARGV)

local unique_keys = redis.call("ZRANGEBYSCORE", schedule, start, cutoff)

if next(unique_keys) == nil then
    return {}
end

return redis.call("HMGET", job_pool, unpack(unique_keys))
