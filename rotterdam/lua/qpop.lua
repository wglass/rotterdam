local scheduled_set, working_set, job_pool = unpack(KEYS)
local timestamp, cutoff, maxitems = unpack(ARGV)

redis.log(redis.LOG_DEBUG, "getting items, max: " .. maxitems)

local unique_keys = redis.call(
    "ZRANGEBYSCORE", scheduled_set, 0, cutoff, "LIMIT", 0, maxitems
)

redis.log(redis.LOG_DEBUG, "got " .. table.getn(unique_keys) .. " items")

if next(unique_keys) == nil then
    return {}
end

local zadd_args = {}
for i, unique_key in ipairs(unique_keys) do
    zadd_args[2 * i - 1] = timestamp
    zadd_args[2 * i] = unique_key
end

redis.call("ZREM", scheduled_set, unpack(unique_keys))
redis.call("ZADD", working_set, zadd_args)

return redis.call("HMGET", job_pool, unpack(unique_keys))
