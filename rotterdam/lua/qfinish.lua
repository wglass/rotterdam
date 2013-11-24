local working_set, done_set, job_pool = unpack(KEYS)
local timestamp = ARGV[1]
table.remove(ARGV, 1)
local unique_keys = ARGV

if next(unique_keys) == nil then
    return {}
end

local zadd_args = {}
for i, unique_key in ipairs(unique_keys) do
    zadd_args[2 * i - 1] = timestamp
    zadd_args[2 * i] = unique_key
end

redis.call("ZREM", working_set, unpack(unique_keys))
redis.call("ZADD", done_set, zadd_args)
redis.call("HDEL", job_pool, unpack(unique_keys))

return 1
