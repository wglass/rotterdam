local ready_set, working_set, job_pool = unpack(KEYS)
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

redis.call("ZREM", ready_set, unpack(unique_keys))
redis.call("ZADD", working_set, zadd_args)

return 1
