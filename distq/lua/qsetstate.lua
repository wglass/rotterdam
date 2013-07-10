local current_set, target_set = unpack(KEYS)
local current_time = ARGV[1]
table.remove(ARGV, 1)
local unique_keys = ARGV

local zadd_args = {}
for i, unique_key in ipairs(unique_keys) do
    zadd_args[2 * i - 1] = current_time
    zadd_args[2 * i] = unique_key
end

redis.call("ZREM", current_set, unpack(unique_keys))
redis.call("ZADD", target_set, unpack(zadd_args))
