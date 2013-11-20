local working_set, done_set, job_pool = unpack(KEYS)
local unique_keys = ARGV

if next(unique_keys) == nil then
    return {}
end

redis.call("SREM", working_set, unpack(unique_keys))
redis.call("SADD", done_set, unpack(unique_keys))
redis.call("HDEL", job_pool, unpack(unique_keys))

return 1
