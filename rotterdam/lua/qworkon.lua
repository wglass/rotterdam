local ready_set, working_set, job_pool = unpack(KEYS)
local unique_keys = ARGV

if next(unique_keys) == nil then
    return {}
end

redis.call("SREM", ready_set, unpack(unique_keys))
redis.call("SADD", working_set, unpack(unique_keys))

return 1
