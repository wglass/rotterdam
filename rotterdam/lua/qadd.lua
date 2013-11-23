local scheduled_set, ready_set, working_set, job_pool = unpack(KEYS)
local timestamp, when_to_fire, unique_key, payload = unpack(ARGV)

if redis.call("ZSCORE", scheduled_set, unique_key) ~= false then
    return nil
end
if redis.call("ZSCORE", ready_set, unique_key) ~= false then
    return nil
end
if redis.call("ZSCORE", working_set, unique_key) ~= false then
    return nil
end

redis.call("ZADD", scheduled_set, when_to_fire, unique_key)
redis.call("HSET", job_pool, unique_key, payload)

return 1
