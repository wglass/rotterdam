local scheduled_set, ready_set, working_set, job_pool = unpack(KEYS)
local when_to_fire, unique_key, payload = unpack(ARGV)

local scheduled_time = redis.call('ZSCORE', scheduled_set, unique_key)
if not scheduled_time == nil then
    return nil
end

if not redis.call("SISMEMBER", ready_set, unique_key) == nil then
    return nil
end
if not redis.call("SISMEMBER", working_set, unique_key) == nil then
    return nil
end

redis.call("ZADD", scheduled_set, when_to_fire, unique_key)
redis.call("HSET", job_pool, unique_key, payload)

return 1
