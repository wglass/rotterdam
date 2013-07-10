local schedule, working_set, job_pool = unpack(KEYS)
local seconds, unique_key, payload = unpack(ARGV)

local scheduled_time = redis.call('ZSCORE', schedule, unique_key)
if not scheduled_time == nil then
    return nil
end
local working_score = redis.call('ZSCORE', working_set, unique_key)
if not working_score == nil then
    return nil
end

redis.call("ZADD", schedule, seconds, unique_key)
redis.call("HSET", job_pool, unique_key, payload)

return 1
