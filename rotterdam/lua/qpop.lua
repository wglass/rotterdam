local function log(msg)
    redis.log(redis.LOG_DEBUG, msg)
end

local function sortbyvalue(t)
    local keys = {}
    for key in pairs(t) do keys[#keys+1] = key end

    table.sort(keys, function(a, b) return t[a] < t[b] end)

    local i = 0
    return function()
        i = i + 1
        if keys[i] then
            return keys[i], t[keys[i]]
        end
    end
end

local function getkeysandscores(schedule_set, cutoff_time, item_limit)
    return redis.call(
        "ZRANGEBYSCORE", schedule_set, 0, cutoff_time,
        "WITHSCORES", "LIMIT", 0, item_limit
    )
end

local function getjobs(sched_set, work_set, pool, unique_keys, tstamp)
    if #unique_keys == 0 then
        return {}
    end

    local zadd_args = {}
    for i, unique_key in ipairs(unique_keys) do
        zadd_args[2 * i - 1] = tstamp
        zadd_args[2 * i] = unique_key
    end

    local num_removed = redis.call("ZREM", sched_set, unpack(unique_keys))

    if num_removed == 0 then
        return {}
    end

    redis.call("ZADD", work_set, zadd_args)

    return redis.call("HMGET", pool, unpack(unique_keys))
end

local timestamp, cutoff, maxitems = unpack(ARGV)

local keys_by_queue = {}
local uniques = {}
for i = 1, #KEYS, 3 do
    local scheduled_set = KEYS[i]

    local keys_and_scores = getkeysandscores(scheduled_set, cutoff, maxitems)

    for i = 1, #keys_and_scores, 2 do
        uniques[keys_and_scores[i]] = keys_and_scores[i+1]
    end
end

local keys = {}
local count = 0
for key, score in sortbyvalue(uniques) do
    if count < tonumber(maxitems) then
        keys[#keys+1] = key
        count = count + 1
    end
end

local payloads = {}

for i = 1, #KEYS, 3 do
    local scheduled_set = KEYS[i]
    local working_set = KEYS[i+1]
    local job_pool = KEYS[i+2]

    local jobs = getjobs(scheduled_set, working_set, job_pool, keys, timestamp)

    for index, payload in pairs(jobs) do
        if payload ~= false then
            payloads[#payloads+1] = payload
        end
    end
end

return payloads
