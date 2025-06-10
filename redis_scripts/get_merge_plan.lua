-- Construct the merge key from the provided KEYS
local prefix = KEYS[1]
local database = KEYS[2]
local table = KEYS[3]
local suffix = KEYS[4]
local layer = KEYS[5]
local writer_id = KEYS[6]

if suffix ~= "" then
    suffix = suffix .. ":"
end
local merge_key_base = prefix .. ":" .. database .. ":" .. table .. ":" .. suffix .. layer .. ":" .. writer_id
local merge_key_idle = merge_key_base .. ":idle"
local merge_key_processing = merge_key_base .. ":processing"

-- Get current time in seconds
local current_time = tonumber(redis.call("TIME")[1])

-- Function to process an idle item
local function process_idle_item()
    local merge_item_json = redis.call("LPOP", merge_key_idle)
    if merge_item_json then
        local merge_item = cjson.decode(merge_item_json)
        if merge_item.time_s > current_time then
            redis.call("LPUSH", merge_key_idle, merge_item_json)
            return false
        end
        merge_item.time_s = current_time + 1800 -- 30m timeout to reprocess the dead items
        local updated_item_json = cjson.encode(merge_item)

        -- Push the updated item to the processing list
        redis.call("RPUSH", merge_key_processing, updated_item_json)

        return updated_item_json
    end
    return false
end

-- Function to process a processing item
local function process_processing_item()
    local merge_item_json = redis.call("LPOP", merge_key_processing)
    if merge_item_json then
        local merge_item = cjson.decode(merge_item_json)
        if merge_item.time_s > current_time then
            redis.call("LPUSH", merge_key_processing, merge_item_json)
            return false
        end
        merge_item.time_s = current_time + 1800 -- 30m timeout to reprocess the dead items
        local updated_item_json = cjson.encode(merge_item)

        -- Push the updated item to the processing list
        redis.call("RPUSH", merge_key_processing, updated_item_json)

        return updated_item_json
    end
    return false
end

-- Try to process an idle item first
local result = process_idle_item()
if result then
    return result
end

-- If no idle item, try to process a processing item
result = process_processing_item()
if result then
    return result
end

-- If no item was processed, return nil
return ""