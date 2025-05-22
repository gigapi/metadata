-- Construct the merge key from the provided KEYS
local database = KEYS[1]
local table = KEYS[2]
local index = KEYS[3]
-- Timeout before we take an idle merge plan into process
local timeout_s = tonumber(KEYS[4])
local merge_key_base = "merge:" .. database .. ":" .. table .. ":" .. index
local merge_key_idle = merge_key_base .. ":idle"
local merge_key_processing = merge_key_base .. ":processing"

-- Get current time in seconds
local current_time = tonumber(redis.call("TIME")[1])

-- Function to process an idle item
local function process_idle_item()
    local merge_item_json = redis.call("LPOP", merge_key_idle)
    if merge_item_json then
        local merge_item = cjson.decode(merge_item_json)
        if tonumber(merge_item.time) + timeout_s < tonumber(current_time) then
            return false
        end
        merge_item.time = current_time
        local updated_item_json = cjson.encode(merge_item)

        -- Push the updated item to the processing list
        redis.call("RPUSH", merge_key_processing, updated_item_json)

        return updated_item_json
    end
    return false
end

-- Function to process a processing item
local function process_processing_item()
    local processing_item_json = redis.call("LINDEX", merge_key_processing, 0)

    if processing_item_json then
        local processing_item = cjson.decode(processing_item_json)

        -- Check if the first item in processing is older than the timeout
        if tonumber(current_time) - tonumber(processing_item.time) > timeout_s then
            -- Remove the item from the processing list
            redis.call("LPOP", merge_key_processing)

            -- Update the time and re-add to processing
            processing_item.time = current_time
            local updated_item_json = cjson.encode(processing_item)
            redis.call("RPUSH", merge_key_processing, updated_item_json)

            return updated_item_json
        end
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