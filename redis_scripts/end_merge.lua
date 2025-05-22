-- Get the parameters from KEYS
local database = KEYS[1]
local table = KEYS[2]
local index = KEYS[3]
local merge_id = KEYS[4]

-- Construct the merge key
local merge_key = "merge:" .. database .. ":" .. table .. ":" .. index .. ":processing"

-- Function to remove a merge plan by ID
local function remove_merge_plan(key, id)
    local items = redis.call("LRANGE", key, 0, -1)
    for i, item_json in ipairs(items) do
        local item = cjson.decode(item_json)
        if item.id == id then
            -- Remove the item from the list
            redis.call("LREM", key, 1, item_json)
            return true
        end
    end
    return false
end

-- Try to remove the merge plan
remove_merge_plan(merge_key, merge_id)

return ""