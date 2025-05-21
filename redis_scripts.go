package metadata

const SCRIPT_PATCH_INDEX = `
-- Function to create and push a new merge object
local function create_and_push_new_merge(merge_key, path, size)
    local new_merge = cjson.encode({
        paths = {path},
        size = size
    })
    redis.call("RPUSH", merge_key, new_merge)
    return new_merge
end

local function hash_key(entry)
    local main_key = string.match(entry.path, "([^/]+)/.*")
    return "files:" .. entry.database .. ":" .. entry.table .. ":" .. main_key
end

local function delete_file(entry)
    -- Split the path into main key and hash field
    local main_key = hash_key(entry)
    
    if not main_key then
        return {success = false, error = "Invalid file path format for deletion: " .. entry.path}
    end
    
    -- Delete the hash field from the main key
    redis.call("HDEL", main_key, entry.path)
    return {success = true}
end

-- Function to process a single file
local function process_file(entry)
    -- Extract the index from the file path
    local path, index = string.match(entry.path, "(.+)/[^/]+%.(%d+)%.parquet$")

    if not index then
        return {success = false, error = "Invalid file path format: " .. entry.path}
    end

	local index_num = tonumber(index)
    if entry.cmd == "DELETE" then
        return {success = delete_file(entry)}
    end

    if index_num > #KEYS then
        return {success = true}
    end

    -- Create a Redis entry for the file
	local main_key = hash_key(entry)
    redis.call("HSET", main_key, entry.path, cjson.encode(entry))

    -- Get the last value from the merge list
    local merge_key = "merge:" .. entry.database .. ":" entry.table .. ":" .. index .. ":" .. path .. ":idle"
    local last_merge = redis.call("LINDEX", merge_key, -1)

    if not last_merge then
        -- Create and push a new merge object
        create_and_push_new_merge(merge_key, entry.path, entry.size_bytes)
        return {success = true}
    end

    -- Parse JSON from the last merge entry
    local last_merge_data = cjson.decode(last_merge)
    if last_merge_data.state ~= "idle" then
        -- Create and push a new merge object
        create_and_push_new_merge(merge_key, entry.path, entry.size_bytes)
        return {success = true}
    end

    if last_merge_data.size + entry.size_bytes > tonumber(KEYS[index_num]) then
        -- Create and push a new merge object
        create_and_push_new_merge(merge_key, entry.path, entry.size_bytes)
        return {success = true}
    end

    -- Update the last merge entry
    last_merge_data.size = last_merge_data.size + entry.size_bytes
    table.insert(last_merge_data.paths, entry.path)
    local updated_merge = cjson.encode(last_merge_data)
    redis.call("LSET", merge_key, -1, updated_merge)
    return {success = true}
end

-- Process all files
local results = {
    processed_count = 0
}

for i = 1, #ARGV do
    local entry = cjson.decode(ARGV[i])
    local result = process_file(entry)
    if result.success then
        results.processed_count = results.processed_count + 1
    else
		return redis.error_reply("Error processing file: ".. entry.path.. " - ".. result.error)
    end
end

return results.processed_count
`

const GET_MERGE_PLAN_SCRIPT = `
-- Construct the merge key from the provided KEYS
database = KEYS[1]
table = KEYS[2]
index = KEYS[3]
path = KEYS[4]
local merge_key = "merge:" .. database .. ":" .. table .. ":" .. index .. ":" .. path

-- Get the entire list for the merge key
local merge_list = redis.call("LRANGE", merge_key .. ":idle", 0, -1)

-- Iterate through the list to find the first 'idle' element
for i, merge_item_json in ipairs(merge_list) do
    local merge_item = cjson.decode(merge_item_json)
    
    if merge_item.state == "idle" then
        -- Change the state to 'merging'
        merge_item.state = "merging"
        
        -- Encode the updated item back to JSON
        local updated_item_json = cjson.encode(merge_item)
        
        -- Update the item in the list
        redis.call("LSET", merge_key, i - 1, updated_item_json)
        
        -- Return the updated item
        return updated_item_json
    end
end

-- If no 'idle' item was found, return nil
return nil
`
