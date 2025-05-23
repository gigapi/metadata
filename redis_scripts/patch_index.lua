-- Function to generate a pseudo-UUID
local function generate_uuid()
    local random = math.random
    local template ='xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'
    return string.gsub(template, '[xy]', function (c)
        local v = (c == 'x') and random(0, 0xf) or random(8, 0xb)
        return string.format('%x', v)
    end)
end

-- Function to get the directory path from a full path
local function get_dir(path)
    return string.match(path, "(.+)/[^/]+$")
end

-- Function to create and push a new merge object
local function create_and_push_new_merge(merge_key, path, size)
	local current_time = redis.call("TIME")[1]
    local new_merge = cjson.encode({
        id = generate_uuid(),
        time = current_time,
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

    redis.call("HDEL", main_key, entry.path)
    local dir = get_dir(entry.path)
    local files_cnt = redis.call("HINCRBY", "folders:" .. entry.database .. ":" .. entry.table, dir, -1)
    if files_cnt == 0 then
        redis.call("HDEL", "folders:" .. entry.database .. ":" .. entry.table, dir)
    end
    
    local folders_cnt = redis.call("HLEN", "folders:" .. entry.database .. ":" .. entry.table)
    if folders_cnt == 0 then
        redis.call("DEL", "folders:" .. entry.database .. ":" .. entry.table)
    end

    return {success = true}
end

-- Function to process a single file
local function process_file(entry)
    if entry.cmd == "DELETE" then
        return delete_file(entry)
    end
    -- Extract the index from the file path
    local path, index = string.match(entry.path, "(.+)/[^/]+%.(%d+)%.parquet$")

    if not index then
        return {success = false, error = "Invalid file path format: " .. entry.path}
    end

	local index_num = tonumber(index)

    local dir = get_dir(entry.path)
    redis.call("HINCRBY", "folders:" .. entry.database .. ":" .. entry.table, dir, 1)

    -- Create a Redis entry for the file
	local main_key = hash_key(entry)
    redis.call("HSET", main_key, entry.path, cjson.encode(entry))

    if index_num > #KEYS then
        return {success = true}
    end

    -- Get the last value from the merge list
    local merge_key = "merge:" .. entry.database .. ":" .. entry.table .. ":" .. index .. ":idle"
    local last_merge = redis.call("LINDEX", merge_key, -1)

    if not last_merge then
        -- Create and push a new merge object
        create_and_push_new_merge(merge_key, entry.path, entry.size_bytes)
        return {success = true}
    end

    -- Parse JSON from the last merge entry
    local last_merge_data = cjson.decode(last_merge)

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