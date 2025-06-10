local merge_conf = cjson.decode(KEYS[1])
local move_conf = cjson.decode(KEYS[2])

math.randomseed(tonumber(redis.call('TIME')[1]) * 1000 +
        tonumber(redis.call('TIME')[2]) / 1000) -- Seed the random number generator with the current time

-- Function to generate a pseudo-UUID
local function generate_uuid()
    local template ='xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'
    return string.gsub(template, '[xy]', function (c)
        local v = (c == 'x') and math.random(0, 0xf) or math.random(8, 0xb)
        return string.format('%x', v)
    end)
end

-- Function to get the directory path from a full path
local function get_dir(path)
    return string.match(path, "(.+)/[^/]+$")
end

-- Function to create and push a new merge object
local function create_and_push_new_merge(merge_key, path, size, index)
	local current_time = tonumber(redis.call("TIME")[1])
    local merge_ttl_s = merge_conf[index][1]
    local new_merge = cjson.encode({
        id = generate_uuid(),
        time_s = current_time + merge_ttl_s,
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

    local drop_queue_key = "drop:" .. entry.database .. ":".. entry.table .. ":".. entry.layer .. ":"..
            entry.writer_id .. ":idle"
    local new_drop = cjson.encode({
        id = generate_uuid(),
        writer_id = entry.writer_id,
        layer = entry.layer,
        path = entry.path,
        database = entry.database,
        table = entry.table,
        time_s = tonumber(redis.call("TIME")[1]) + 30
    })
    redis.call("RPUSH", drop_queue_key, new_drop)

    return {success = true}
end

local function merge_entry(entry, index)
    local dir = get_dir(entry.path)
    local merge_key = "merge:" .. entry.database .. ":" .. entry.table .. ":" .. index .. ":" .. dir .. ":" .. entry.layer .. ":" .. entry.writer_id .. ":idle"
    local last_merge = redis.call("LINDEX", merge_key, -1)

    if not last_merge then
        -- Create and push a new merge object
        create_and_push_new_merge(merge_key, entry.path, entry.size_bytes, index)
        return {success = true}
    end

    -- Parse JSON from the last merge entry
    local last_merge_data = cjson.decode(last_merge)

    if last_merge_data.size + entry.size_bytes > tonumber(merge_conf[index][2]) then
        -- Create and push a new merge object
        create_and_push_new_merge(merge_key, entry.path, entry.size_bytes, index)
        return {success = true}
    end

    -- Update the last merge entry
    last_merge_data.size = last_merge_data.size + entry.size_bytes
    table.insert(last_merge_data.paths, entry.path)
    local updated_merge = cjson.encode(last_merge_data)
    redis.call("LSET", merge_key, -1, updated_merge)
    return {success = true}
end

local function move_entry(entry)
    local move_key = "move:".. entry.database.. ":".. entry.table.. ":".. entry.layer .. ":".. entry.writer_id .. ":idle"

    -- Extract parts of the path
    local folder, uuid, iteration = string.match(entry.path, "(.+)/([^/.]+)%.(%d+)%.parquet$")

    -- Generate a new UUID for the destination path
    local new_uuid = generate_uuid()

    -- Construct the new path
    local path_to = folder .. "/" .. new_uuid .. "." .. iteration .. ".parquet"
    local move_entry = {
        id = generate_uuid(),
        writer_id = entry.writer_id,
        database = entry.database,
        table = entry.table,
        path_from = entry.path,
        layer_from = entry.layer,
        path_to = path_to,
        layer_to = move_conf[entry.layer].layer_to,
        time_s = entry.time_s
    }
    redis.call("RPUSH", move_key, cjson.encode(move_entry))
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

    local merge_ttl = -1
    local move_ttl = -1
    if index_num <= #merge_conf then
         merge_ttl = merge_conf[index_num][1]
    end
    if move_conf[entry.layer].ttl_sec > 0 then
        move_ttl = move_conf[entry.layer].ttl_sec
    end

    if merge_ttl ~= -1 and move_ttl == -1 then
        return merge_entry(entry, index_num)
    end
    if move_ttl ~= -1 and merge_ttl == -1 then
        return move_entry(entry)
    end
    if merge_ttl == -1 and move_ttl == -1 then
        return {success = true}
    end

    local chunk_time_s = tonumber(entry.str_chunk_time) / 1000000000
    local merge_time_s = chunk_time_s + tonumber(merge_conf[index_num][1])
    local move_time_s = chunk_time_s + move_conf[entry.layer].ttl_sec

    if move_conf[entry.layer].ttl_sec == 0 or merge_time_s <= move_time_s then
        entry.time_s = merge_time_s
        return merge_entry(entry, index_num)
    end
    entry.time_s = move_time_s
    return move_entry(entry)
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