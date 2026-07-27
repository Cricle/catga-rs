pub(crate) const CREATE: &str = r#"
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('SET', KEYS[1], ARGV[1])
redis.call('ZADD', KEYS[4], 0, ARGV[2])
if ARGV[3] == '' then redis.call('ZREM', KEYS[2], ARGV[2])
else redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2]) end
if ARGV[4] == '1' then redis.call('ZADD', KEYS[3], 0, ARGV[2]) end
return 1
"#;

pub(crate) const COMPARE_AND_SET: &str = r#"
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[1], ARGV[2])
redis.call('ZADD', KEYS[7], 0, ARGV[3])
redis.call('ZREM', KEYS[3], ARGV[3])
redis.call('HDEL', KEYS[4], ARGV[3])
if ARGV[4] == '' then redis.call('ZREM', KEYS[2], ARGV[3])
else redis.call('ZADD', KEYS[2], ARGV[4], ARGV[3]) end
if ARGV[5] == '1' then
  redis.call('ZREM', KEYS[5], ARGV[3])
  if redis.call('ZCARD', KEYS[5]) == 0 then redis.call('DEL', KEYS[5]) end
end
if ARGV[6] == '1' then redis.call('ZADD', KEYS[6], 0, ARGV[3]) end
return 1
"#;

pub(crate) const DELETE_IF_EQUAL: &str = r#"
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[6], ARGV[2])
redis.call('ZREM', KEYS[2], ARGV[2])
redis.call('ZREM', KEYS[3], ARGV[2])
redis.call('HDEL', KEYS[4], ARGV[2])
if ARGV[3] == '1' then
  redis.call('ZREM', KEYS[5], ARGV[2])
  if redis.call('ZCARD', KEYS[5]) == 0 then redis.call('DEL', KEYS[5]) end
end
return 1
"#;

pub(crate) const POLL: &str = r#"
local inspected = 0
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[2], 'LIMIT', 0, ARGV[4])
for _, id in ipairs(expired) do
  local receipt = redis.call('HGET', KEYS[3], id)
  if receipt then
    local deadline = string.match(receipt, '^[^:]+:(%d+)$')
    if deadline then redis.call('ZADD', KEYS[1], deadline, id) end
  end
  redis.call('HDEL', KEYS[3], id)
  redis.call('ZREM', KEYS[2], id)
  inspected = inspected + 1
end
local remaining = tonumber(ARGV[4]) - inspected
if remaining <= 0 then return {} end
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[2], 'LIMIT', 0, remaining)
local out = {}
for _, id in ipairs(ids) do
  if redis.call('EXISTS', ARGV[1] .. ':' .. id) == 0 then
    redis.call('ZREM', KEYS[1], id)
  else
    local deadline = math.floor(tonumber(redis.call('ZSCORE', KEYS[1], id)))
    local token = redis.call('INCR', KEYS[4]) .. ':' .. deadline
    redis.call('ZREM', KEYS[1], id)
    redis.call('HSET', KEYS[3], id, token)
    redis.call('ZADD', KEYS[2], ARGV[5], id)
    table.insert(out, id)
    table.insert(out, token)
    if (#out / 2) >= tonumber(ARGV[3]) then break end
  end
end
return out
"#;

pub(crate) const ACK: &str = r#"
if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[2] then
  redis.call('HDEL', KEYS[1], ARGV[1])
  redis.call('ZREM', KEYS[2], ARGV[1])
end
return 1
"#;

pub(crate) const RELEASE: &str = r#"
if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[2] then
  local deadline = string.match(ARGV[2], '^[^:]+:(%d+)$')
  redis.call('HDEL', KEYS[1], ARGV[1])
  redis.call('ZREM', KEYS[2], ARGV[1])
  if deadline then redis.call('ZADD', KEYS[3], deadline, ARGV[1]) end
end
return 1
"#;
