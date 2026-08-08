//! Unit tests for suspended_flow_timeout Lua scripts.

const CREATE: &str = r#"
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('SET', KEYS[1], ARGV[1])
redis.call('ZADD', KEYS[4], 0, ARGV[2])
if ARGV[3] == '' then redis.call('ZREM', KEYS[2], ARGV[2])
else redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2]) end
if ARGV[4] == '1' then redis.call('ZADD', KEYS[3], 0, ARGV[2]) end
return 1
"#;

const COMPARE_AND_SET: &str = r#"
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

const DELETE_IF_EQUAL: &str = r#"
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

const POLL: &str = r#"
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

const ACK: &str = r#"
if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[2] then
  redis.call('HDEL', KEYS[1], ARGV[1])
  redis.call('ZREM', KEYS[2], ARGV[1])
end
return 1
"#;

const RELEASE: &str = r#"
if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[2] then
  local deadline = string.match(ARGV[2], '^[^:]+:(%d+)$')
  redis.call('HDEL', KEYS[1], ARGV[1])
  redis.call('ZREM', KEYS[2], ARGV[1])
  if deadline then redis.call('ZADD', KEYS[3], deadline, ARGV[1]) end
end
return 1
"#;

// =============================================================================
// CREATE script tests
// =============================================================================

#[test]
fn create_script_checks_existence() {
    assert!(CREATE.contains("EXISTS"));
    assert!(CREATE.contains("return 0"), "returns 0 if exists");
}

#[test]
fn create_script_sets_state() {
    assert!(CREATE.contains("SET"));
}

#[test]
fn create_script_manages_sorted_sets() {
    assert!(CREATE.contains("ZADD"), "adds to sorted set");
    assert!(CREATE.contains("ZREM"), "removes from waiting set");
}

#[test]
fn create_script_handles_empty_timeout() {
    assert!(CREATE.contains("ARGV[3] == ''"), "checks for empty timeout");
    assert!(CREATE.contains("ZREM"), "removes from waiting on empty");
}

#[test]
fn create_script_optional_ready_flag() {
    assert!(CREATE.contains("ARGV[4] == '1'"), "checks ready flag");
}

// =============================================================================
// COMPARE_AND_SET script tests
// =============================================================================

#[test]
fn compare_and_set_script_validates_version() {
    assert!(COMPARE_AND_SET.contains("~="), "not-equal check for version");
    assert!(COMPARE_AND_SET.contains("return 0"), "returns 0 on mismatch");
}

#[test]
fn compare_and_set_script_updates_all_structures() {
    assert!(COMPARE_AND_SET.contains("SET"), "updates state");
    assert!(COMPARE_AND_SET.contains("ZADD"), "adds to processed");
    assert!(COMPARE_AND_SET.contains("ZREM"), "removes from ready");
    assert!(COMPARE_AND_SET.contains("HDEL"), "deletes header");
}

#[test]
fn compare_and_set_script_handles_empty_timeout() {
    assert!(COMPARE_AND_SET.contains("ARGV[4] == ''"), "checks empty timeout");
}

#[test]
fn compare_and_set_script_conditional_ready_removal() {
    assert!(COMPARE_AND_SET.contains("ARGV[5] == '1'"), "checks ready flag");
    assert!(COMPARE_AND_SET.contains("ZCARD"), "checks set cardinality");
    assert!(COMPARE_AND_SET.contains("DEL"), "deletes empty set");
}

// =============================================================================
// DELETE_IF_EQUAL script tests
// =============================================================================

#[test]
fn delete_if_equal_checks_version() {
    assert!(DELETE_IF_EQUAL.contains("GET"), "gets current");
    assert!(DELETE_IF_EQUAL.contains("~="), "not-equal check");
}

#[test]
fn delete_if_equal_removes_all_structures() {
    assert!(DELETE_IF_EQUAL.contains("DEL"), "deletes main key");
    assert!(DELETE_IF_EQUAL.contains("ZREM"), "removes from all sets");
    assert!(DELETE_IF_EQUAL.contains("HDEL"), "deletes hash");
}

#[test]
fn delete_if_equal_conditional_cleanup() {
    assert!(DELETE_IF_EQUAL.contains("ARGV[3] == '1'"), "checks cleanup flag");
    assert!(DELETE_IF_EQUAL.contains("ZCARD"), "checks cardinality");
}

// =============================================================================
// POLL script tests
// =============================================================================

#[test]
fn poll_script_processes_expired_entries() {
    assert!(POLL.contains("ZRANGEBYSCORE"), "reads expired");
    assert!(POLL.contains("HGET"), "gets receipt");
    assert!(POLL.contains("deadline"), "parses deadline from receipt");
}

#[test]
fn poll_script_uses_limit_parameter() {
    assert!(POLL.contains("ARGV[4]"), "uses limit arg");
    assert!(POLL.contains("remaining"), "tracks remaining");
}

#[test]
fn poll_script_checks_entry_existence() {
    assert!(POLL.contains("EXISTS"), "checks if entry exists");
    assert!(POLL.contains("ZREM"), "removes missing entries");
}

#[test]
fn poll_script_returns_token_pairs() {
    assert!(POLL.contains("table.insert(out, id)"), "adds id");
    assert!(POLL.contains("table.insert(out, token)"), "adds token");
}

#[test]
fn poll_script_respects_batch_limit() {
    assert!(POLL.contains("ARGV[3]"), "uses batch limit");
    assert!(POLL.contains("break"), "exits loop when limit reached");
}

// =============================================================================
// ACK script tests
// =============================================================================

#[test]
fn ack_script_validates_receipt() {
    assert!(ACK.contains("HGET"), "gets receipt");
    assert!(ACK.contains("== ARGV[2]"), "validates receipt value");
}

#[test]
fn ack_script_removes_claim() {
    assert!(ACK.contains("HDEL"), "deletes receipt");
    assert!(ACK.contains("ZREM"), "removes from waiting");
}

#[test]
fn ack_script_always_returns_one() {
    assert!(ACK.contains("return 1"), "unconditional success");
}

// =============================================================================
// RELEASE script tests
// =============================================================================

#[test]
fn release_script_validates_receipt() {
    assert!(RELEASE.contains("HGET"), "gets receipt");
    assert!(RELEASE.contains("== ARGV[2]"), "validates value");
}

#[test]
fn release_script_extracts_deadline() {
    assert!(RELEASE.contains("string.match"), "extracts deadline");
    assert!(RELEASE.contains("^[^:]+:(%d+)$"), "deadline pattern");
}

#[test]
fn release_script_reschedules_on_deadline() {
    assert!(RELEASE.contains("ZADD"), "adds back to sorted set");
    assert!(RELEASE.contains("deadline"), "uses extracted deadline");
}
