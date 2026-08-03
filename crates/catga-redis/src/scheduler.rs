//! Redis-backed durable flow-resume scheduling.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use catga_core::flow::{DueFlowScheduler, FlowScheduler, ScheduledResume};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use redis::{Script, aio::ConnectionManager};
use uuid::Uuid;

use crate::transport::map_error;

const SCHEDULE: &str = r#"
local existing=redis.call('HGET', KEYS[3], ARGV[1])
if existing then
  local existing_target=redis.call('HGET', ARGV[6]..existing, 'target')
  if existing_target == ARGV[1] then return existing end
  redis.call('HDEL', KEYS[3], ARGV[1])
end
redis.call('HSET', KEYS[3], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[1], 'flow_id', ARGV[3], 'state_id', ARGV[4], 'due_at', ARGV[5], 'target', ARGV[1], 'owner', '', 'lease_until', '0')
redis.call('ZADD', KEYS[2], ARGV[5], ARGV[2])
return ARGV[2]
"#;
const CANCEL: &str = r#"
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
local owner=redis.call('HGET', KEYS[1], 'owner')
local lease_until=tonumber(redis.call('HGET', KEYS[1], 'lease_until') or '0')
if owner ~= '' and lease_until > tonumber(ARGV[2]) then return 0 end
local target=redis.call('HGET', KEYS[1], 'target')
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
if target and redis.call('HGET', KEYS[4], target) == ARGV[1] then redis.call('HDEL', KEYS[4], target) end
return 1
"#;
const CLAIM: &str = r#"
local expired=redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[3], 'LIMIT', 0, ARGV[5])
for _,id in ipairs(expired) do
  local record=ARGV[1]..id
  if redis.call('EXISTS', record) == 0 then
    redis.call('ZREM', KEYS[2], id)
  elseif tonumber(redis.call('HGET', record, 'lease_until') or '0') <= tonumber(ARGV[3]) then
    local due=redis.call('HGET', record, 'due_at')
    redis.call('HSET', record, 'owner', '', 'lease_until', '0')
    redis.call('ZREM', KEYS[2], id)
    if due then redis.call('ZADD', KEYS[1], due, id) else redis.call('DEL', record) end
  end
end
local ids=redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[3], 'LIMIT', 0, ARGV[5])
local out={}
for _,id in ipairs(ids) do
  local record=ARGV[1]..id
  local flow=redis.call('HGET', record, 'flow_id')
  local state=redis.call('HGET', record, 'state_id')
  local due=redis.call('HGET', record, 'due_at')
  if flow and state and due then
    redis.call('ZREM', KEYS[1], id)
    redis.call('HSET', record, 'owner', ARGV[2], 'lease_until', ARGV[4])
    redis.call('ZADD', KEYS[2], ARGV[4], id)
    table.insert(out, id)
    table.insert(out, flow)
    table.insert(out, state)
    table.insert(out, due)
  else
    redis.call('ZREM', KEYS[1], id)
    redis.call('DEL', record)
  end
end
return out
"#;
const ACK: &str = r#"
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[2])
redis.call('ZREM', KEYS[3], ARGV[2])
if redis.call('HGET', KEYS[4], ARGV[3]) == ARGV[2] then redis.call('HDEL', KEYS[4], ARGV[3]) end
return 1
"#;
const RELEASE: &str = r#"
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[1] then return 0 end
local due=redis.call('HGET', KEYS[1], 'due_at')
if not due then return 0 end
redis.call('HSET', KEYS[1], 'owner', '', 'lease_until', '0')
redis.call('ZREM', KEYS[2], ARGV[2])
redis.call('ZADD', KEYS[3], due, ARGV[2])
return 1
"#;
const RENEW: &str = r#"
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[1] then return 0 end
if tonumber(redis.call('HGET', KEYS[1], 'lease_until') or '0') <= tonumber(ARGV[3]) then return 0 end
redis.call('HSET', KEYS[1], 'lease_until', ARGV[4])
redis.call('ZADD', KEYS[2], ARGV[4], ARGV[2])
return 1
"#;

/// A bounded, at-least-once flow scheduler backed by Redis sorted sets and Lua transitions.
///
/// The scheduler keeps ready work and leased work in separate sorted sets. Claiming moves at
/// most `limit` entries between those sets in one Redis script; expired leases are reclaimed in
/// the same bounded operation. A per-target index prevents duplicate resumes for one suspended
/// flow state.
pub struct RedisFlowScheduler {
    connection: ConnectionManager,
    prefix: Box<str>,
}

impl RedisFlowScheduler {
    /// Connects to Redis and namespaces scheduler keys beneath `prefix`.
    pub async fn connect(
        server: impl AsRef<str>,
        prefix: impl Into<Box<str>>,
    ) -> CatgaResult<Self> {
        let client = redis::Client::open(server.as_ref()).map_err(map_error)?;
        let connection = client
            .get_connection_manager_with_config(crate::config::command_connection_manager_config())
            .await
            .map_err(map_error)?;
        Ok(Self {
            connection,
            prefix: prefix.into(),
        })
    }

    fn record_key(&self, schedule_id: &str) -> String {
        format!("{}:schedule:{schedule_id}", self.prefix)
    }

    fn record_prefix(&self) -> String {
        format!("{}:schedule:", self.prefix)
    }

    fn due_key(&self) -> String {
        format!("{}:due", self.prefix)
    }

    fn leased_key(&self) -> String {
        format!("{}:leased", self.prefix)
    }

    fn targets_key(&self) -> String {
        format!("{}:targets", self.prefix)
    }
}

#[async_trait]
impl FlowScheduler for RedisFlowScheduler {
    async fn schedule_resume(
        &self,
        flow_id: &str,
        state_id: &str,
        due_at: SystemTime,
    ) -> CatgaResult<Box<str>> {
        let due_at = unix_millis(due_at)?;
        let target = target_key(flow_id, state_id)?;
        let schedule_id: Box<str> = Uuid::new_v4().to_string().into();
        let mut connection = self.connection.clone();
        let existing_or_new: String = Script::new(SCHEDULE)
            .key(self.record_key(&schedule_id))
            .key(self.due_key())
            .key(self.targets_key())
            .arg(target)
            .arg(&*schedule_id)
            .arg(flow_id)
            .arg(state_id)
            .arg(due_at)
            .arg(self.record_prefix())
            .invoke_async(&mut connection)
            .await
            .map_err(map_error)?;
        Ok(existing_or_new.into_boxed_str())
    }

    async fn cancel_resume(&self, schedule_id: &str) -> CatgaResult<bool> {
        let key = self.record_key(schedule_id);
        let now = unix_millis(SystemTime::now())?;
        let mut connection = self.connection.clone();
        let cancelled: i64 = Script::new(CANCEL)
            .key(key)
            .key(self.due_key())
            .key(self.leased_key())
            .key(self.targets_key())
            .arg(schedule_id)
            .arg(now)
            .invoke_async(&mut connection)
            .await
            .map_err(map_error)?;
        Ok(cancelled == 1)
    }
}

#[async_trait]
impl DueFlowScheduler for RedisFlowScheduler {
    async fn claim_due(
        &self,
        owner: &str,
        now: SystemTime,
        lease_for: Duration,
        limit: usize,
    ) -> CatgaResult<Vec<ScheduledResume>> {
        if lease_for.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "due-work lease duration must be greater than zero",
            ));
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        let now = unix_millis(now)?;
        let lease_until = now
            .checked_add(duration_millis(lease_for)?)
            .ok_or_else(|| {
                CatgaError::new(ErrorCode::Validation, "due-work lease exceeds Unix time")
            })?;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let mut connection = self.connection.clone();
        let values: Vec<String> = Script::new(CLAIM)
            .key(self.due_key())
            .key(self.leased_key())
            .arg(self.record_prefix())
            .arg(owner)
            .arg(now)
            .arg(lease_until)
            .arg(limit)
            .invoke_async(&mut connection)
            .await
            .map_err(map_error)?;
        values
            .chunks_exact(4)
            .map(|fields| {
                Ok(ScheduledResume::new(
                    fields[0].clone(),
                    fields[1].clone(),
                    fields[2].clone(),
                    from_unix_millis(&fields[3])?,
                ))
            })
            .collect()
    }

    async fn ack_due(&self, owner: &str, schedule_id: &str) -> CatgaResult<bool> {
        let key = self.record_key(schedule_id);
        let target: Option<Vec<u8>> = {
            use redis::AsyncCommands;
            let mut connection = self.connection.clone();
            connection.hget(&key, "target").await.map_err(map_error)?
        };
        let Some(target) = target else {
            return Ok(false);
        };
        let mut connection = self.connection.clone();
        let acknowledged: i64 = Script::new(ACK)
            .key(key)
            .key(self.due_key())
            .key(self.leased_key())
            .key(self.targets_key())
            .arg(owner)
            .arg(schedule_id)
            .arg(target)
            .invoke_async(&mut connection)
            .await
            .map_err(map_error)?;
        Ok(acknowledged == 1)
    }

    async fn release_due(&self, owner: &str, schedule_id: &str) -> CatgaResult<bool> {
        let mut connection = self.connection.clone();
        let released: i64 = Script::new(RELEASE)
            .key(self.record_key(schedule_id))
            .key(self.leased_key())
            .key(self.due_key())
            .arg(owner)
            .arg(schedule_id)
            .invoke_async(&mut connection)
            .await
            .map_err(map_error)?;
        Ok(released == 1)
    }

    async fn renew_due(
        &self,
        owner: &str,
        schedule_id: &str,
        now: SystemTime,
        lease_for: Duration,
    ) -> CatgaResult<bool> {
        if lease_for.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "due-work lease duration must be greater than zero",
            ));
        }
        let lease_until = unix_millis(now)?
            .checked_add(duration_millis(lease_for)?)
            .ok_or_else(|| {
                CatgaError::new(ErrorCode::Validation, "due-work lease exceeds Unix time")
            })?;
        let mut connection = self.connection.clone();
        let renewed: i64 = Script::new(RENEW)
            .key(self.record_key(schedule_id))
            .key(self.leased_key())
            .arg(owner)
            .arg(schedule_id)
            .arg(unix_millis(now)?)
            .arg(lease_until)
            .invoke_async(&mut connection)
            .await
            .map_err(map_error)?;
        Ok(renewed == 1)
    }
}

fn target_key(flow_id: &str, state_id: &str) -> CatgaResult<Vec<u8>> {
    let flow_len = u64::try_from(flow_id.len()).map_err(|_| {
        CatgaError::new(
            ErrorCode::Validation,
            "flow identifier is too long for Redis",
        )
    })?;
    let state_len = u64::try_from(state_id.len()).map_err(|_| {
        CatgaError::new(
            ErrorCode::Validation,
            "state identifier is too long for Redis",
        )
    })?;
    let mut key = Vec::with_capacity(16 + flow_id.len() + state_id.len());
    key.extend_from_slice(&flow_len.to_be_bytes());
    key.extend_from_slice(flow_id.as_bytes());
    key.extend_from_slice(&state_len.to_be_bytes());
    key.extend_from_slice(state_id.as_bytes());
    Ok(key)
}

fn unix_millis(value: SystemTime) -> CatgaResult<i64> {
    let elapsed = value
        .duration_since(UNIX_EPOCH)
        .map_err(|_| CatgaError::new(ErrorCode::Validation, "due time precedes the Unix epoch"))?;
    i64::try_from(elapsed.as_millis())
        .map_err(|_| CatgaError::new(ErrorCode::Validation, "due time exceeds Redis range"))
}

fn duration_millis(value: Duration) -> CatgaResult<i64> {
    i64::try_from(value.as_millis())
        .map_err(|_| CatgaError::new(ErrorCode::Validation, "lease duration exceeds Redis range"))
}

fn from_unix_millis(value: &str) -> CatgaResult<SystemTime> {
    let millis = value.parse::<u64>().map_err(|_| {
        CatgaError::new(
            ErrorCode::Internal,
            "Redis scheduler stored an invalid due time",
        )
    })?;
    UNIX_EPOCH
        .checked_add(Duration::from_millis(millis))
        .ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Internal,
                "Redis scheduler due time is out of range",
            )
        })
}
