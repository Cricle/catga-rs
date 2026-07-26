//! Redis storage for the plain durable [`catga_flow::FlowStore`] contract.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use catga_codec_postcard::PostcardCodec;
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{FlowState, FlowStatus, FlowStore};
use redis::{
    AsyncCommands, Script,
    aio::{ConnectionManager, ConnectionManagerConfig},
};
use sha2::{Digest, Sha256};

use crate::transport::map_error;

/// Maximum stale candidates inspected by one [`RedisFlows::try_claim`] call.
pub const MAX_REDIS_FLOW_CLAIM_CANDIDATES: usize = 32;

const CREATE: &str = r#"
if redis.call('EXISTS', KEYS[1]) ~= 0 then return 0 end
redis.call('HSET', KEYS[1], 'version', ARGV[1], 'value', ARGV[2], 'heartbeat', ARGV[3], 'status', ARGV[4], 'flow_type', ARGV[5], 'owner', ARGV[6])
if ARGV[7] == '1' then
  redis.call('ZADD', KEYS[2], ARGV[3], KEYS[1])
else
  redis.call('ZREM', KEYS[2], KEYS[1])
end
return 1
"#;

const UPDATE: &str = r#"
if redis.call('HGET', KEYS[1], 'version') ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[1], 'flow_type') ~= ARGV[5] then return 0 end
redis.call('HSET', KEYS[1], 'version', ARGV[2], 'value', ARGV[3], 'heartbeat', ARGV[4], 'status', ARGV[6], 'owner', ARGV[7])
if ARGV[8] == '1' then
  redis.call('ZADD', KEYS[2], ARGV[4], KEYS[1])
else
  redis.call('ZREM', KEYS[2], KEYS[1])
end
return 1
"#;

const CLAIM: &str = r#"
if redis.call('HGET', KEYS[1], 'version') ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[1], 'status') ~= 'running' then return 0 end
local heartbeat = tonumber(redis.call('HGET', KEYS[1], 'heartbeat'))
if heartbeat == nil or heartbeat > tonumber(ARGV[2]) then return 0 end
redis.call('HSET', KEYS[1], 'version', ARGV[3], 'value', ARGV[4], 'heartbeat', ARGV[5], 'status', ARGV[6], 'owner', ARGV[7])
redis.call('ZADD', KEYS[2], ARGV[5], KEYS[1])
return 1
"#;

const HEARTBEAT: &str = r#"
if redis.call('HGET', KEYS[1], 'version') ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[2] then return 0 end
redis.call('HSET', KEYS[1], 'value', ARGV[3], 'heartbeat', ARGV[4])
if redis.call('HGET', KEYS[1], 'status') == 'running' then
  redis.call('ZADD', KEYS[2], ARGV[4], KEYS[1])
else
  redis.call('ZREM', KEYS[2], KEYS[1])
end
return 1
"#;

/// Redis-backed versioned flow state with one bounded claim index per flow type.
pub struct RedisFlows {
    connection: ConnectionManager,
    prefix: Box<str>,
    codec: PostcardCodec,
}

impl RedisFlows {
    /// Connects to Redis and namespaces plain flow state beneath `prefix`.
    pub async fn connect(
        server: impl AsRef<str>,
        prefix: impl Into<Box<str>>,
    ) -> CatgaResult<Self> {
        let client = redis::Client::open(server.as_ref()).map_err(map_error)?;
        let connection = client
            .get_connection_manager_with_config(
                ConnectionManagerConfig::new().set_response_timeout(None),
            )
            .await
            .map_err(map_error)?;
        Ok(Self {
            connection,
            prefix: prefix.into(),
            codec: PostcardCodec,
        })
    }

    fn record_key(&self, id: &str) -> String {
        flow_key(&self.prefix, id)
    }

    fn index_key(&self, flow_type: &str) -> String {
        type_index_key(&self.prefix, flow_type)
    }

    fn fields<'a>(
        &self,
        state: &'a FlowState,
    ) -> CatgaResult<(Vec<u8>, u64, &'static str, &'a str, bool)> {
        Ok((
            self.codec.encode_value(state)?,
            unix_millis(state.heartbeat())?,
            status_code(state.status()),
            state.owner().unwrap_or(""),
            state.status() == FlowStatus::Running,
        ))
    }
}

#[async_trait]
impl FlowStore for RedisFlows {
    async fn create(&self, state: FlowState) -> CatgaResult<bool> {
        let (value, heartbeat, status, owner, indexed) = self.fields(&state)?;
        let mut connection = self.connection.clone();
        let created: i64 = Script::new(CREATE)
            .key(self.record_key(state.id()))
            .key(self.index_key(state.flow_type()))
            .arg(state.version())
            .arg(value)
            .arg(heartbeat)
            .arg(status)
            .arg(state.flow_type())
            .arg(owner)
            .arg(if indexed { 1 } else { 0 })
            .invoke_async(&mut connection)
            .await
            .map_err(map_error)?;
        Ok(created == 1)
    }

    async fn update(&self, expected_version: i64, next: FlowState) -> CatgaResult<bool> {
        if next.version() != expected_version.saturating_add(1) {
            return Ok(false);
        }
        let (value, heartbeat, status, owner, indexed) = self.fields(&next)?;
        let mut connection = self.connection.clone();
        let updated: i64 = Script::new(UPDATE)
            .key(self.record_key(next.id()))
            .key(self.index_key(next.flow_type()))
            .arg(expected_version)
            .arg(next.version())
            .arg(value)
            .arg(heartbeat)
            .arg(next.flow_type())
            .arg(status)
            .arg(owner)
            .arg(if indexed { 1 } else { 0 })
            .invoke_async(&mut connection)
            .await
            .map_err(map_error)?;
        Ok(updated == 1)
    }

    async fn get(&self, id: &str) -> CatgaResult<Option<FlowState>> {
        let mut connection = self.connection.clone();
        let value: Option<Vec<u8>> = connection
            .hget(self.record_key(id), "value")
            .await
            .map_err(map_error)?;
        value
            .map(|value| self.codec.decode_value(&value))
            .transpose()
    }

    async fn try_claim(
        &self,
        flow_type: &str,
        owner: &str,
        stale_after: Duration,
    ) -> CatgaResult<Option<FlowState>> {
        let stale_before = stale_before(stale_after)?;
        let index = self.index_key(flow_type);
        let mut connection = self.connection.clone();
        let candidates: Vec<String> = redis::cmd("ZRANGEBYSCORE")
            .arg(&index)
            .arg("-inf")
            .arg(stale_before)
            .arg("LIMIT")
            .arg(0)
            .arg(MAX_REDIS_FLOW_CLAIM_CANDIDATES)
            .query_async(&mut connection)
            .await
            .map_err(map_error)?;
        for key in candidates {
            let value: Option<Vec<u8>> = connection.hget(&key, "value").await.map_err(map_error)?;
            let Some(value) = value else { continue };
            let current: FlowState = self.codec.decode_value(&value)?;
            if current.flow_type() != flow_type || current.status() != FlowStatus::Running {
                continue;
            }
            let next = current.clone().claimed_by(owner).next_version();
            let (next_value, heartbeat, status, next_owner, _) = self.fields(&next)?;
            let claimed: i64 = Script::new(CLAIM)
                .key(&key)
                .key(&index)
                .arg(current.version())
                .arg(stale_before)
                .arg(next.version())
                .arg(next_value)
                .arg(heartbeat)
                .arg(status)
                .arg(next_owner)
                .invoke_async(&mut connection)
                .await
                .map_err(map_error)?;
            if claimed == 1 {
                return Ok(Some(next));
            }
        }
        Ok(None)
    }

    async fn heartbeat(&self, id: &str, owner: &str, version: i64) -> CatgaResult<bool> {
        let Some(current) = self.get(id).await? else {
            return Ok(false);
        };
        if current.owner() != Some(owner) || current.version() != version {
            return Ok(false);
        }
        let next = current.heartbeated_at(SystemTime::now());
        let (value, heartbeat, _, _, _) = self.fields(&next)?;
        let mut connection = self.connection.clone();
        let updated: i64 = Script::new(HEARTBEAT)
            .key(self.record_key(id))
            .key(self.index_key(next.flow_type()))
            .arg(version)
            .arg(owner)
            .arg(value)
            .arg(heartbeat)
            .invoke_async(&mut connection)
            .await
            .map_err(map_error)?;
        Ok(updated == 1)
    }
}

pub(crate) fn flow_key(prefix: &str, id: &str) -> String {
    hashed_key(prefix, "flow", id)
}

pub(crate) fn type_index_key(prefix: &str, flow_type: &str) -> String {
    hashed_key(prefix, "flow-type", flow_type)
}

fn hashed_key(prefix: &str, kind: &str, value: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(value.len().to_be_bytes());
    digest.update(value.as_bytes());
    format!("{prefix}:{kind}:{:x}", digest.finalize())
}

fn unix_millis(time: SystemTime) -> CatgaResult<u64> {
    let duration = time.duration_since(UNIX_EPOCH).map_err(|_| {
        CatgaError::new(
            ErrorCode::Validation,
            "Redis flow heartbeat precedes the Unix epoch",
        )
    })?;
    u64::try_from(duration.as_millis()).map_err(|_| {
        CatgaError::new(
            ErrorCode::Validation,
            "Redis flow heartbeat exceeds the supported range",
        )
    })
}

fn stale_before(stale_after: Duration) -> CatgaResult<u64> {
    let now = unix_millis(SystemTime::now())?;
    let elapsed = stale_after.as_millis().min(u128::from(u64::MAX)) as u64;
    Ok(now.saturating_sub(elapsed))
}

const fn status_code(status: FlowStatus) -> &'static str {
    match status {
        FlowStatus::Running => "running",
        FlowStatus::Compensating => "compensating",
        FlowStatus::Suspended => "suspended",
        FlowStatus::Done => "done",
        FlowStatus::Failed => "failed",
        FlowStatus::Cancelled => "cancelled",
    }
}

#[cfg(test)]
mod tests {
    use super::HEARTBEAT;

    #[test]
    fn heartbeat_removes_non_running_flows_from_claim_index() {
        assert!(HEARTBEAT.contains("if redis.call('HGET', KEYS[1], 'status') == 'running' then"));
        assert!(HEARTBEAT.contains("redis.call('ZREM', KEYS[2], KEYS[1])"));
    }
}
