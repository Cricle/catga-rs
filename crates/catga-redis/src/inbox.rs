//! Redis Lua-CAS inbox processing records.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, DEFAULT_INBOX_CLAIM_LEASE, ErrorCode, InboxClaim, InboxStore,
    ProcessingState, inbox_claim_expires_at, telemetry, validate_retention_cleanup_limit,
};
use redis::{AsyncCommands, Script, aio::ConnectionManager};

use crate::transport::map_error;

const CLAIMED: u8 = 1;
const COMPLETED_EMPTY: u8 = 2;
const COMPLETED_RESULT: u8 = 3;
const FAILED: u8 = 4;

const CLAIM: &str = r#"
local value = redis.call('GET', KEYS[1])
local state = 0
local expiry = 0
local generation = 0
if value ~= false then
    state = string.byte(value, 1)
    local stored_expiry, stored_generation = string.match(string.sub(value, 2), '^(%d+):(%d+):')
    expiry = tonumber(stored_expiry) or 0
    generation = tonumber(stored_generation) or 0
end
if value == false or state == 4 or (state == 1 and expiry <= tonumber(ARGV[1])) then
    generation = generation + 1
    redis.call('SET', KEYS[1], string.char(1) .. ARGV[2] .. ':' .. generation .. ':')
    return generation
end
return 0
"#;

const TRANSITION: &str = r#"
local value = redis.call('GET', KEYS[1])
if value == false then return -1 end
if string.byte(value, 1) ~= 1 then return 0 end
local generation = tonumber(string.match(string.sub(value, 2), '^%d+:(%d+):')) or 0
if generation ~= tonumber(ARGV[2]) then return 0 end
redis.call('SET', KEYS[1], ARGV[1])
return 1
"#;

const COMPLETE: &str = r#"
local value = redis.call('GET', KEYS[1])
if value == false then return -1 end
if string.byte(value, 1) ~= 1 then return 0 end
local generation = tonumber(string.match(string.sub(value, 2), '^%d+:(%d+):')) or 0
if generation ~= tonumber(ARGV[2]) then return 0 end
redis.call('SET', KEYS[1], ARGV[1])
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
return 1
"#;

const CLEANUP_COMPLETED: &str = r#"
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local removed = 0
for _, id in ipairs(ids) do
    local key = ARGV[3] .. ':' .. id
    local value = redis.call('GET', key)
    if value ~= false and (string.byte(value, 1) == 2 or string.byte(value, 1) == 3) then
        redis.call('DEL', key)
        removed = removed + 1
    end
    redis.call('ZREM', KEYS[1], id)
end
return removed
"#;

/// Redis-backed inbox with atomic per-message processing transitions.
pub struct RedisInbox {
    connection: ConnectionManager,
    prefix: Box<str>,
}

impl RedisInbox {
    /// Connects and namespaces message records beneath `prefix`.
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

    fn key(&self, message_id: u64) -> String {
        format!("{}:{message_id}", self.prefix)
    }

    fn completed(&self) -> String {
        format!("{}:completed", self.prefix)
    }

    async fn transition(&self, claim: InboxClaim, value: Vec<u8>) -> CatgaResult<()> {
        let mut connection = self.connection.clone();
        match Script::new(TRANSITION)
            .key(self.key(claim.message_id()))
            .arg(value)
            .arg(claim.generation())
            .invoke_async::<i64>(&mut connection)
            .await
            .map_err(map_error)?
        {
            1 => Ok(()),
            -1 => Err(CatgaError::new(
                ErrorCode::NotFound,
                "inbox message is not claimed",
            )),
            _ => Err(CatgaError::new(
                ErrorCode::Conflict,
                "inbox message is not currently claimed",
            )),
        }
    }
}

#[async_trait]
impl InboxStore for RedisInbox {
    async fn try_claim(&self, message_id: u64) -> CatgaResult<Option<InboxClaim>> {
        self.try_claim_for(message_id, DEFAULT_INBOX_CLAIM_LEASE)
            .await
    }

    async fn try_claim_for(
        &self,
        message_id: u64,
        lease: Duration,
    ) -> CatgaResult<Option<InboxClaim>> {
        telemetry::record_persistence_optional_claim("redis", "inbox", "try_claim", async {
            let expires_at = inbox_claim_expires_at(lease)?;
            let now = current_unix_ms()?;
            let mut connection = self.connection.clone();
            let generation = Script::new(CLAIM)
                .key(self.key(message_id))
                .arg(now)
                .arg(expires_at)
                .invoke_async::<i64>(&mut connection)
                .await
                .map_err(map_error)?;
            if generation == 0 {
                return Ok(None);
            }
            let generation = u64::try_from(generation).map_err(|_| {
                CatgaError::new(
                    ErrorCode::Internal,
                    "Redis inbox claim generation is invalid",
                )
            })?;
            InboxClaim::new(message_id, generation)
                .map(Some)
                .ok_or_else(|| {
                    CatgaError::new(
                        ErrorCode::Internal,
                        "Redis inbox claim generation is invalid",
                    )
                })
        })
        .await
    }

    async fn complete(&self, claim: InboxClaim, result: Option<Arc<[u8]>>) -> CatgaResult<()> {
        telemetry::record_persistence("redis", "inbox", "complete", async {
            let mut value = Vec::with_capacity(
                result
                    .as_ref()
                    .map_or(1, |value| value.len().saturating_add(1)),
            );
            value.push(if result.is_some() {
                COMPLETED_RESULT
            } else {
                COMPLETED_EMPTY
            });
            if let Some(result) = result {
                value.extend_from_slice(&result);
            }
            let mut connection = self.connection.clone();
            match Script::new(COMPLETE)
                .key(self.key(claim.message_id()))
                .key(self.completed())
                .arg(value)
                .arg(claim.generation())
                .arg(current_unix_ms()?)
                .arg(claim.message_id())
                .invoke_async::<i64>(&mut connection)
                .await
                .map_err(map_error)?
            {
                1 => Ok(()),
                -1 => Err(CatgaError::new(
                    ErrorCode::NotFound,
                    "inbox message is not claimed",
                )),
                _ => Err(CatgaError::new(
                    ErrorCode::Conflict,
                    "inbox message is not currently claimed",
                )),
            }
        })
        .await
    }

    async fn fail(&self, claim: InboxClaim) -> CatgaResult<()> {
        telemetry::record_persistence("redis", "inbox", "fail", async {
            self.transition(claim, vec![FAILED]).await
        })
        .await
    }

    async fn state(&self, message_id: u64) -> CatgaResult<Option<ProcessingState>> {
        telemetry::record_persistence("redis", "inbox", "state", async {
            let mut connection = self.connection.clone();
            let value: Option<Vec<u8>> = connection
                .get(self.key(message_id))
                .await
                .map_err(map_error)?;
            value.map(|value| state(&value)).transpose()
        })
        .await
    }

    async fn result(&self, message_id: u64) -> CatgaResult<Option<Arc<[u8]>>> {
        telemetry::record_persistence("redis", "inbox", "result", async {
            let mut connection = self.connection.clone();
            let value: Option<Vec<u8>> = connection
                .get(self.key(message_id))
                .await
                .map_err(map_error)?;
            Ok(value.and_then(|value| {
                (value.first() == Some(&COMPLETED_RESULT)).then(|| Arc::from(&value[1..]))
            }))
        })
        .await
    }

    async fn cleanup_completed(&self, retention: Duration, limit: usize) -> CatgaResult<usize> {
        telemetry::record_persistence("redis", "inbox", "cleanup", async {
            validate_retention_cleanup_limit(limit)?;
            if limit == 0 {
                return Ok(0);
            }
            let retention = u64::try_from(retention.as_millis()).map_err(|_| {
                CatgaError::new(
                    ErrorCode::Validation,
                    "inbox retention exceeds the supported millisecond range",
                )
            })?;
            let cutoff = current_unix_ms()?.saturating_sub(retention);
            let mut connection = self.connection.clone();
            Script::new(CLEANUP_COMPLETED)
                .key(self.completed())
                .arg(cutoff)
                .arg(limit)
                .arg(&*self.prefix)
                .invoke_async::<usize>(&mut connection)
                .await
                .map_err(map_error)
        })
        .await
    }
}

fn current_unix_ms() -> CatgaResult<u64> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| {
        CatgaError::new(ErrorCode::Internal, "system clock precedes the Unix epoch")
    })?;
    u64::try_from(elapsed.as_millis()).map_err(|_| {
        CatgaError::new(
            ErrorCode::Internal,
            "system clock exceeds the supported millisecond range",
        )
    })
}

fn state(value: &[u8]) -> CatgaResult<ProcessingState> {
    match value.first() {
        Some(&CLAIMED) => Ok(ProcessingState::Claimed),
        Some(&COMPLETED_EMPTY | &COMPLETED_RESULT) => Ok(ProcessingState::Completed),
        Some(&FAILED) => Ok(ProcessingState::Failed),
        _ => Err(CatgaError::new(
            ErrorCode::Internal,
            "Redis inbox record is malformed",
        )),
    }
}
