//! Redis Lua-CAS durable outbox.

use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use catga_core::codec::memorypack::MemoryPackCodec;
use catga_core::{
    CatgaError, CatgaResult, DEFAULT_OUTBOX_CLAIM_LEASE, DEFAULT_OUTBOX_MAX_RETRIES, EnvelopeCodec,
    ErrorCode, OutboxMessage, OutboxStore, outbox_claim_expires_at, telemetry,
    validate_outbox_claim_limit, validate_outbox_message_id, validate_retention_cleanup_limit,
};
use redis::{Script, aio::ConnectionManager};
use uuid::Uuid;

use crate::transport::map_error;

const ENQUEUE: &str = r#"if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end redis.call('HSET',KEYS[1],'payload',ARGV[1],'owner','','claim_token','','claimed_until','','retry_count','0','max_retries',ARGV[4],'last_error','','state','pending','published_at',''); redis.call('ZADD',KEYS[2],ARGV[2],ARGV[3]); return 1"#;
const CLAIM: &str = r#"local out={} local candidates=redis.call('ZRANGEBYSCORE',KEYS[1],'-inf',ARGV[2],'LIMIT',ARGV[8],ARGV[5]) for _,id in ipairs(candidates) do local k=ARGV[1]..':'..id local state=redis.call('HGET',k,'state') local owner=redis.call('HGET',k,'owner') local claimed_until=tonumber(redis.call('HGET',k,'claimed_until')) or 0 local retries=tonumber(redis.call('HGET',k,'retry_count')) or 0 local maximum=tonumber(redis.call('HGET',k,'max_retries')) or 3 if ((not state) or (owner == '' and state == 'pending') or (state == 'claimed' and claimed_until <= tonumber(ARGV[2]))) and retries < maximum then redis.call('HSET',k,'owner',ARGV[4],'claim_token',ARGV[7]..':'..id,'state','claimed','claimed_until',ARGV[6]); table.insert(out,id) if #out >= tonumber(ARGV[3]) then break end end end table.insert(out,'scan:'..#candidates) return out"#;
const CLAIM_SCAN_FACTOR: usize = 4;
const ACK: &str = r#"if redis.call('HGET',KEYS[1],'owner') == ARGV[1] and redis.call('HGET',KEYS[1],'claim_token') == ARGV[2] and redis.call('HGET',KEYS[1],'state') == 'claimed' then redis.call('HSET',KEYS[1],'owner','','claim_token','','claimed_until','','state','published','published_at',ARGV[4]); redis.call('ZREM',KEYS[2],ARGV[3]); redis.call('ZADD',KEYS[3],ARGV[4],ARGV[3]); return 1 end return 0"#;
const RELEASE: &str = r#"if redis.call('HGET',KEYS[1],'owner') == ARGV[1] and redis.call('HGET',KEYS[1],'claim_token') == ARGV[2] and redis.call('HGET',KEYS[1],'state') == 'claimed' then redis.call('HSET',KEYS[1],'owner','','claim_token','','claimed_until','','state','pending'); return 1 end return 0"#;
const RECORD_FAILURE: &str = r#"if redis.call('HGET',KEYS[1],'owner') ~= ARGV[1] or redis.call('HGET',KEYS[1],'claim_token') ~= ARGV[2] or redis.call('HGET',KEYS[1],'state') ~= 'claimed' then return 0 end local retries=(tonumber(redis.call('HGET',KEYS[1],'retry_count')) or 0)+1 local maximum=tonumber(redis.call('HGET',KEYS[1],'max_retries')) or 3 redis.call('HSET',KEYS[1],'retry_count',retries,'last_error',ARGV[4],'owner','','claim_token','','claimed_until','') if retries >= maximum then redis.call('HSET',KEYS[1],'state','failed'); redis.call('ZREM',KEYS[2],ARGV[3]); else redis.call('HSET',KEYS[1],'state','pending'); end return 1"#;
const CANCEL: &str = r#"local state=redis.call('HGET',KEYS[1],'state') if redis.call('HGET',KEYS[1],'owner') == '' and (not state or state == 'pending') then redis.call('DEL',KEYS[1]); redis.call('ZREM',KEYS[2],ARGV[1]); return 1 end return 0"#;
const CLEANUP_PUBLISHED: &str = r#"
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local removed = 0
for _, id in ipairs(ids) do
    local key = ARGV[3] .. ':' .. id
    if redis.call('HGET', key, 'state') == 'published' then
        redis.call('DEL', key)
        redis.call('ZREM', KEYS[2], id)
        removed = removed + 1
    end
    redis.call('ZREM', KEYS[1], id)
end
return removed
"#;

/// Redis-backed ordered outbox with atomic owner transitions.
pub struct RedisOutbox {
    connection: ConnectionManager,
    prefix: Box<str>,
    codec: MemoryPackCodec,
    claim_scan_offset: AtomicUsize,
}

impl RedisOutbox {
    /// Connects and namespaces durable outbox records beneath `prefix`.
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
            codec: MemoryPackCodec::default(),
            claim_scan_offset: AtomicUsize::new(0),
        })
    }
    fn key(&self, id: u64) -> String {
        format!("{}:{id}", self.prefix)
    }
    fn pending(&self) -> String {
        format!("{}:pending", self.prefix)
    }

    fn published(&self) -> String {
        format!("{}:published", self.prefix)
    }
}

#[async_trait]
impl OutboxStore for RedisOutbox {
    async fn enqueue(&self, message: OutboxMessage) -> CatgaResult<()> {
        telemetry::record_persistence("redis", "outbox", "enqueue", async {
            let id = message.id();
            validate_outbox_message_id(id)?;
            let payload = self.codec.encode(message.envelope())?;
            let due_at = message.not_before_unix_ms().unwrap_or(current_unix_ms()?);
            let mut c = self.connection.clone();
            let inserted = Script::new(ENQUEUE)
                .key(self.key(id))
                .key(self.pending())
                .arg(payload)
                .arg(due_at)
                .arg(id)
                .arg(message.max_retries())
                .invoke_async::<i64>(&mut c)
                .await
                .map_err(map_error)?;
            if inserted == 1 {
                Ok(())
            } else {
                Err(CatgaError::new(
                    ErrorCode::Conflict,
                    "an outbox message with this identifier already exists",
                ))
            }
        })
        .await
    }
    async fn claim(&self, owner: &str, limit: usize) -> CatgaResult<Vec<OutboxMessage>> {
        self.claim_for(owner, limit, DEFAULT_OUTBOX_CLAIM_LEASE)
            .await
    }

    async fn claim_for(
        &self,
        owner: &str,
        limit: usize,
        lease: Duration,
    ) -> CatgaResult<Vec<OutboxMessage>> {
        telemetry::record_persistence("redis", "outbox", "claim", async {
            validate_outbox_claim_limit(limit)?;
            let expires_at = outbox_claim_expires_at(lease)?;
            if limit == 0 {
                return Ok(Vec::new());
            }
            let scan_limit = limit.saturating_mul(CLAIM_SCAN_FACTOR);
            let claim_token_prefix = Uuid::new_v4().to_string();
            let scan_offset = self
                .claim_scan_offset
                .fetch_add(scan_limit, Ordering::Relaxed);
            let mut c = self.connection.clone();
            let mut results = Script::new(CLAIM)
                .key(self.pending())
                .arg(&*self.prefix)
                .arg(current_unix_ms()?)
                .arg(limit)
                .arg(owner)
                .arg(scan_limit)
                .arg(expires_at)
                .arg(&claim_token_prefix)
                .arg(scan_offset)
                .invoke_async::<Vec<String>>(&mut c)
                .await
                .map_err(map_error)?;
            let scanned = results
                .pop()
                .and_then(|marker| {
                    marker
                        .strip_prefix("scan:")
                        .and_then(|value| value.parse::<usize>().ok())
                })
                .ok_or_else(|| {
                    CatgaError::new(
                        ErrorCode::Internal,
                        "Redis outbox claim scan response is malformed",
                    )
                })?;
            if scanned < scan_limit {
                self.claim_scan_offset.store(0, Ordering::Relaxed);
            }
            let mut claimed = Vec::with_capacity(results.len());
            for id in results {
                let id = id.parse().map_err(|_| {
                    CatgaError::new(
                        ErrorCode::Internal,
                        "Redis outbox claim identifier is malformed",
                    )
                })?;
                let key = self.key(id);
                let (payload, retry_count, max_retries, last_error): (
                    Option<Vec<u8>>,
                    Option<u32>,
                    Option<u32>,
                    Option<String>,
                ) = redis::pipe()
                    .hget(&key, "payload")
                    .hget(&key, "retry_count")
                    .hget(&key, "max_retries")
                    .hget(&key, "last_error")
                    .query_async(&mut c)
                    .await
                    .map_err(map_error)?;
                if let Some(payload) = payload {
                    let max_retries = max_retries.unwrap_or(DEFAULT_OUTBOX_MAX_RETRIES);
                    let mut message = OutboxMessage::new(self.codec.decode(&payload)?)
                        .with_max_retries(max_retries)?
                        .with_retry_history(
                            retry_count.unwrap_or(0),
                            last_error.as_deref().filter(|reason| !reason.is_empty()),
                        );
                    message.claim_until_with_token(
                        owner,
                        format!("{claim_token_prefix}:{id}"),
                        expires_at,
                    );
                    claimed.push(message);
                }
            }
            Ok(claimed)
        })
        .await
    }
    async fn ack(&self, owner: &str, id: u64, claim_token: &str) -> CatgaResult<()> {
        telemetry::record_persistence("redis", "outbox", "ack", async {
            let mut c = self.connection.clone();
            Script::new(ACK)
                .key(self.key(id))
                .key(self.pending())
                .key(self.published())
                .arg(owner)
                .arg(claim_token)
                .arg(id)
                .arg(current_unix_ms()?)
                .invoke_async::<i64>(&mut c)
                .await
                .map(|_| ())
                .map_err(map_error)
        })
        .await
    }
    async fn release(&self, owner: &str, id: u64, claim_token: &str) -> CatgaResult<()> {
        telemetry::record_persistence("redis", "outbox", "release", async {
            let mut c = self.connection.clone();
            Script::new(RELEASE)
                .key(self.key(id))
                .arg(owner)
                .arg(claim_token)
                .invoke_async::<i64>(&mut c)
                .await
                .map(|_| ())
                .map_err(map_error)
        })
        .await
    }

    async fn record_failure(
        &self,
        owner: &str,
        id: u64,
        claim_token: &str,
        reason: &str,
    ) -> CatgaResult<()> {
        telemetry::record_persistence("redis", "outbox", "failure", async {
            let mut c = self.connection.clone();
            let reason = OutboxMessage::bounded_failure_reason(reason);
            Script::new(RECORD_FAILURE)
                .key(self.key(id))
                .key(self.pending())
                .arg(owner)
                .arg(claim_token)
                .arg(id)
                .arg(reason.as_ref())
                .invoke_async::<i64>(&mut c)
                .await
                .map(|_| ())
                .map_err(map_error)
        })
        .await
    }

    async fn cancel(&self, id: u64) -> CatgaResult<bool> {
        telemetry::record_persistence("redis", "outbox", "cancel", async {
            let mut c = self.connection.clone();
            let cancelled = Script::new(CANCEL)
                .key(self.key(id))
                .key(self.pending())
                .arg(id)
                .invoke_async::<i64>(&mut c)
                .await
                .map_err(map_error)?;
            Ok(cancelled == 1)
        })
        .await
    }

    async fn list_published(&self, limit: usize) -> CatgaResult<Vec<OutboxMessage>> {
        telemetry::record_persistence("redis", "outbox", "list_published", async {
            validate_outbox_claim_limit(limit)?;
            if limit == 0 {
                return Ok(Vec::new());
            }
            let mut connection = self.connection.clone();
            let ids: Vec<u64> = redis::cmd("ZRANGE")
                .arg(self.published())
                .arg(0)
                .arg(limit - 1)
                .query_async(&mut connection)
                .await
                .map_err(map_error)?;
            let mut published = Vec::with_capacity(ids.len());
            for id in ids {
                let key = self.key(id);
                let (payload, retry_count, max_retries, last_error, state, published_at): (
                    Option<Vec<u8>>,
                    Option<u32>,
                    Option<u32>,
                    Option<String>,
                    Option<String>,
                    Option<u64>,
                ) = redis::pipe()
                    .hget(&key, "payload")
                    .hget(&key, "retry_count")
                    .hget(&key, "max_retries")
                    .hget(&key, "last_error")
                    .hget(&key, "state")
                    .hget(&key, "published_at")
                    .query_async(&mut connection)
                    .await
                    .map_err(map_error)?;
                if state.as_deref() != Some("published") {
                    continue;
                }
                let Some(payload) = payload else {
                    continue;
                };
                let published_at = published_at.ok_or_else(|| {
                    CatgaError::new(ErrorCode::Internal, "published outbox timestamp is missing")
                })?;
                let mut message = OutboxMessage::new(self.codec.decode(&payload)?)
                    .with_max_retries(max_retries.unwrap_or(DEFAULT_OUTBOX_MAX_RETRIES))?
                    .with_retry_history(
                        retry_count.unwrap_or(0),
                        last_error.as_deref().filter(|reason| !reason.is_empty()),
                    );
                message.claim("");
                message.mark_published(published_at);
                published.push(message);
            }
            Ok(published)
        })
        .await
    }

    async fn cleanup_published(&self, retention: Duration, limit: usize) -> CatgaResult<usize> {
        telemetry::record_persistence("redis", "outbox", "cleanup_published", async {
            validate_retention_cleanup_limit(limit)?;
            if limit == 0 {
                return Ok(0);
            }
            let retention = u64::try_from(retention.as_millis()).map_err(|_| {
                CatgaError::new(
                    ErrorCode::Validation,
                    "outbox retention exceeds the supported millisecond range",
                )
            })?;
            let cutoff = current_unix_ms()?.saturating_sub(retention);
            let mut connection = self.connection.clone();
            Script::new(CLEANUP_PUBLISHED)
                .key(self.published())
                .key(self.pending())
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
            "system clock exceeds the supported range",
        )
    })
}
