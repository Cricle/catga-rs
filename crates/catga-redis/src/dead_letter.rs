//! Redis-backed ordered dead-letter queue.

use async_trait::async_trait;
use catga_codec_memorypack::MemoryPackCodec;
use catga_core::{
    CatgaError, CatgaResult, DeadLetter, DeadLetterDiagnostics, DeadLetterStore, EnvelopeCodec,
    ErrorCode,
};
use redis::{
    AsyncCommands, Script,
    aio::{ConnectionManager, ConnectionManagerConfig},
};

use crate::transport::map_error;

const ENQUEUE: &str = r#"local id=redis.call('INCR',KEYS[1]); local key=KEYS[2]..':'..id; redis.call('HSET',key,'payload',ARGV[1],'reason',ARGV[2],'attempts',ARGV[3],'failed_at_unix_ms',ARGV[4],'error_code',ARGV[5],'stage',ARGV[6]); redis.call('RPUSH',KEYS[3],id); return id"#;

/// Redis list-backed FIFO dead-letter store.
pub struct RedisDeadLetters {
    connection: ConnectionManager,
    prefix: Box<str>,
    codec: MemoryPackCodec,
}

impl RedisDeadLetters {
    /// Connects and namespaces dead letters beneath `prefix`.
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
            codec: MemoryPackCodec::default(),
        })
    }
    fn sequence(&self) -> String {
        format!("{}:sequence", self.prefix)
    }
    fn details(&self) -> String {
        format!("{}:details", self.prefix)
    }
    fn queue(&self) -> String {
        format!("{}:queue", self.prefix)
    }
}

#[async_trait]
impl DeadLetterStore for RedisDeadLetters {
    async fn enqueue(&self, letter: DeadLetter) -> CatgaResult<()> {
        let payload = self.codec.encode(letter.envelope())?;
        let mut c = self.connection.clone();
        Script::new(ENQUEUE)
            .key(self.sequence())
            .key(self.details())
            .key(self.queue())
            .arg(payload)
            .arg(letter.reason())
            .arg(letter.attempts())
            .arg(letter.diagnostics().failed_at_unix_ms())
            .arg(letter.diagnostics().error_code().as_stable_str())
            .arg(letter.diagnostics().stage())
            .invoke_async::<u64>(&mut c)
            .await
            .map(|_| ())
            .map_err(map_error)
    }
    async fn list(&self, limit: usize) -> CatgaResult<Vec<DeadLetter>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut c = self.connection.clone();
        let ids: Vec<u64> = c
            .lrange(
                self.queue(),
                0,
                isize::try_from(limit.saturating_sub(1)).unwrap_or(isize::MAX),
            )
            .await
            .map_err(map_error)?;
        let mut letters = Vec::with_capacity(ids.len());
        for id in ids {
            let key = format!("{}:{id}", self.details());
            let payload: Option<Vec<u8>> = c.hget(&key, "payload").await.map_err(map_error)?;
            let reason: Option<String> = c.hget(&key, "reason").await.map_err(map_error)?;
            let attempts: Option<u32> = c.hget(&key, "attempts").await.map_err(map_error)?;
            let failed_at_unix_ms: Option<u64> =
                c.hget(&key, "failed_at_unix_ms").await.map_err(map_error)?;
            let error_code: Option<String> = c.hget(&key, "error_code").await.map_err(map_error)?;
            let stage: Option<String> = c.hget(&key, "stage").await.map_err(map_error)?;
            if let (Some(payload), Some(reason), Some(attempts)) = (payload, reason, attempts) {
                letters.push(decode_dead_letter(
                    &self.codec,
                    payload,
                    reason,
                    attempts,
                    failed_at_unix_ms,
                    error_code,
                    stage,
                )?);
            }
        }
        Ok(letters)
    }
}

fn decode_dead_letter(
    codec: &MemoryPackCodec,
    payload: Vec<u8>,
    reason: String,
    attempts: u32,
    failed_at_unix_ms: Option<u64>,
    error_code: Option<String>,
    stage: Option<String>,
) -> CatgaResult<DeadLetter> {
    let envelope = codec.decode(&payload)?;
    match (failed_at_unix_ms, error_code, stage) {
        (None, None, None) => Ok(DeadLetter::new(envelope, reason, attempts)),
        (Some(failed_at_unix_ms), Some(error_code), Some(stage)) => {
            let error_code = ErrorCode::from_stable_str(&error_code).ok_or_else(|| {
                CatgaError::new(
                    ErrorCode::Internal,
                    "Redis dead-letter error code is unknown",
                )
            })?;
            let diagnostics = DeadLetterDiagnostics::try_at(failed_at_unix_ms, error_code, stage)
                .map_err(|_| {
                CatgaError::new(
                    ErrorCode::Internal,
                    "Redis dead-letter diagnostics are invalid",
                )
            })?;
            DeadLetter::try_with_diagnostics(envelope, reason, attempts, diagnostics).map_err(
                |_| {
                    CatgaError::new(
                        ErrorCode::Internal,
                        "Redis dead-letter description is invalid",
                    )
                },
            )
        }
        _ => Err(CatgaError::new(
            ErrorCode::Internal,
            "Redis dead-letter diagnostics are incomplete",
        )),
    }
}
