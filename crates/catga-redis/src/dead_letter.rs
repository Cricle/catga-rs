//! Redis-backed ordered dead-letter queue.

use async_trait::async_trait;
use catga_codec_postcard::PostcardCodec;
use catga_core::{CatgaResult, DeadLetter, DeadLetterStore, EnvelopeCodec};
use redis::{
    AsyncCommands, Script,
    aio::{ConnectionManager, ConnectionManagerConfig},
};

use crate::transport::map_error;

const ENQUEUE: &str = r#"local id=redis.call('INCR',KEYS[1]); local key=KEYS[2]..':'..id; redis.call('HSET',key,'payload',ARGV[1],'reason',ARGV[2],'attempts',ARGV[3]); redis.call('RPUSH',KEYS[3],id); return id"#;

/// Redis list-backed FIFO dead-letter store.
pub struct RedisDeadLetters {
    connection: ConnectionManager,
    prefix: Box<str>,
    codec: PostcardCodec,
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
            codec: PostcardCodec,
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
            if let (Some(payload), Some(reason), Some(attempts)) = (payload, reason, attempts) {
                letters.push(DeadLetter::new(
                    self.codec.decode(&payload)?,
                    reason,
                    attempts,
                ));
            }
        }
        Ok(letters)
    }
}
