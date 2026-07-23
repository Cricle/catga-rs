//! Redis Lua-CAS durable outbox.

use async_trait::async_trait;
use catga_codec_postcard::PostcardCodec;
use catga_core::{CatgaError, CatgaResult, EnvelopeCodec, ErrorCode, OutboxMessage, OutboxStore};
use redis::{
    AsyncCommands, Script,
    aio::{ConnectionManager, ConnectionManagerConfig},
};

use crate::transport::map_error;

const ENQUEUE: &str = r#"if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end local n=redis.call('INCR',KEYS[3]); redis.call('HSET',KEYS[1],'payload',ARGV[1],'owner',''); redis.call('ZADD',KEYS[2],n,ARGV[2]); return 1"#;
const CLAIM: &str = r#"local out={} for _,id in ipairs(redis.call('ZRANGE',KEYS[1],0,ARGV[2]-1)) do local k=ARGV[1]..':'..id if redis.call('HGET',k,'owner') == '' then redis.call('HSET',k,'owner',ARGV[3]); table.insert(out,id) end end return out"#;
const ACK: &str = r#"if redis.call('HGET',KEYS[1],'owner') == ARGV[1] then redis.call('DEL',KEYS[1]); redis.call('ZREM',KEYS[2],ARGV[2]); return 1 end return 0"#;
const RELEASE: &str = r#"if redis.call('HGET',KEYS[1],'owner') == ARGV[1] then redis.call('HSET',KEYS[1],'owner',''); return 1 end return 0"#;

/// Redis-backed ordered outbox with atomic owner transitions.
pub struct RedisOutbox {
    connection: ConnectionManager,
    prefix: Box<str>,
    codec: PostcardCodec,
}

impl RedisOutbox {
    /// Connects and namespaces durable outbox records beneath `prefix`.
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
    fn key(&self, id: u64) -> String {
        format!("{}:{id}", self.prefix)
    }
    fn pending(&self) -> String {
        format!("{}:pending", self.prefix)
    }
    fn sequence(&self) -> String {
        format!("{}:sequence", self.prefix)
    }
}

#[async_trait]
impl OutboxStore for RedisOutbox {
    async fn enqueue(&self, message: OutboxMessage) -> CatgaResult<()> {
        let id = message.id();
        let payload = self.codec.encode(message.envelope())?;
        let mut c = self.connection.clone();
        let inserted = Script::new(ENQUEUE)
            .key(self.key(id))
            .key(self.pending())
            .key(self.sequence())
            .arg(payload)
            .arg(id)
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
    }
    async fn claim(&self, owner: &str, limit: usize) -> CatgaResult<Vec<OutboxMessage>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut c = self.connection.clone();
        let ids = Script::new(CLAIM)
            .key(self.pending())
            .arg(&*self.prefix)
            .arg(limit)
            .arg(owner)
            .invoke_async::<Vec<u64>>(&mut c)
            .await
            .map_err(map_error)?;
        let mut claimed = Vec::with_capacity(ids.len());
        for id in ids {
            let payload: Option<Vec<u8>> =
                c.hget(self.key(id), "payload").await.map_err(map_error)?;
            if let Some(payload) = payload {
                let mut message = OutboxMessage::new(self.codec.decode(&payload)?);
                message.claim(owner);
                claimed.push(message);
            }
        }
        Ok(claimed)
    }
    async fn ack(&self, owner: &str, id: u64) -> CatgaResult<()> {
        let mut c = self.connection.clone();
        Script::new(ACK)
            .key(self.key(id))
            .key(self.pending())
            .arg(owner)
            .arg(id)
            .invoke_async::<i64>(&mut c)
            .await
            .map(|_| ())
            .map_err(map_error)
    }
    async fn release(&self, owner: &str, id: u64) -> CatgaResult<()> {
        let mut c = self.connection.clone();
        Script::new(RELEASE)
            .key(self.key(id))
            .arg(owner)
            .invoke_async::<i64>(&mut c)
            .await
            .map(|_| ())
            .map_err(map_error)
    }
}
