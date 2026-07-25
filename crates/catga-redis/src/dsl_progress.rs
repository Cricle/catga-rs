//! Redis persistence for versioned, application-encoded DSL step progress.

use async_trait::async_trait;
use catga_codec_postcard::PostcardCodec;
use catga_core::CatgaResult;
use catga_flow::{DslStepProgress, DslStepProgressStore};
use redis::{
    Script,
    aio::{ConnectionManager, ConnectionManagerConfig},
};
use sha2::{Digest, Sha256};

use crate::transport::map_error;

const CREATE: &str = r#"
if redis.call('EXISTS', KEYS[1]) ~= 0 then return 0 end
redis.call('HSET', KEYS[1], 'version', ARGV[1], 'value', ARGV[2])
return 1
"#;
const UPDATE: &str = r#"
if redis.call('HGET', KEYS[1], 'version') ~= ARGV[1] then return 0 end
redis.call('HSET', KEYS[1], 'version', ARGV[2], 'value', ARGV[3])
return 1
"#;

/// Redis-backed CAS storage for recoverable [`DslStepProgress`] records.
///
/// Each flow identity and step index maps to one SHA-256-derived key. The
/// application payload remains an opaque Postcard value, while a separate
/// Redis hash field lets the update script compare its version atomically.
/// This prevents a read-modify-write race between distributed flow workers.
pub struct RedisDslStepProgress {
    connection: ConnectionManager,
    prefix: Box<str>,
    codec: PostcardCodec,
}

impl RedisDslStepProgress {
    /// Connects to Redis and namespaces progress keys beneath `prefix`.
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

    fn key(&self, flow_id: &str, step_index: u32) -> String {
        let mut digest = Sha256::new();
        digest.update(flow_id.len().to_be_bytes());
        digest.update(flow_id.as_bytes());
        digest.update(step_index.to_be_bytes());
        format!("{}:dsl-progress:{:x}", self.prefix, digest.finalize())
    }
}

#[async_trait]
impl DslStepProgressStore for RedisDslStepProgress {
    async fn create(&self, progress: DslStepProgress) -> CatgaResult<bool> {
        let value = self.codec.encode_value(&progress)?;
        let mut connection = self.connection.clone();
        let created: i64 = Script::new(CREATE)
            .key(self.key(progress.flow_id(), progress.step_index()))
            .arg(progress.version())
            .arg(value)
            .invoke_async(&mut connection)
            .await
            .map_err(map_error)?;
        Ok(created == 1)
    }

    async fn update(&self, expected_version: i64, next: DslStepProgress) -> CatgaResult<bool> {
        if next.version() != expected_version.saturating_add(1) {
            return Ok(false);
        }
        let value = self.codec.encode_value(&next)?;
        let mut connection = self.connection.clone();
        let updated: i64 = Script::new(UPDATE)
            .key(self.key(next.flow_id(), next.step_index()))
            .arg(expected_version)
            .arg(next.version())
            .arg(value)
            .invoke_async(&mut connection)
            .await
            .map_err(map_error)?;
        Ok(updated == 1)
    }

    async fn get(&self, flow_id: &str, step_index: u32) -> CatgaResult<Option<DslStepProgress>> {
        use redis::AsyncCommands;

        let mut connection = self.connection.clone();
        let value: Option<Vec<u8>> = connection
            .hget(self.key(flow_id, step_index), "value")
            .await
            .map_err(map_error)?;
        value
            .map(|value| self.codec.decode_value(&value))
            .transpose()
    }

    async fn delete(&self, flow_id: &str, step_index: u32) -> CatgaResult<bool> {
        use redis::AsyncCommands;

        let mut connection = self.connection.clone();
        let deleted: i64 = connection
            .del(self.key(flow_id, step_index))
            .await
            .map_err(map_error)?;
        Ok(deleted == 1)
    }
}
