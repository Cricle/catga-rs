//! Redis persistence for versioned, application-encoded DSL step progress.

use async_trait::async_trait;
use catga_core::codec::memorypack::MemoryPackCodec;
use catga_core::flow::{DslStepProgress, DslStepProgressStore};
use catga_core::hash::sha256_concat_digest;
use catga_core::{CatgaError, CatgaResult};
use redis::{Script, aio::ConnectionManager};

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
/// application payload remains an opaque MemoryPack value, while a separate
/// Redis hash field lets the update script compare its version atomically.
/// This prevents a read-modify-write race between distributed flow workers.
pub struct RedisDslStepProgress {
    connection: ConnectionManager,
    prefix: Box<str>,
    codec: MemoryPackCodec,
}

impl RedisDslStepProgress {
    /// Connects to Redis and namespaces progress keys beneath `prefix`.
    pub async fn connect(
        server: impl AsRef<str>,
        prefix: impl Into<Box<str>>,
    ) -> CatgaResult<Self> {
        let client = redis::Client::open(server.as_ref()).map_err(CatgaError::transient)?;
        let connection = client
            .get_connection_manager_with_config(crate::config::command_connection_manager_config())
            .await
            .map_err(CatgaError::transient)?;
        Ok(Self {
            connection,
            prefix: prefix.into(),
            codec: MemoryPackCodec::default(),
        })
    }

    fn key(&self, flow_id: &str, step_index: u32) -> String {
        let flow_id_len = flow_id.len().to_be_bytes();
        let step_index = step_index.to_be_bytes();
        format!(
            "{}:dsl-progress:{}",
            self.prefix,
            hex::encode(sha256_concat_digest(&[
                &flow_id_len,
                flow_id.as_bytes(),
                &step_index
            ]))
        )
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
            .map_err(CatgaError::transient)?;
        Ok(created == 1)
    }

    async fn update(&self, expected_version: i64, next: DslStepProgress) -> CatgaResult<bool> {
        if !DslStepProgress::is_next_version(expected_version, next.version()) {
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
            .map_err(CatgaError::transient)?;
        Ok(updated == 1)
    }

    async fn get(&self, flow_id: &str, step_index: u32) -> CatgaResult<Option<DslStepProgress>> {
        use redis::AsyncCommands;

        let mut connection = self.connection.clone();
        let value: Option<Vec<u8>> = connection
            .hget(self.key(flow_id, step_index), "value")
            .await
            .map_err(CatgaError::transient)?;
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
            .map_err(CatgaError::transient)?;
        Ok(deleted == 1)
    }
}
