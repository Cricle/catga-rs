//! Redis hash-backed immutable aggregate snapshots.

use std::{
    any::{Any, TypeId},
    marker::PhantomData,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use catga_core::codec::memorypack::MemoryPackSnapshotCodec;
use catga_core::{CatgaError, CatgaResult, ErrorCode, Snapshot, SnapshotCodec, SnapshotStore};
use redis::{AsyncCommands, Script, aio::ConnectionManager};

use crate::transport::map_error;

const SAVE: &str = r#"
local current = redis.call('HGET', KEYS[1], 'version')
if current == false then current = -1 else current = tonumber(current) end
if current > tonumber(ARGV[1]) then
    return {err = 'CATGA_SNAPSHOT_CONFLICT'}
end
redis.call('HSET', KEYS[1], 'version', ARGV[1], 'timestamp', ARGV[2], 'state', ARGV[3])
return ARGV[1]
"#;

/// Redis-backed latest snapshots for one explicit aggregate state type.
pub struct RedisSnapshotStore<S, C = MemoryPackSnapshotCodec<S>> {
    connection: ConnectionManager,
    prefix: Box<str>,
    codec: C,
    state: PhantomData<fn() -> S>,
}

impl<S> RedisSnapshotStore<S>
where
    S: Send + Sync + 'static,
    MemoryPackSnapshotCodec<S>: SnapshotCodec<S>,
{
    /// Connects using compact MemoryPack state serialization.
    pub async fn connect(
        server: impl AsRef<str>,
        prefix: impl Into<Box<str>>,
    ) -> CatgaResult<Self> {
        Self::with_codec(server, prefix, MemoryPackSnapshotCodec::default()).await
    }
}

impl<S, C> RedisSnapshotStore<S, C>
where
    S: Send + Sync + 'static,
    C: SnapshotCodec<S>,
{
    /// Connects using an explicit state codec.
    pub async fn with_codec(
        server: impl AsRef<str>,
        prefix: impl Into<Box<str>>,
        codec: C,
    ) -> CatgaResult<Self> {
        let client = redis::Client::open(server.as_ref()).map_err(map_error)?;
        let connection = client
            .get_connection_manager_with_config(crate::config::command_connection_manager_config())
            .await
            .map_err(map_error)?;
        Ok(Self {
            connection,
            prefix: prefix.into(),
            codec,
            state: PhantomData,
        })
    }

    fn key(&self, stream_id: &str) -> String {
        format!("{}:snapshot:{stream_id}", self.prefix)
    }

    fn require_state<T>() -> CatgaResult<()>
    where
        T: Send + Sync + 'static,
    {
        if TypeId::of::<S>() == TypeId::of::<T>() {
            Ok(())
        } else {
            Err(CatgaError::new(
                ErrorCode::Validation,
                "the requested snapshot state type does not match this store codec",
            ))
        }
    }
}

#[async_trait]
impl<S, C> SnapshotStore for RedisSnapshotStore<S, C>
where
    S: Send + Sync + 'static,
    C: SnapshotCodec<S>,
{
    async fn save<T>(&self, snapshot: Snapshot<T>) -> CatgaResult<()>
    where
        T: Send + Sync + 'static,
    {
        Self::require_state::<T>()?;
        let state: Arc<dyn Any + Send + Sync> = snapshot.shared_state();
        let state = Arc::downcast::<S>(state).map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "the snapshot state does not match this store codec",
            )
        })?;
        let payload = self.codec.encode_state(&state)?;
        let mut connection = self.connection.clone();
        Script::new(SAVE)
            .key(self.key(snapshot.stream_id()))
            .arg(snapshot.version())
            .arg(unix_millis(snapshot.timestamp()))
            .arg(payload)
            .invoke_async::<i64>(&mut connection)
            .await
            .map(|_| ())
            .map_err(map_save_error)
    }

    async fn load<T>(&self, stream_id: &str) -> CatgaResult<Option<Snapshot<T>>>
    where
        T: Send + Sync + 'static,
    {
        Self::require_state::<T>()?;
        let key = self.key(stream_id);
        let mut connection = self.connection.clone();
        let version: Option<i64> = connection.hget(&key, "version").await.map_err(map_error)?;
        let Some(version) = version else {
            return Ok(None);
        };
        let timestamp: Option<u64> = connection
            .hget(&key, "timestamp")
            .await
            .map_err(map_error)?;
        let payload: Option<Vec<u8>> = connection.hget(&key, "state").await.map_err(map_error)?;
        let timestamp = timestamp.ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Internal,
                "Redis snapshot is missing its timestamp",
            )
        })?;
        let payload = payload.ok_or_else(|| {
            CatgaError::new(ErrorCode::Internal, "Redis snapshot is missing its state")
        })?;
        let state: Arc<dyn Any + Send + Sync> = Arc::new(self.codec.decode_state(&payload)?);
        let state = Arc::downcast::<T>(state).map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "the stored snapshot state does not match the requested type",
            )
        })?;
        Ok(Some(Snapshot::from_shared(
            stream_id,
            state,
            version,
            from_unix_millis(timestamp),
        )))
    }

    async fn delete(&self, stream_id: &str) -> CatgaResult<()> {
        let mut connection = self.connection.clone();
        connection.del(self.key(stream_id)).await.map_err(map_error)
    }
}

fn unix_millis(time: SystemTime) -> u64 {
    u64::try_from(
        time.duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

fn from_unix_millis(millis: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(millis)
}

fn map_save_error(error: redis::RedisError) -> CatgaError {
    if error.to_string().contains("CATGA_SNAPSHOT_CONFLICT") {
        CatgaError::new(
            ErrorCode::Conflict,
            "a newer snapshot already exists for this stream",
        )
    } else {
        map_error(error)
    }
}
