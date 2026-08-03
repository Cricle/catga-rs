//! Redis persistence for compact, historical aggregate snapshots.

use std::{
    any::{Any, TypeId},
    marker::PhantomData,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use catga_core::codec::memorypack::{
    MemoryPackCodec, MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    MemoryPackSnapshotCodec, MemoryPackable, MemoryPackWriter,
};
use catga_core::{
    CatgaError, CatgaResult, EnhancedSnapshotStore, ErrorCode, Snapshot, SnapshotCodec,
    SnapshotInfo, SnapshotStore,
};
use redis::{Script, aio::ConnectionManager};
use serde::{Deserialize, Serialize};

use crate::transport::map_error;

const SAVE: &str = r#"
local current = redis.call('ZREVRANGE', KEYS[1], 0, 0)
if #current ~= 0 and current[1] > ARGV[1] then return -1 end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('ZADD', KEYS[1], 0, ARGV[1])
return 1
"#;
const LOAD_LATEST: &str = r#"
local versions
if ARGV[1] == '' then
  versions = redis.call('ZREVRANGE', KEYS[1], 0, 0)
else
  versions = redis.call('ZREVRANGEBYLEX', KEYS[1], '[' .. ARGV[1], '-', 'LIMIT', 0, 1)
end
if #versions == 0 then return {} end
local value = redis.call('HGET', KEYS[2], versions[1])
if not value then return {err = 'CATGA_SNAPSHOT_CORRUPT'} end
return {versions[1], value}
"#;
const HISTORY: &str = r#"
local versions = redis.call('ZRANGE', KEYS[1], 0, -1)
local values = {}
for _, version in ipairs(versions) do
  local value = redis.call('HGET', KEYS[2], version)
  if not value then return {err = 'CATGA_SNAPSHOT_CORRUPT'} end
  table.insert(values, version)
  table.insert(values, value)
end
return values
"#;
const DELETE: &str = r#"
redis.call('DEL', KEYS[1])
redis.call('DEL', KEYS[2])
return 1
"#;
const DELETE_BEFORE: &str = r#"
local versions = redis.call('ZRANGEBYLEX', KEYS[1], '-', '(' .. ARGV[1], 'LIMIT', 0, ARGV[2])
if #versions == 0 then return 0 end
redis.call('ZREM', KEYS[1], unpack(versions))
redis.call('HDEL', KEYS[2], unpack(versions))
return #versions
"#;
const CLEANUP: &str = r#"
local excess = redis.call('ZCARD', KEYS[1]) - tonumber(ARGV[1])
if excess <= 0 then return 0 end
local count = math.min(excess, tonumber(ARGV[2]))
local versions = redis.call('ZRANGE', KEYS[1], 0, count - 1)
if #versions == 0 then return 0 end
redis.call('ZREM', KEYS[1], unpack(versions))
redis.call('HDEL', KEYS[2], unpack(versions))
return #versions
"#;
const MUTATION_BATCH: i64 = 128;

#[derive(Deserialize, MemoryPackable, Serialize)]
struct StoredSnapshot {
    timestamp_millis: i64,
    state: Vec<u8>,
}

/// Redis-backed, multi-version snapshots for one explicit aggregate state type.
///
/// The adapter keeps an all-zero-score sorted set of order-preserving version
/// members plus a hash of compact MemoryPack records. Lexicographic version
/// ordering avoids precision loss from Redis floating-point scores, while Lua
/// scripts update both structures atomically. Cleanup and range deletion work
/// in bounded batches so one stream cannot create an unbounded server-side
/// script invocation.
pub struct RedisEnhancedSnapshots<S, C = MemoryPackSnapshotCodec<S>> {
    connection: ConnectionManager,
    prefix: Box<str>,
    codec: C,
    value_codec: MemoryPackCodec,
    state: PhantomData<fn() -> S>,
}

impl<S> RedisEnhancedSnapshots<S>
where
    S: Send + Sync + 'static,
    MemoryPackSnapshotCodec<S>: SnapshotCodec<S>,
{
    /// Connects using compact MemoryPack aggregate-state serialization.
    pub async fn connect(
        server: impl AsRef<str>,
        prefix: impl Into<Box<str>>,
    ) -> CatgaResult<Self> {
        Self::with_codec(server, prefix, MemoryPackSnapshotCodec::default()).await
    }
}

impl<S, C> RedisEnhancedSnapshots<S, C>
where
    S: Send + Sync + 'static,
    C: SnapshotCodec<S>,
{
    /// Connects using an explicit, typed aggregate-state codec.
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
            value_codec: MemoryPackCodec::default(),
            state: PhantomData,
        })
    }

    fn version_key(&self, stream_id: &str) -> String {
        format!("{}:enhanced-snapshot:{stream_id}:versions", self.prefix)
    }

    fn record_key(&self, stream_id: &str) -> String {
        format!("{}:enhanced-snapshot:{stream_id}:records", self.prefix)
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

    fn encode_snapshot(&self, snapshot: &Snapshot<S>) -> CatgaResult<Vec<u8>> {
        self.value_codec.encode_value(&StoredSnapshot {
            timestamp_millis: unix_millis(snapshot.timestamp())?,
            state: self.codec.encode_state(snapshot.state())?,
        })
    }

    fn decode_snapshot<T>(
        &self,
        stream_id: &str,
        version: i64,
        value: &[u8],
    ) -> CatgaResult<Snapshot<T>>
    where
        T: Send + Sync + 'static,
    {
        let stored: StoredSnapshot = self.value_codec.decode_value(value)?;
        let state: Arc<dyn Any + Send + Sync> = Arc::new(self.codec.decode_state(&stored.state)?);
        let state = Arc::downcast::<T>(state).map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "the stored snapshot state does not match the requested type",
            )
        })?;
        Ok(Snapshot::from_shared(
            stream_id,
            state,
            version,
            from_unix_millis(stored.timestamp_millis)?,
        ))
    }

    async fn load_record(
        &self,
        stream_id: &str,
        at_or_before: Option<i64>,
    ) -> CatgaResult<Option<(i64, Vec<u8>)>> {
        let mut connection = self.connection.clone();
        let fields: Vec<Vec<u8>> = Script::new(LOAD_LATEST)
            .key(self.version_key(stream_id))
            .key(self.record_key(stream_id))
            .arg(at_or_before.map(version_member).unwrap_or_default())
            .invoke_async(&mut connection)
            .await
            .map_err(map_snapshot_error)?;
        if fields.is_empty() {
            return Ok(None);
        }
        let [member, value] = fields.as_slice() else {
            return Err(malformed_snapshot());
        };
        Ok(Some((parse_version_member(member)?, value.clone())))
    }

    async fn remove_before(&self, stream_id: &str, version: i64) -> CatgaResult<()> {
        let cutoff = version_member(version);
        loop {
            let mut connection = self.connection.clone();
            let removed: i64 = Script::new(DELETE_BEFORE)
                .key(self.version_key(stream_id))
                .key(self.record_key(stream_id))
                .arg(&cutoff)
                .arg(MUTATION_BATCH)
                .invoke_async(&mut connection)
                .await
                .map_err(map_snapshot_error)?;
            if removed == 0 {
                return Ok(());
            }
        }
    }
}

#[async_trait]
impl<S, C> SnapshotStore for RedisEnhancedSnapshots<S, C>
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
        let snapshot = Snapshot::from_shared(
            snapshot.stream_id(),
            state,
            snapshot.version(),
            snapshot.timestamp(),
        );
        let payload = self.encode_snapshot(&snapshot)?;
        let mut connection = self.connection.clone();
        let saved: i64 = Script::new(SAVE)
            .key(self.version_key(snapshot.stream_id()))
            .key(self.record_key(snapshot.stream_id()))
            .arg(version_member(snapshot.version()))
            .arg(payload)
            .invoke_async(&mut connection)
            .await
            .map_err(map_snapshot_error)?;
        if saved == 1 {
            Ok(())
        } else {
            Err(CatgaError::new(
                ErrorCode::Conflict,
                "a newer snapshot already exists for this stream",
            ))
        }
    }

    async fn load<T>(&self, stream_id: &str) -> CatgaResult<Option<Snapshot<T>>>
    where
        T: Send + Sync + 'static,
    {
        Self::require_state::<T>()?;
        self.load_record(stream_id, None)
            .await?
            .map(|(version, value)| self.decode_snapshot(stream_id, version, &value))
            .transpose()
    }

    async fn delete(&self, stream_id: &str) -> CatgaResult<()> {
        let mut connection = self.connection.clone();
        Script::new(DELETE)
            .key(self.version_key(stream_id))
            .key(self.record_key(stream_id))
            .invoke_async::<i64>(&mut connection)
            .await
            .map_err(map_snapshot_error)?;
        Ok(())
    }
}

#[async_trait]
impl<S, C> EnhancedSnapshotStore for RedisEnhancedSnapshots<S, C>
where
    S: Send + Sync + 'static,
    C: SnapshotCodec<S>,
{
    async fn load_at_version<T>(
        &self,
        stream_id: &str,
        version: i64,
    ) -> CatgaResult<Option<Snapshot<T>>>
    where
        T: Send + Sync + 'static,
    {
        Self::require_state::<T>()?;
        self.load_record(stream_id, Some(version))
            .await?
            .map(|(saved_version, value)| self.decode_snapshot(stream_id, saved_version, &value))
            .transpose()
    }

    async fn history(&self, stream_id: &str) -> CatgaResult<Vec<SnapshotInfo>> {
        let mut connection = self.connection.clone();
        let fields: Vec<Vec<u8>> = Script::new(HISTORY)
            .key(self.version_key(stream_id))
            .key(self.record_key(stream_id))
            .invoke_async(&mut connection)
            .await
            .map_err(map_snapshot_error)?;
        let mut pairs = fields.chunks_exact(2);
        let mut history = Vec::with_capacity(fields.len() / 2);
        for pair in &mut pairs {
            let version = parse_version_member(&pair[0])?;
            let stored: StoredSnapshot = self.value_codec.decode_value(&pair[1])?;
            history.push(SnapshotInfo::new(
                version,
                from_unix_millis(stored.timestamp_millis)?,
            ));
        }
        if !pairs.remainder().is_empty() {
            return Err(malformed_snapshot());
        }
        Ok(history)
    }

    async fn delete_before_version(&self, stream_id: &str, version: i64) -> CatgaResult<()> {
        self.remove_before(stream_id, version).await
    }

    async fn cleanup(&self, stream_id: &str, keep_count: usize) -> CatgaResult<()> {
        let keep_count = i64::try_from(keep_count).map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "snapshot retention count exceeds Redis integer range",
            )
        })?;
        loop {
            let mut connection = self.connection.clone();
            let removed: i64 = Script::new(CLEANUP)
                .key(self.version_key(stream_id))
                .key(self.record_key(stream_id))
                .arg(keep_count)
                .arg(MUTATION_BATCH)
                .invoke_async(&mut connection)
                .await
                .map_err(map_snapshot_error)?;
            if removed == 0 {
                return Ok(());
            }
        }
    }
}

fn version_member(version: i64) -> String {
    format!("{:016x}", (version as u64) ^ (1_u64 << 63))
}

fn parse_version_member(member: &[u8]) -> CatgaResult<i64> {
    let member = std::str::from_utf8(member).map_err(|_| malformed_snapshot())?;
    let encoded = u64::from_str_radix(member, 16).map_err(|_| malformed_snapshot())?;
    Ok((encoded ^ (1_u64 << 63)) as i64)
}

fn unix_millis(time: SystemTime) -> CatgaResult<i64> {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis()).map_err(|_| time_range_error()),
        Err(error) => i64::try_from(error.duration().as_millis())
            .ok()
            .and_then(|millis| millis.checked_neg())
            .ok_or_else(time_range_error),
    }
}

fn from_unix_millis(millis: i64) -> CatgaResult<SystemTime> {
    let duration = Duration::from_millis(millis.unsigned_abs());
    if millis >= 0 {
        UNIX_EPOCH.checked_add(duration)
    } else {
        UNIX_EPOCH.checked_sub(duration)
    }
    .ok_or_else(time_range_error)
}

fn malformed_snapshot() -> CatgaError {
    CatgaError::new(
        ErrorCode::Internal,
        "Redis enhanced snapshot record is malformed",
    )
}

fn time_range_error() -> CatgaError {
    CatgaError::new(
        ErrorCode::Validation,
        "snapshot timestamp exceeds the supported system time range",
    )
}

fn map_snapshot_error(error: redis::RedisError) -> CatgaError {
    if error.to_string().contains("CATGA_SNAPSHOT_CORRUPT") {
        malformed_snapshot()
    } else {
        map_error(error)
    }
}
