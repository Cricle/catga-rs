//! JetStream KV multi-version aggregate snapshots with bounded revision retries.

use std::{
    any::{Any, TypeId},
    error::Error as _,
    marker::PhantomData,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_nats::jetstream::{self, kv};
use async_trait::async_trait;
use catga_core::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    MemoryPackSerializer, MemoryPackSnapshotCodec, MemoryPackWriter, MemoryPackable,
};
use catga_core::{
    CatgaError, CatgaResult, EnhancedSnapshotStore, ErrorCode, Snapshot, SnapshotCodec,
    SnapshotInfo, SnapshotStore,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::record::{create_record, decode_record};

const MAX_CAS_RETRIES: usize = 8;

/// A JetStream KV store retaining multiple immutable snapshots for one concrete aggregate state.
///
/// Every event stream maps to one SHA-256-derived KV key. Its compact MemoryPack value contains an
/// ordered version history, so reads and cleanup do not enumerate the bucket or expose stream IDs
/// as broker keys. Revision CAS serializes concurrent writers without a process-local lock.
pub struct NatsEnhancedSnapshots<S, C = MemoryPackSnapshotCodec<S>> {
    store: kv::Store,
    codec: C,
    state: PhantomData<fn() -> S>,
}

impl<S> NatsEnhancedSnapshots<S>
where
    S: Send + Sync + 'static,
    MemoryPackSnapshotCodec<S>: SnapshotCodec<S>,
{
    /// Connects with compact MemoryPack encoding for aggregate state `S`.
    pub async fn connect(server: &str, bucket: impl Into<Box<str>>) -> CatgaResult<Self> {
        Self::with_codec(server, bucket, MemoryPackSnapshotCodec::default()).await
    }
}

impl<S, C> NatsEnhancedSnapshots<S, C>
where
    S: Send + Sync + 'static,
    C: SnapshotCodec<S>,
{
    /// Connects using an explicit state codec and opens or creates the named KV `bucket`.
    pub async fn with_codec(
        server: &str,
        bucket: impl Into<Box<str>>,
        codec: C,
    ) -> CatgaResult<Self> {
        let context = jetstream::new(async_nats::connect(server).await.map_err(map_error)?);
        let bucket = bucket.into();
        let store = crate::kv::open_or_create(&context, bucket.as_ref())
            .await
            .map_err(map_error)?;
        Ok(Self {
            store,
            codec,
            state: PhantomData,
        })
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

    async fn entry(&self, key: &str) -> CatgaResult<Option<kv::Entry>> {
        self.store.entry(key).await.map_err(map_error)
    }

    async fn compare_and_set(&self, key: &str, value: Vec<u8>, revision: u64) -> CatgaResult<bool> {
        match self.store.update(key, value.clone().into(), revision).await {
            Ok(_) => Ok(true),
            Err(error) if is_revision_conflict(&error) => Ok(false),
            Err(error) => {
                let reported = map_error(error);
                let committed = matches!(
                    self.store.entry(key).await,
                    Ok(Some(entry))
                        if matches!(entry.operation, kv::Operation::Put)
                            && entry.value.as_ref() == value.as_slice()
                );
                if committed { Ok(true) } else { Err(reported) }
            }
        }
    }

    async fn create(&self, key: &str, history: &StoredHistory) -> CatgaResult<bool> {
        let record = create_record(&encode(history)?);
        match self
            .store
            .update(key, record.value().to_vec().into(), 0)
            .await
        {
            Ok(_) => Ok(true),
            Err(error) if is_revision_conflict(&error) => Ok(false),
            Err(error) => {
                let reported = map_error(error);
                let committed = match self.store.entry(key).await {
                    Ok(Some(entry)) if matches!(entry.operation, kv::Operation::Put) => {
                        record.matches(&decode_record(&entry.value)?)
                    }
                    _ => false,
                };
                if committed { Ok(true) } else { Err(reported) }
            }
        }
    }

    fn entry_from_snapshot(&self, snapshot: &Snapshot<S>) -> CatgaResult<StoredSnapshot> {
        Ok(StoredSnapshot {
            version: snapshot.version(),
            timestamp_unix_ms: unix_millis(snapshot.timestamp()),
            state: self.codec.encode_state(snapshot.state())?,
        })
    }

    fn decode_snapshot<T>(
        &self,
        stream_id: &str,
        snapshot: StoredSnapshot,
    ) -> CatgaResult<Snapshot<T>>
    where
        T: Send + Sync + 'static,
    {
        let state: Arc<dyn Any + Send + Sync> = Arc::new(self.codec.decode_state(&snapshot.state)?);
        let state = Arc::downcast::<T>(state).map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "the stored snapshot state does not match the requested type",
            )
        })?;
        Ok(Snapshot::from_shared(
            stream_id,
            state,
            snapshot.version,
            UNIX_EPOCH + Duration::from_millis(snapshot.timestamp_unix_ms),
        ))
    }

    async fn history(&self, stream_id: &str) -> CatgaResult<Option<(kv::Entry, StoredHistory)>> {
        let Some(entry) = self.entry(&stream_key(stream_id)).await? else {
            return Ok(None);
        };
        if matches!(
            entry.operation,
            kv::Operation::Delete | kv::Operation::Purge
        ) {
            return Ok(None);
        }
        let history = decode::<StoredHistory>(decode_record(&entry.value)?.payload())?;
        Ok(Some((entry, history)))
    }
}

#[async_trait]
impl<S, C> SnapshotStore for NatsEnhancedSnapshots<S, C>
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
        let key = stream_key(snapshot.stream_id());
        let next = self.entry_from_snapshot(&snapshot)?;
        for _ in 0..MAX_CAS_RETRIES {
            let Some(entry) = self.entry(&key).await? else {
                if self
                    .create(&key, &StoredHistory::with(next.clone()))
                    .await?
                {
                    return Ok(());
                }
                continue;
            };
            if matches!(
                entry.operation,
                kv::Operation::Delete | kv::Operation::Purge
            ) {
                if self
                    .create(&key, &StoredHistory::with(next.clone()))
                    .await?
                {
                    return Ok(());
                }
                continue;
            }
            let record = decode_record(&entry.value)?;
            let mut history = decode::<StoredHistory>(record.payload())?;
            if history
                .entries
                .last()
                .is_some_and(|latest| latest.version > next.version)
            {
                return Err(CatgaError::new(
                    ErrorCode::Conflict,
                    "a newer snapshot already exists for this stream",
                ));
            }
            history.upsert(next.clone());
            if self
                .compare_and_set(
                    &key,
                    record.with_payload(&encode(&history)?),
                    entry.revision,
                )
                .await?
            {
                return Ok(());
            }
        }
        Err(cas_error("save"))
    }

    async fn load<T>(&self, stream_id: &str) -> CatgaResult<Option<Snapshot<T>>>
    where
        T: Send + Sync + 'static,
    {
        Self::require_state::<T>()?;
        let Some((_, history)) = self.history(stream_id).await? else {
            return Ok(None);
        };
        history
            .entries
            .last()
            .cloned()
            .map(|snapshot| self.decode_snapshot(stream_id, snapshot))
            .transpose()
    }

    async fn delete(&self, stream_id: &str) -> CatgaResult<()> {
        let key = stream_key(stream_id);
        for _ in 0..MAX_CAS_RETRIES {
            let Some(entry) = self.entry(&key).await? else {
                return Ok(());
            };
            if matches!(
                entry.operation,
                kv::Operation::Delete | kv::Operation::Purge
            ) {
                return Ok(());
            }
            if self
                .store
                .delete_expect_revision(&key, Some(entry.revision))
                .await
                .is_ok()
            {
                return Ok(());
            }
        }
        Err(cas_error("delete"))
    }
}

#[async_trait]
impl<S, C> EnhancedSnapshotStore for NatsEnhancedSnapshots<S, C>
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
        let Some((_, history)) = self.history(stream_id).await? else {
            return Ok(None);
        };
        history
            .entries
            .iter()
            .rev()
            .find(|snapshot| snapshot.version <= version)
            .cloned()
            .map(|snapshot| self.decode_snapshot(stream_id, snapshot))
            .transpose()
    }

    async fn history(&self, stream_id: &str) -> CatgaResult<Vec<SnapshotInfo>> {
        let Some((_, history)) = self.history(stream_id).await? else {
            return Ok(Vec::new());
        };
        Ok(history
            .entries
            .into_iter()
            .map(|snapshot| {
                SnapshotInfo::new(
                    snapshot.version,
                    UNIX_EPOCH + Duration::from_millis(snapshot.timestamp_unix_ms),
                )
            })
            .collect())
    }

    async fn delete_before_version(&self, stream_id: &str, version: i64) -> CatgaResult<()> {
        self.mutate(stream_id, |history| {
            let previous = history.entries.len();
            history
                .entries
                .retain(|snapshot| snapshot.version >= version);
            previous != history.entries.len()
        })
        .await
    }

    async fn cleanup(&self, stream_id: &str, keep_count: usize) -> CatgaResult<()> {
        self.mutate(stream_id, |history| {
            if history.entries.len() <= keep_count {
                return false;
            }
            let first = history.entries.len().saturating_sub(keep_count);
            history.entries.drain(..first);
            true
        })
        .await
    }
}

impl<S, C> NatsEnhancedSnapshots<S, C>
where
    S: Send + Sync + 'static,
    C: SnapshotCodec<S>,
{
    async fn mutate(
        &self,
        stream_id: &str,
        transform: impl Fn(&mut StoredHistory) -> bool,
    ) -> CatgaResult<()> {
        let key = stream_key(stream_id);
        for _ in 0..MAX_CAS_RETRIES {
            let Some(entry) = self.entry(&key).await? else {
                return Ok(());
            };
            if matches!(
                entry.operation,
                kv::Operation::Delete | kv::Operation::Purge
            ) {
                return Ok(());
            }
            let record = decode_record(&entry.value)?;
            let mut history = decode::<StoredHistory>(record.payload())?;
            if !transform(&mut history) {
                return Ok(());
            }
            if history.entries.is_empty() {
                if self
                    .store
                    .delete_expect_revision(&key, Some(entry.revision))
                    .await
                    .is_ok()
                {
                    return Ok(());
                }
            } else if self
                .compare_and_set(
                    &key,
                    record.with_payload(&encode(&history)?),
                    entry.revision,
                )
                .await?
            {
                return Ok(());
            }
        }
        Err(cas_error("mutate"))
    }
}

#[derive(Deserialize, MemoryPackable, Serialize)]
struct StoredHistory {
    entries: Vec<StoredSnapshot>,
}

impl StoredHistory {
    fn with(snapshot: StoredSnapshot) -> Self {
        Self {
            entries: vec![snapshot],
        }
    }

    fn upsert(&mut self, snapshot: StoredSnapshot) {
        match self
            .entries
            .binary_search_by_key(&snapshot.version, |entry| entry.version)
        {
            Ok(index) => self.entries[index] = snapshot,
            Err(index) => self.entries.insert(index, snapshot),
        }
    }
}

#[derive(Clone, Deserialize, MemoryPackable, Serialize)]
struct StoredSnapshot {
    version: i64,
    timestamp_unix_ms: u64,
    state: Vec<u8>,
}

fn stream_key(stream_id: &str) -> String {
    format!("s{}", hex::encode(Sha256::digest(stream_id.as_bytes())))
}

fn encode<T: MemoryPackSerialize>(value: &T) -> CatgaResult<Vec<u8>> {
    MemoryPackSerializer::serialize(value)
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))
}

fn decode<T: MemoryPackDeserialize>(value: &[u8]) -> CatgaResult<T> {
    MemoryPackSerializer::deserialize(value)
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))
}

fn is_revision_conflict(error: &kv::UpdateError) -> bool {
    error
        .source()
        .and_then(|source| source.downcast_ref::<jetstream::context::PublishError>())
        .is_some_and(|source| {
            source.kind() == jetstream::context::PublishErrorKind::WrongLastSequence
        })
}

fn cas_error(operation: &str) -> CatgaError {
    CatgaError::new(
        ErrorCode::Transient,
        format!("NATS enhanced snapshot {operation} compare-and-set did not stabilize"),
    )
}

fn unix_millis(time: SystemTime) -> u64 {
    u64::try_from(
        time.duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

fn map_error(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}

