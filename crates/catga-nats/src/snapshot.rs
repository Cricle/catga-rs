//! JetStream KV-backed immutable aggregate snapshots.

use std::{
    any::{Any, TypeId},
    marker::PhantomData,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_nats::jetstream::{self, kv};
use async_trait::async_trait;
use catga_core::codec::memorypack::MemoryPackSnapshotCodec;
use catga_core::{CatgaError, CatgaResult, ErrorCode, Snapshot, SnapshotCodec, SnapshotStore};

const METADATA_BYTES: usize = 16;
const MAX_CAS_RETRIES: usize = 8;

/// JetStream KV-backed latest snapshots for one explicit aggregate state type.
pub struct NatsSnapshotStore<S, C = MemoryPackSnapshotCodec<S>> {
    store: kv::Store,
    codec: C,
    state: PhantomData<fn() -> S>,
}

impl<S> NatsSnapshotStore<S>
where
    S: Send + Sync + 'static,
    MemoryPackSnapshotCodec<S>: SnapshotCodec<S>,
{
    /// Connects using compact MemoryPack state serialization.
    pub async fn connect(server: &str, bucket: impl Into<Box<str>>) -> CatgaResult<Self> {
        Self::with_codec(server, bucket, MemoryPackSnapshotCodec::default()).await
    }
}

impl<S, C> NatsSnapshotStore<S, C>
where
    S: Send + Sync + 'static,
    C: SnapshotCodec<S>,
{
    /// Connects using an explicit state codec.
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

    fn encode(&self, snapshot: &Snapshot<S>) -> CatgaResult<Vec<u8>> {
        let state = self.codec.encode_state(snapshot.state())?;
        let mut value = Vec::with_capacity(METADATA_BYTES.saturating_add(state.len()));
        value.extend_from_slice(&snapshot.version().to_be_bytes());
        value.extend_from_slice(&unix_millis(snapshot.timestamp()).to_be_bytes());
        value.extend_from_slice(&state);
        Ok(value)
    }

    fn decode(&self, value: &[u8]) -> CatgaResult<(i64, SystemTime, S)> {
        if value.len() < METADATA_BYTES {
            return Err(CatgaError::new(
                ErrorCode::Internal,
                "NATS snapshot value is missing metadata",
            ));
        }
        let (version, value) = value.split_at(8);
        let (timestamp, state) = value.split_at(8);
        let version = i64::from_be_bytes(version.try_into().map_err(|_| {
            CatgaError::new(ErrorCode::Internal, "NATS snapshot version is malformed")
        })?);
        let timestamp = u64::from_be_bytes(timestamp.try_into().map_err(|_| {
            CatgaError::new(ErrorCode::Internal, "NATS snapshot timestamp is malformed")
        })?);
        Ok((
            version,
            UNIX_EPOCH + Duration::from_millis(timestamp),
            self.codec.decode_state(state)?,
        ))
    }
}

#[async_trait]
impl<S, C> SnapshotStore for NatsSnapshotStore<S, C>
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
        let value = self.encode(&snapshot)?;
        for _ in 0..MAX_CAS_RETRIES {
            let entry = self
                .store
                .entry(snapshot.stream_id())
                .await
                .map_err(map_error)?;
            let Some(entry) = entry else {
                if self
                    .store
                    .create(snapshot.stream_id(), value.clone().into())
                    .await
                    .is_ok()
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
                    .store
                    .update(snapshot.stream_id(), value.clone().into(), entry.revision)
                    .await
                    .is_ok()
                {
                    return Ok(());
                }
                continue;
            }
            let (current_version, _, _) = self.decode(&entry.value)?;
            if current_version > snapshot.version() {
                return Err(CatgaError::new(
                    ErrorCode::Conflict,
                    "a newer snapshot already exists for this stream",
                ));
            }
            if self
                .store
                .update(snapshot.stream_id(), value.clone().into(), entry.revision)
                .await
                .is_ok()
            {
                return Ok(());
            }
        }
        Err(CatgaError::new(
            ErrorCode::Transient,
            "NATS snapshot compare-and-swap did not stabilize",
        ))
    }

    async fn load<T>(&self, stream_id: &str) -> CatgaResult<Option<Snapshot<T>>>
    where
        T: Send + Sync + 'static,
    {
        Self::require_state::<T>()?;
        let Some(value) = self.store.get(stream_id).await.map_err(map_error)? else {
            return Ok(None);
        };
        let (version, timestamp, state) = self.decode(&value)?;
        let state: Arc<dyn Any + Send + Sync> = Arc::new(state);
        let state = Arc::downcast::<T>(state).map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "the stored snapshot state does not match the requested type",
            )
        })?;
        Ok(Some(Snapshot::from_shared(
            stream_id, state, version, timestamp,
        )))
    }

    async fn delete(&self, stream_id: &str) -> CatgaResult<()> {
        self.store.delete(stream_id).await.map_err(map_error)
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

fn map_error(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}

