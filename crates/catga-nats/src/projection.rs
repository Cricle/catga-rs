//! JetStream KV projection checkpoints with per-projection revision CAS.

use std::{
    error::Error as _,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_nats::jetstream::{self, kv};
use async_trait::async_trait;
use catga_codec_memorypack::{
    MemoryPackDeserialize, MemoryPackSerialize, MemoryPackSerializer, MemoryPackable,
};
use catga_core::{
    CatgaError, CatgaResult, ErrorCode, ProjectionCheckpoint, ProjectionCheckpointStore,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::record::{create_record, decode_record};

const MAX_CAS_RETRIES: usize = 8;

/// A JetStream KV-backed projection checkpoint store.
///
/// Each projection is stored under a SHA-256-derived key and contains its stream checkpoints in
/// one compact MemoryPack record. This makes deleting every checkpoint for a projection one
/// revision-checked operation and avoids exposing caller-provided names as KV keys.
pub struct NatsProjectionCheckpoints {
    store: kv::Store,
}

impl NatsProjectionCheckpoints {
    /// Connects to `server`, opening or creating the named JetStream KV `bucket`.
    ///
    /// The bucket keeps one history entry because the store performs bounded compare-and-set
    /// retries instead of retaining old checkpoint versions.
    pub async fn connect(server: &str, bucket: impl Into<Box<str>>) -> CatgaResult<Self> {
        let context = jetstream::new(async_nats::connect(server).await.map_err(map_error)?);
        let bucket = bucket.into();
        let store = match context.get_key_value(bucket.as_ref()).await {
            Ok(store) => store,
            Err(_) => match context
                .create_key_value(kv::Config {
                    bucket: bucket.to_string(),
                    history: 1,
                    ..Default::default()
                })
                .await
            {
                Ok(store) => store,
                Err(_) => context
                    .get_key_value(bucket.as_ref())
                    .await
                    .map_err(map_error)?,
            },
        };
        Ok(Self { store })
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

    async fn create(&self, key: &str, checkpoints: &StoredCheckpoints) -> CatgaResult<bool> {
        let record = create_record(&encode(checkpoints)?);
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
}

#[async_trait]
impl ProjectionCheckpointStore for NatsProjectionCheckpoints {
    async fn save(&self, checkpoint: ProjectionCheckpoint) -> CatgaResult<()> {
        let key = projection_key(checkpoint.projection_name());
        for _ in 0..MAX_CAS_RETRIES {
            let Some(entry) = self.entry(&key).await? else {
                if self
                    .create(&key, &StoredCheckpoints::with(checkpoint.clone()))
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
                    .create(&key, &StoredCheckpoints::with(checkpoint.clone()))
                    .await?
                {
                    return Ok(());
                }
                continue;
            }
            let record = decode_record(&entry.value)?;
            let mut checkpoints = decode::<StoredCheckpoints>(record.payload())?;
            checkpoints.save(checkpoint.clone());
            if self
                .compare_and_set(
                    &key,
                    record.with_payload(&encode(&checkpoints)?),
                    entry.revision,
                )
                .await?
            {
                return Ok(());
            }
        }
        Err(cas_error("save"))
    }

    async fn load(
        &self,
        projection_name: &str,
        stream_id: &str,
    ) -> CatgaResult<Option<ProjectionCheckpoint>> {
        let Some(entry) = self.entry(&projection_key(projection_name)).await? else {
            return Ok(None);
        };
        if matches!(
            entry.operation,
            kv::Operation::Delete | kv::Operation::Purge
        ) {
            return Ok(None);
        }
        let checkpoints = decode::<StoredCheckpoints>(decode_record(&entry.value)?.payload())?;
        Ok(checkpoints
            .checkpoints
            .into_iter()
            .find(|checkpoint| checkpoint.stream_id.as_ref() == stream_id)
            .map(|checkpoint| checkpoint.into_checkpoint(projection_name)))
    }

    async fn delete(&self, projection_name: &str, stream_id: &str) -> CatgaResult<()> {
        let key = projection_key(projection_name);
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
            let mut checkpoints = decode::<StoredCheckpoints>(record.payload())?;
            if !checkpoints.remove(stream_id) {
                return Ok(());
            }
            if checkpoints.checkpoints.is_empty() {
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
                    record.with_payload(&encode(&checkpoints)?),
                    entry.revision,
                )
                .await?
            {
                return Ok(());
            }
        }
        Err(cas_error("delete"))
    }

    async fn delete_all(&self, projection_name: &str) -> CatgaResult<()> {
        let key = projection_key(projection_name);
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
        Err(cas_error("delete all"))
    }
}

#[derive(Deserialize, MemoryPackable, Serialize)]
struct StoredCheckpoints {
    checkpoints: Vec<StoredCheckpoint>,
}

impl StoredCheckpoints {
    fn with(checkpoint: ProjectionCheckpoint) -> Self {
        Self {
            checkpoints: vec![StoredCheckpoint::from(checkpoint)],
        }
    }

    fn save(&mut self, checkpoint: ProjectionCheckpoint) {
        if let Some(existing) = self
            .checkpoints
            .iter_mut()
            .find(|existing| existing.stream_id.as_ref() == checkpoint.stream_id())
        {
            *existing = checkpoint.into();
        } else {
            self.checkpoints.push(checkpoint.into());
        }
    }

    fn remove(&mut self, stream_id: &str) -> bool {
        let before = self.checkpoints.len();
        self.checkpoints
            .retain(|checkpoint| checkpoint.stream_id.as_ref() != stream_id);
        self.checkpoints.len() != before
    }
}

#[derive(Deserialize, MemoryPackable, Serialize)]
struct StoredCheckpoint {
    stream_id: Box<str>,
    version: i64,
    updated_at_unix_ms: u64,
}

impl From<ProjectionCheckpoint> for StoredCheckpoint {
    fn from(checkpoint: ProjectionCheckpoint) -> Self {
        Self {
            stream_id: checkpoint.stream_id().into(),
            version: checkpoint.version(),
            updated_at_unix_ms: unix_millis(checkpoint.updated_at()),
        }
    }
}

impl StoredCheckpoint {
    fn into_checkpoint(self, projection_name: &str) -> ProjectionCheckpoint {
        ProjectionCheckpoint::from_persisted(
            projection_name,
            self.stream_id,
            self.version,
            UNIX_EPOCH + Duration::from_millis(self.updated_at_unix_ms),
        )
    }
}

fn projection_key(projection_name: &str) -> String {
    format!(
        "p{}",
        hex::encode(Sha256::digest(projection_name.as_bytes()))
    )
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
        format!("NATS projection checkpoint {operation} compare-and-set did not stabilize"),
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
