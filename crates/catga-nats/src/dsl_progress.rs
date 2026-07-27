//! JetStream KV persistence for explicitly encoded recoverable DSL step progress.

use std::error::Error as _;

use async_nats::jetstream::{self, kv};
use async_trait::async_trait;
use catga_codec_memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackSerialize, MemoryPackSerializer,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{DslStepProgress, DslStepProgressStore};
use sha2::{Digest, Sha256};

use crate::record::{create_record, decode_record};

const MAX_CAS_RETRIES: usize = 8;

/// A JetStream KV store for versioned, application-encoded DSL step progress.
///
/// Keys hash the flow identity and step index, keeping user identifiers out of NATS subjects.
/// The provider stores opaque payload bytes unchanged and never attempts to serialize closures.
pub struct NatsDslStepProgress {
    store: kv::Store,
}

impl NatsDslStepProgress {
    /// Connects to `server`, opening or creating the named JetStream KV `bucket`.
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

    async fn entry(&self, flow_id: &str, step_index: u32) -> CatgaResult<Option<kv::Entry>> {
        self.store
            .entry(&key(flow_id, step_index))
            .await
            .map_err(map_error)
    }

    async fn compare_and_set(&self, key: &str, value: Vec<u8>, revision: u64) -> CatgaResult<bool> {
        match self.store.update(key, value.clone().into(), revision).await {
            Ok(_) => Ok(true),
            Err(error) if is_revision_conflict(&error) => Ok(false),
            Err(error) => {
                let reported = map_error(error);
                let committed = matches!(self.store.entry(key).await, Ok(Some(entry)) if matches!(entry.operation, kv::Operation::Put) && entry.value.as_ref() == value.as_slice());
                if committed { Ok(true) } else { Err(reported) }
            }
        }
    }
}

#[async_trait]
impl DslStepProgressStore for NatsDslStepProgress {
    async fn create(&self, progress: DslStepProgress) -> CatgaResult<bool> {
        let key = key(progress.flow_id(), progress.step_index());
        let record = create_record(&encode(&progress)?);
        match self
            .store
            .update(&key, record.value().to_vec().into(), 0)
            .await
        {
            Ok(_) => Ok(true),
            Err(error) if is_revision_conflict(&error) => Ok(false),
            Err(error) => {
                let reported = map_error(error);
                let committed = match self.store.entry(&key).await {
                    Ok(Some(entry)) if matches!(entry.operation, kv::Operation::Put) => {
                        record.matches(&decode_record(&entry.value)?)
                    }
                    _ => false,
                };
                if committed { Ok(true) } else { Err(reported) }
            }
        }
    }

    async fn update(&self, expected_version: i64, next: DslStepProgress) -> CatgaResult<bool> {
        if !DslStepProgress::is_next_version(expected_version, next.version()) {
            return Ok(false);
        }
        let key = key(next.flow_id(), next.step_index());
        let Some(entry) = self.entry(next.flow_id(), next.step_index()).await? else {
            return Ok(false);
        };
        if matches!(
            entry.operation,
            kv::Operation::Delete | kv::Operation::Purge
        ) {
            return Ok(false);
        }
        let record = decode_record(&entry.value)?;
        let current: DslStepProgress = decode(record.payload())?;
        if current.version() != expected_version {
            return Ok(false);
        }
        self.compare_and_set(&key, record.with_payload(&encode(&next)?), entry.revision)
            .await
    }

    async fn get(&self, flow_id: &str, step_index: u32) -> CatgaResult<Option<DslStepProgress>> {
        let Some(entry) = self.entry(flow_id, step_index).await? else {
            return Ok(None);
        };
        if matches!(
            entry.operation,
            kv::Operation::Delete | kv::Operation::Purge
        ) {
            return Ok(None);
        }
        decode(decode_record(&entry.value)?.payload()).map(Some)
    }

    async fn delete(&self, flow_id: &str, step_index: u32) -> CatgaResult<bool> {
        let key = key(flow_id, step_index);
        for _ in 0..MAX_CAS_RETRIES {
            let Some(entry) = self.entry(flow_id, step_index).await? else {
                return Ok(false);
            };
            if matches!(
                entry.operation,
                kv::Operation::Delete | kv::Operation::Purge
            ) {
                return Ok(false);
            }
            if self
                .store
                .delete_expect_revision(&key, Some(entry.revision))
                .await
                .is_ok()
            {
                return Ok(true);
            }
        }
        Err(CatgaError::new(
            ErrorCode::Transient,
            "NATS DSL progress delete compare-and-set did not stabilize",
        ))
    }
}

fn key(flow_id: &str, step_index: u32) -> String {
    let mut digest = Sha256::new();
    digest.update(flow_id.as_bytes());
    digest.update(step_index.to_be_bytes());
    format!("d{:x}", digest.finalize())
}

fn encode<T: MemoryPackSerialize>(value: &T) -> CatgaResult<Vec<u8>> {
    MemoryPackSerializer::serialize(value).map_err(map_memorypack)
}
fn decode<T: MemoryPackDeserialize>(value: &[u8]) -> CatgaResult<T> {
    MemoryPackSerializer::deserialize(value).map_err(map_memorypack)
}
fn map_memorypack(error: MemoryPackError) -> CatgaError {
    CatgaError::new(ErrorCode::Internal, error.to_string())
}
fn is_revision_conflict(error: &kv::UpdateError) -> bool {
    error
        .source()
        .and_then(|source| source.downcast_ref::<jetstream::context::PublishError>())
        .is_some_and(|source| {
            source.kind() == jetstream::context::PublishErrorKind::WrongLastSequence
        })
}
fn map_error(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}
