//! JetStream KV persistence for explicitly encoded recoverable DSL step progress.

use std::error::Error as _;

use async_nats::jetstream::{self, kv};
use async_trait::async_trait;
use catga_core::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackSerialize, MemoryPackSerializer,
};
use catga_core::flow::{DslStepProgress, DslStepProgressStore};
use catga_core::hash::sha256_concat_digest;
use catga_core::{CatgaError, CatgaResult, ErrorCode};

use crate::record::{create_record, decode_record};

/// A JetStream KV store for versioned, application-encoded DSL step progress.
///
/// Keys hash the flow identity and step index, keeping user identifiers out of NATS subjects.
/// The provider stores opaque payload bytes unchanged and never attempts to serialize closures.
pub struct NatsDslStepProgress {
    store: kv::Store,
}

impl NatsDslStepProgress {
    /// Connects to `server`, opening or creating the named JetStream KV `bucket`.
    ///
    /// Step progress is kept in one KV bucket shared by every worker of the flow.
    ///
    /// ```no_run
    /// use catga_nats::NatsDslStepProgress;
    ///
    /// # async fn run() -> catga_core::CatgaResult<()> {
    /// let progress = NatsDslStepProgress::connect("nats://127.0.0.1:4222", "app-dsl-progress").await?;
    /// # drop(progress);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(server: &str, bucket: impl Into<Box<str>>) -> CatgaResult<Self> {
        let context = jetstream::new(
            async_nats::connect(server)
                .await
                .map_err(CatgaError::transient)?,
        );
        let bucket = bucket.into();
        let store = crate::kv::open_or_create(&context, bucket.as_ref())
            .await
            .map_err(CatgaError::transient)?;
        Ok(Self { store })
    }

    async fn entry(&self, flow_id: &str, step_index: u32) -> CatgaResult<Option<kv::Entry>> {
        self.store
            .entry(&key(flow_id, step_index))
            .await
            .map_err(CatgaError::transient)
    }

    async fn compare_and_set(&self, key: &str, value: Vec<u8>, revision: u64) -> CatgaResult<bool> {
        match self.store.update(key, value.clone().into(), revision).await {
            Ok(_) => Ok(true),
            Err(error) if is_revision_conflict(&error) => Ok(false),
            Err(error) => {
                let reported = CatgaError::transient(error);
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
                let reported = CatgaError::transient(error);
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
        for _ in 0..crate::kv::MAX_CAS_RETRIES {
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
        Err(crate::kv::cas_error("DSL progress", "delete"))
    }
}

fn key(flow_id: &str, step_index: u32) -> String {
    format!(
        "d{}",
        hex::encode(sha256_concat_digest(&[
            flow_id.as_bytes(),
            &step_index.to_be_bytes()
        ]))
    )
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
