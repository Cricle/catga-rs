//! JetStream KV state-machine snapshots with revision CAS.

use std::{error::Error as _, marker::PhantomData};

use async_nats::jetstream::{self, kv};
use async_trait::async_trait;
use catga_codec_memorypack::MemoryPackSnapshotCodec;
use catga_core::{CatgaError, CatgaResult, ErrorCode, SnapshotCodec};
use catga_flow::{
    StateMachineSnapshot, StateMachineStore, decode_state_machine_snapshot,
    encode_state_machine_snapshot,
};
use sha2::{Digest, Sha256};

use crate::record::{create_record, decode_record};

const MAX_CAS_RETRIES: usize = 8;

/// JetStream KV-backed state-machine store using per-key revision CAS.
pub struct NatsStateMachines<S, C = MemoryPackSnapshotCodec<S>> {
    store: kv::Store,
    codec: C,
    state: PhantomData<fn() -> S>,
}

impl<S> NatsStateMachines<S>
where
    S: Send + Sync + 'static,
    MemoryPackSnapshotCodec<S>: SnapshotCodec<S>,
{
    /// Connects with compact MemoryPack state encoding.
    pub async fn connect(server: &str, bucket: impl Into<Box<str>>) -> CatgaResult<Self> {
        Self::with_codec(server, bucket, MemoryPackSnapshotCodec::default()).await
    }
}

impl<S, C> NatsStateMachines<S, C>
where
    S: Send + Sync + 'static,
    C: SnapshotCodec<S>,
{
    /// Connects with an explicit state codec and provisions a one-history KV bucket.
    pub async fn with_codec(
        server: &str,
        bucket: impl Into<Box<str>>,
        codec: C,
    ) -> CatgaResult<Self> {
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
        Ok(Self {
            store,
            codec,
            state: PhantomData,
        })
    }

    async fn entry(&self, key: &str) -> CatgaResult<Option<kv::Entry>> {
        self.store.entry(key).await.map_err(map_error)
    }

    async fn compare_and_set(&self, key: &str, next: Vec<u8>, revision: u64) -> CatgaResult<bool> {
        match self.store.update(key, next.clone().into(), revision).await {
            Ok(_) => Ok(true),
            Err(error) if is_revision_conflict(&error) => Ok(false),
            Err(error) => {
                let reported = map_error(error);
                let committed = matches!(
                    self.store.entry(key).await,
                    Ok(Some(entry))
                        if matches!(entry.operation, kv::Operation::Put)
                            && entry.value.as_ref() == next.as_slice()
                );
                if committed { Ok(true) } else { Err(reported) }
            }
        }
    }
}

#[async_trait]
impl<S, C> StateMachineStore<S> for NatsStateMachines<S, C>
where
    S: Clone + Send + Sync + 'static,
    C: SnapshotCodec<S>,
{
    async fn create(&self, snapshot: StateMachineSnapshot<S>) -> CatgaResult<bool> {
        let key = kv_key(snapshot.instance_id());
        let record = create_record(&encode_state_machine_snapshot(&snapshot, &self.codec)?);
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

    async fn get(&self, instance_id: &str) -> CatgaResult<Option<StateMachineSnapshot<S>>> {
        let Some(entry) = self.entry(&kv_key(instance_id)).await? else {
            return Ok(None);
        };
        if matches!(
            entry.operation,
            kv::Operation::Delete | kv::Operation::Purge
        ) {
            return Ok(None);
        }
        decode_state_machine_snapshot(
            instance_id,
            decode_record(&entry.value)?.payload(),
            &self.codec,
        )
        .map(Some)
    }

    async fn update(
        &self,
        expected_version: i64,
        next: StateMachineSnapshot<S>,
    ) -> CatgaResult<bool> {
        if !StateMachineSnapshot::<S>::is_next_version(expected_version, next.version()) {
            return Ok(false);
        }
        let key = kv_key(next.instance_id());
        let next_value = encode_state_machine_snapshot(&next, &self.codec)?;
        for _ in 0..MAX_CAS_RETRIES {
            let Some(entry) = self.entry(&key).await? else {
                return Ok(false);
            };
            if matches!(
                entry.operation,
                kv::Operation::Delete | kv::Operation::Purge
            ) {
                return Ok(false);
            }
            let record = decode_record(&entry.value)?;
            let current =
                decode_state_machine_snapshot(next.instance_id(), record.payload(), &self.codec)?;
            if current.version() != expected_version {
                return Ok(false);
            }
            if self
                .compare_and_set(&key, record.with_payload(&next_value), entry.revision)
                .await?
            {
                return Ok(true);
            }
        }
        Err(CatgaError::new(
            ErrorCode::Transient,
            "NATS state-machine compare-and-set did not stabilize",
        ))
    }
}

fn kv_key(instance_id: &str) -> String {
    format!("s{:x}", Sha256::digest(instance_id.as_bytes()))
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
