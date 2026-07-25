//! JetStream KV durable flow state with a revision-safe type index.

use std::{
    error::Error as _,
    time::{Duration, SystemTime},
};

use async_nats::jetstream::{self, kv};
use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{FlowState, FlowStatus, FlowStore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::record::{create_record, decode_record};

const MAX_CAS_RETRIES: usize = 8;

/// A JetStream KV-backed flow store with a separate, compact flow-type index.
///
/// Flow identities and types are SHA-256-derived keys. Claims only inspect flows indexed for the
/// requested type, rather than scanning the state bucket, while revisions provide cross-process
/// optimistic concurrency.
pub struct NatsFlows {
    states: kv::Store,
    index: kv::Store,
}

impl NatsFlows {
    /// Connects to `server`, provisioning a state bucket named `bucket` and its type index.
    pub async fn connect(server: &str, bucket: impl Into<Box<str>>) -> CatgaResult<Self> {
        let context = jetstream::new(async_nats::connect(server).await.map_err(map_error)?);
        let bucket = bucket.into();
        let states = open_bucket(&context, bucket.as_ref()).await?;
        let index = open_bucket(&context, &format!("{bucket}_IDX")).await?;
        Ok(Self { states, index })
    }

    async fn state_entry(&self, id: &str) -> CatgaResult<Option<kv::Entry>> {
        self.states.entry(&flow_key(id)).await.map_err(map_error)
    }

    async fn get_state(&self, id: &str) -> CatgaResult<Option<(kv::Entry, FlowState)>> {
        let Some(entry) = self.state_entry(id).await? else {
            return Ok(None);
        };
        if matches!(
            entry.operation,
            kv::Operation::Delete | kv::Operation::Purge
        ) {
            return Ok(None);
        }
        let state = decode::<FlowState>(decode_record(&entry.value)?.payload())?;
        Ok(Some((entry, state)))
    }

    async fn index_flow(&self, flow_type: &str, id: &str) -> CatgaResult<()> {
        let key = type_key(flow_type);
        for _ in 0..MAX_CAS_RETRIES {
            let entry = self.index.entry(&key).await.map_err(map_error)?;
            let Some(entry) = entry else {
                let values = vec![Box::<str>::from(id)];
                if create(&self.index, &key, &values).await? {
                    return Ok(());
                }
                continue;
            };
            if matches!(
                entry.operation,
                kv::Operation::Delete | kv::Operation::Purge
            ) {
                let values = vec![Box::<str>::from(id)];
                if create(&self.index, &key, &values).await? {
                    return Ok(());
                }
                continue;
            }
            let record = decode_record(&entry.value)?;
            let mut values = decode::<Vec<Box<str>>>(record.payload())?;
            if values.iter().any(|value| value.as_ref() == id) {
                return Ok(());
            }
            values.push(id.into());
            if compare_and_set(
                &self.index,
                &key,
                record.with_payload(&encode(&values)?),
                entry.revision,
            )
            .await?
            {
                return Ok(());
            }
        }
        Err(cas_error("index flow"))
    }

    async fn replace(
        &self,
        id: &str,
        expected_revision: u64,
        next: &FlowState,
    ) -> CatgaResult<bool> {
        compare_and_set(
            &self.states,
            &flow_key(id),
            encode(next)?,
            expected_revision,
        )
        .await
    }
}

#[async_trait]
impl FlowStore for NatsFlows {
    async fn create(&self, state: FlowState) -> CatgaResult<bool> {
        let key = flow_key(state.id());
        let created = create(&self.states, &key, &state).await?;
        if created {
            self.index_flow(state.flow_type(), state.id()).await?;
            return Ok(true);
        }
        if let Some((_, current)) = self.get_state(state.id()).await?
            && current.flow_type() == state.flow_type()
        {
            self.index_flow(current.flow_type(), current.id()).await?;
        }
        Ok(false)
    }

    async fn update(&self, expected_version: i64, next: FlowState) -> CatgaResult<bool> {
        if next.version() != expected_version.saturating_add(1) {
            return Ok(false);
        }
        let Some((entry, current)) = self.get_state(next.id()).await? else {
            return Ok(false);
        };
        if current.version() != expected_version {
            return Ok(false);
        }
        self.replace(next.id(), entry.revision, &next).await
    }

    async fn get(&self, id: &str) -> CatgaResult<Option<FlowState>> {
        self.get_state(id)
            .await
            .map(|value| value.map(|(_, state)| state))
    }

    async fn try_claim(
        &self,
        flow_type: &str,
        owner: &str,
        stale_after: Duration,
    ) -> CatgaResult<Option<FlowState>> {
        let key = type_key(flow_type);
        let Some(entry) = self.index.entry(&key).await.map_err(map_error)? else {
            return Ok(None);
        };
        if matches!(
            entry.operation,
            kv::Operation::Delete | kv::Operation::Purge
        ) {
            return Ok(None);
        }
        let ids = decode::<Vec<Box<str>>>(decode_record(&entry.value)?.payload())?;
        let now = SystemTime::now();
        for id in ids {
            let Some((entry, current)) = self.get_state(&id).await? else {
                continue;
            };
            if current.flow_type() != flow_type
                || current.status() != FlowStatus::Running
                || !is_stale(current.heartbeat(), now, stale_after)
            {
                continue;
            }
            let next = current.clone().claimed_by(owner).next_version();
            if self.replace(&id, entry.revision, &next).await? {
                return Ok(Some(next));
            }
        }
        Ok(None)
    }

    async fn heartbeat(&self, id: &str, owner: &str, version: i64) -> CatgaResult<bool> {
        let Some((entry, current)) = self.get_state(id).await? else {
            return Ok(false);
        };
        if current.owner() != Some(owner) || current.version() != version {
            return Ok(false);
        }
        self.replace(
            id,
            entry.revision,
            &current.heartbeated_at(SystemTime::now()),
        )
        .await
    }
}

async fn open_bucket(context: &jetstream::Context, bucket: &str) -> CatgaResult<kv::Store> {
    match context.get_key_value(bucket).await {
        Ok(store) => Ok(store),
        Err(_) => match context
            .create_key_value(kv::Config {
                bucket: bucket.to_owned(),
                history: 1,
                ..Default::default()
            })
            .await
        {
            Ok(store) => Ok(store),
            Err(_) => context.get_key_value(bucket).await.map_err(map_error),
        },
    }
}

async fn create<T: Serialize>(store: &kv::Store, key: &str, value: &T) -> CatgaResult<bool> {
    let record = create_record(&encode(value)?);
    match store.update(key, record.value().to_vec().into(), 0).await {
        Ok(_) => Ok(true),
        Err(error) if is_revision_conflict(&error) => Ok(false),
        Err(error) => {
            let reported = map_error(error);
            let committed = match store.entry(key).await {
                Ok(Some(entry)) if matches!(entry.operation, kv::Operation::Put) => {
                    record.matches(&decode_record(&entry.value)?)
                }
                _ => false,
            };
            if committed { Ok(true) } else { Err(reported) }
        }
    }
}

async fn compare_and_set(
    store: &kv::Store,
    key: &str,
    value: Vec<u8>,
    revision: u64,
) -> CatgaResult<bool> {
    match store.update(key, value.clone().into(), revision).await {
        Ok(_) => Ok(true),
        Err(error) if is_revision_conflict(&error) => Ok(false),
        Err(error) => {
            let reported = map_error(error);
            let committed = matches!(store.entry(key).await, Ok(Some(entry)) if matches!(entry.operation, kv::Operation::Put) && entry.value.as_ref() == value.as_slice());
            if committed { Ok(true) } else { Err(reported) }
        }
    }
}

fn flow_key(id: &str) -> String {
    format!("f{:x}", Sha256::digest(id.as_bytes()))
}
fn type_key(flow_type: &str) -> String {
    format!("t{:x}", Sha256::digest(flow_type.as_bytes()))
}
fn encode<T: Serialize>(value: &T) -> CatgaResult<Vec<u8>> {
    postcard::to_allocvec(value)
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))
}
fn decode<T: for<'de> Deserialize<'de>>(value: &[u8]) -> CatgaResult<T> {
    postcard::from_bytes(value)
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
fn is_stale(heartbeat: SystemTime, now: SystemTime, stale_after: Duration) -> bool {
    now.duration_since(heartbeat)
        .is_ok_and(|elapsed| elapsed >= stale_after)
}
fn cas_error(operation: &str) -> CatgaError {
    CatgaError::new(
        ErrorCode::Transient,
        format!("NATS flow {operation} compare-and-set did not stabilize"),
    )
}
fn map_error(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}
