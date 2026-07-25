//! JetStream KV suspended flow continuations with revision CAS.

use std::{error::Error as _, time::SystemTime};

use async_nats::jetstream::{self, consumer, consumer::pull, kv};
use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{
    FlowContinuation, FlowQuery, FlowSummary, SuspendedFlowStore, TimedOutFlowPoll,
    TimedOutFlowReceipt, TimedOutFlowStore, decode_continuation, encode_continuation,
};
use futures::TryStreamExt;
use sha2::{Digest, Sha256};

use crate::{
    record::{create_record, decode_record},
    suspended_flow_timeout,
};

const MAX_CAS_RETRIES: usize = 8;

/// JetStream KV-backed suspended flow store using one continuation per revisioned key.
pub struct NatsSuspendedFlows {
    client: async_nats::Client,
    store: kv::Store,
    timeout_consumer: consumer::PullConsumer,
}

impl NatsSuspendedFlows {
    /// Connects and idempotently provisions a one-history KV bucket for suspended flows.
    pub async fn connect(server: &str, bucket: impl Into<Box<str>>) -> CatgaResult<Self> {
        let client = async_nats::connect(server).await.map_err(map_error)?;
        let context = jetstream::new(client.clone());
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
        let stream = context
            .get_stream(&store.stream_name)
            .await
            .map_err(map_error)?;
        let timeout_consumer = stream
            .get_or_create_consumer(
                "catga_flow_timeouts",
                pull::Config {
                    durable_name: Some("catga_flow_timeouts".to_owned()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ack_wait: std::time::Duration::from_secs(30),
                    max_ack_pending: -1,
                    ..Default::default()
                },
            )
            .await
            .map_err(map_error)?;
        Ok(Self {
            client,
            store,
            timeout_consumer,
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

    async fn mutate<F>(&self, flow_id: &str, version: i64, transform: F) -> CatgaResult<bool>
    where
        F: Fn(&FlowContinuation) -> Option<FlowContinuation>,
    {
        let key = kv_key(flow_id);
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
            let current = decode_continuation(record.payload())?;
            if current.state().version() != version {
                return Ok(false);
            }
            let Some(next) = transform(&current) else {
                return Ok(false);
            };
            if next == current {
                return Ok(true);
            }
            if self
                .compare_and_set(
                    &key,
                    record.with_payload(&encode_continuation(&next)?),
                    entry.revision,
                )
                .await?
            {
                return Ok(true);
            }
        }
        Err(CatgaError::new(
            ErrorCode::Transient,
            "NATS suspended-flow compare-and-set did not stabilize",
        ))
    }
}

#[async_trait]
impl SuspendedFlowStore for NatsSuspendedFlows {
    async fn create(&self, continuation: FlowContinuation) -> CatgaResult<bool> {
        let key = kv_key(continuation.state().id());
        let record = create_record(&encode_continuation(&continuation)?);
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

    async fn get(&self, flow_id: &str) -> CatgaResult<Option<FlowContinuation>> {
        let Some(entry) = self.entry(&kv_key(flow_id)).await? else {
            return Ok(None);
        };
        if matches!(
            entry.operation,
            kv::Operation::Delete | kv::Operation::Purge
        ) {
            return Ok(None);
        }
        decode_continuation(decode_record(&entry.value)?.payload()).map(Some)
    }

    async fn query(&self, query: &FlowQuery) -> CatgaResult<Vec<FlowSummary>> {
        let mut keys = self.store.keys().await.map_err(map_error)?;
        let mut summaries = Vec::with_capacity(query.max_results());
        let mut scanned = 0;
        while scanned < query.max_scan() && summaries.len() < query.max_results() {
            let Some(key) = keys.try_next().await.map_err(map_error)? else {
                break;
            };
            scanned = scanned.saturating_add(1);
            let Some(entry) = self.entry(&key).await? else {
                continue;
            };
            if matches!(
                entry.operation,
                kv::Operation::Delete | kv::Operation::Purge
            ) {
                continue;
            }
            let continuation = decode_continuation(decode_record(&entry.value)?.payload())?;
            if query.matches(&continuation) {
                summaries.push(FlowSummary::from_continuation(&continuation));
            }
        }
        Ok(summaries)
    }

    async fn delete(&self, flow_id: &str, expected_version: i64) -> CatgaResult<bool> {
        let key = kv_key(flow_id);
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
            let continuation = decode_continuation(decode_record(&entry.value)?.payload())?;
            if continuation.state().version() != expected_version {
                return Ok(false);
            }
            match self
                .store
                .delete_expect_revision(&key, Some(entry.revision))
                .await
            {
                Ok(()) => return Ok(true),
                Err(error) if is_delete_revision_conflict(&error) => continue,
                Err(error) => return Err(map_error(error)),
            }
        }
        Err(CatgaError::new(
            ErrorCode::Transient,
            "NATS suspended-flow deletion compare-and-set did not stabilize",
        ))
    }

    async fn update(&self, expected_version: i64, next: FlowContinuation) -> CatgaResult<bool> {
        if next.state().version() != expected_version.saturating_add(1) {
            return Ok(false);
        }
        self.mutate(next.state().id(), expected_version, |_| Some(next.clone()))
            .await
    }

    async fn claim(
        &self,
        expected: &FlowContinuation,
        next: FlowContinuation,
    ) -> CatgaResult<bool> {
        if next.state().id() != expected.state().id()
            || next.state().version() != expected.state().version().saturating_add(1)
        {
            return Ok(false);
        }
        let key = kv_key(expected.state().id());
        let next_value = encode_continuation(&next)?;
        for _ in 0..MAX_CAS_RETRIES {
            let Some(entry) = self.entry(&key).await? else {
                return Ok(false);
            };
            let record = decode_record(&entry.value)?;
            if matches!(
                entry.operation,
                kv::Operation::Delete | kv::Operation::Purge
            ) || decode_continuation(record.payload())? != *expected
            {
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
            "NATS suspended-flow claim compare-and-set did not stabilize",
        ))
    }

    async fn record_wait_success(
        &self,
        flow_id: &str,
        version: i64,
        child_id: &str,
        payload: Vec<u8>,
    ) -> CatgaResult<bool> {
        self.mutate(flow_id, version, |current| {
            current.wait().map(|wait| {
                current
                    .clone()
                    .with_wait(wait.record_success(child_id, payload.clone()))
            })
        })
        .await
    }

    async fn record_wait_failure(
        &self,
        flow_id: &str,
        version: i64,
        child_id: &str,
        error: CatgaError,
    ) -> CatgaResult<bool> {
        self.mutate(flow_id, version, |current| {
            current.wait().map(|wait| {
                current
                    .clone()
                    .with_wait(wait.record_failure(child_id, error.clone()))
            })
        })
        .await
    }

    async fn heartbeat(&self, flow_id: &str, owner: &str, version: i64) -> CatgaResult<bool> {
        self.mutate(flow_id, version, |current| {
            (current.state().owner() == Some(owner)).then(|| {
                current
                    .clone()
                    .with_state(current.state().clone().heartbeated_at(SystemTime::now()))
            })
        })
        .await
    }
}

#[async_trait]
impl TimedOutFlowStore for NatsSuspendedFlows {
    async fn poll_timed_out(
        &self,
        poll: &TimedOutFlowPoll,
    ) -> CatgaResult<Vec<TimedOutFlowReceipt>> {
        suspended_flow_timeout::poll(&self.timeout_consumer, poll).await
    }

    async fn ack_timed_out(&self, receipt: &TimedOutFlowReceipt) -> CatgaResult<()> {
        suspended_flow_timeout::ack(&self.client, receipt).await
    }

    async fn release_timed_out(&self, receipt: &TimedOutFlowReceipt) -> CatgaResult<()> {
        suspended_flow_timeout::release(&self.client, receipt).await
    }
}

fn kv_key(flow_id: &str) -> String {
    format!("f{:x}", Sha256::digest(flow_id.as_bytes()))
}

fn is_revision_conflict(error: &kv::UpdateError) -> bool {
    error
        .source()
        .and_then(|source| source.downcast_ref::<jetstream::context::PublishError>())
        .is_some_and(|source| {
            source.kind() == jetstream::context::PublishErrorKind::WrongLastSequence
        })
}

fn is_delete_revision_conflict(error: &kv::DeleteError) -> bool {
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
