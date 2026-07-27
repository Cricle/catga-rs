//! JetStream KV durable event subscriptions with revision-safe ownership leases.

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
    CatgaError, CatgaResult, ErrorCode, PersistentSubscription, SubscriptionCheckpoint,
    SubscriptionStore,
};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::record::{create_record, decode_record};

const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(30);
const MAX_CAS_RETRIES: usize = 8;

/// A JetStream KV-backed store for persistent subscription definitions, checkpoints, and leases.
///
/// Subscription names and stream identifiers are hashed before becoming KV keys. This avoids the
/// JetStream key-character restrictions and keeps caller-provided identifiers out of broker
/// subjects. A definition and all of its checkpoints share one revisioned record, so deleting a
/// definition cannot leave checkpoint records behind.
pub struct NatsSubscriptions {
    store: kv::Store,
    lease_ttl: Duration,
}

impl NatsSubscriptions {
    /// Connects to `server`, opening or creating the named JetStream KV `bucket`.
    ///
    /// Competing-consumer leases expire after 30 seconds unless [`Self::with_lease_ttl`] is used.
    pub async fn connect(server: &str, bucket: impl Into<Box<str>>) -> CatgaResult<Self> {
        Self::with_lease_ttl(server, bucket, DEFAULT_LEASE_TTL).await
    }

    /// Connects with an explicit positive duration for competing-consumer leases.
    ///
    /// The bucket retains only the latest revision per key because conditional updates, rather
    /// than history traversal, provide the store's concurrency control.
    pub async fn with_lease_ttl(
        server: &str,
        bucket: impl Into<Box<str>>,
        lease_ttl: Duration,
    ) -> CatgaResult<Self> {
        if lease_ttl.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "NATS subscription lease TTL must be greater than zero",
            ));
        }
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
        Ok(Self { store, lease_ttl })
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

    async fn create_subscription(
        &self,
        key: &str,
        subscription: &StoredSubscription,
    ) -> CatgaResult<bool> {
        let record = create_record(&encode(subscription)?);
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

    async fn revive_lease(
        &self,
        key: &str,
        lease: &crate::record::CreatedRecord,
        revision: u64,
    ) -> CatgaResult<bool> {
        match self
            .store
            .update(key, lease.value().to_vec().into(), revision)
            .await
        {
            Ok(_) => Ok(true),
            Err(error) if is_revision_conflict(&error) => Ok(false),
            Err(error) => {
                let reported = map_error(error);
                let committed = match self.store.entry(key).await {
                    Ok(Some(entry)) if matches!(entry.operation, kv::Operation::Put) => {
                        lease.matches(&decode_record(&entry.value)?)
                    }
                    _ => false,
                };
                if committed { Ok(true) } else { Err(reported) }
            }
        }
    }

    async fn delete_lease(&self, subscription_name: &str) -> CatgaResult<()> {
        let key = lease_key(subscription_name);
        let Some(entry) = self.entry(&key).await? else {
            return Ok(());
        };
        if matches!(
            entry.operation,
            kv::Operation::Delete | kv::Operation::Purge
        ) {
            return Ok(());
        }
        self.store
            .delete_expect_revision(&key, Some(entry.revision))
            .await
            .map_err(map_error)
    }
}

#[async_trait]
impl SubscriptionStore for NatsSubscriptions {
    async fn save(&self, subscription: PersistentSubscription) -> CatgaResult<()> {
        let key = definition_key(subscription.name());
        let desired = StoredSubscription::from(subscription);
        for _ in 0..MAX_CAS_RETRIES {
            let Some(entry) = self.entry(&key).await? else {
                if self.create_subscription(&key, &desired).await? {
                    return Ok(());
                }
                continue;
            };
            if matches!(
                entry.operation,
                kv::Operation::Delete | kv::Operation::Purge
            ) {
                if self.create_subscription(&key, &desired).await? {
                    return Ok(());
                }
                continue;
            }
            let record = decode_record(&entry.value)?;
            let mut next = decode::<StoredSubscription>(record.payload())?;
            next.name = desired.name.clone();
            next.stream_pattern = desired.stream_pattern.clone();
            next.event_types.clone_from(&desired.event_types);
            if self
                .compare_and_set(&key, record.with_payload(&encode(&next)?), entry.revision)
                .await?
            {
                return Ok(());
            }
        }
        Err(cas_error("save"))
    }

    async fn load(&self, name: &str) -> CatgaResult<Option<PersistentSubscription>> {
        let Some(entry) = self.entry(&definition_key(name)).await? else {
            return Ok(None);
        };
        if matches!(
            entry.operation,
            kv::Operation::Delete | kv::Operation::Purge
        ) {
            return Ok(None);
        }
        Ok(Some(
            decode::<StoredSubscription>(decode_record(&entry.value)?.payload())?.into(),
        ))
    }

    async fn delete(&self, name: &str) -> CatgaResult<()> {
        let key = definition_key(name);
        for _ in 0..MAX_CAS_RETRIES {
            let Some(entry) = self.entry(&key).await? else {
                return self.delete_lease(name).await;
            };
            if matches!(
                entry.operation,
                kv::Operation::Delete | kv::Operation::Purge
            ) {
                return self.delete_lease(name).await;
            }
            match self
                .store
                .delete_expect_revision(&key, Some(entry.revision))
                .await
            {
                Ok(()) => return self.delete_lease(name).await,
                Err(error) => {
                    let reported = map_error(error);
                    if self.entry(&key).await?.is_some_and(|latest| {
                        matches!(latest.operation, kv::Operation::Put)
                            && latest.revision != entry.revision
                    }) {
                        continue;
                    }
                    return Err(reported);
                }
            }
        }
        Err(cas_error("delete"))
    }

    async fn list(&self) -> CatgaResult<Vec<PersistentSubscription>> {
        let keys = self
            .store
            .keys()
            .await
            .map_err(map_error)?
            .try_collect::<Vec<_>>()
            .await
            .map_err(map_error)?;
        let mut subscriptions: Vec<PersistentSubscription> = Vec::new();
        for key in keys.into_iter().filter(|key| key.starts_with('d')) {
            let Some(entry) = self.entry(&key).await? else {
                continue;
            };
            if matches!(
                entry.operation,
                kv::Operation::Delete | kv::Operation::Purge
            ) {
                continue;
            }
            subscriptions
                .push(decode::<StoredSubscription>(decode_record(&entry.value)?.payload())?.into());
        }
        subscriptions.sort_unstable_by(|left, right| left.name().cmp(right.name()));
        Ok(subscriptions)
    }

    async fn save_checkpoint(&self, checkpoint: SubscriptionCheckpoint) -> CatgaResult<()> {
        let key = definition_key(checkpoint.subscription_name());
        for _ in 0..MAX_CAS_RETRIES {
            let Some(entry) = self.entry(&key).await? else {
                return Err(CatgaError::new(
                    ErrorCode::NotFound,
                    "NATS subscription does not exist",
                ));
            };
            if matches!(
                entry.operation,
                kv::Operation::Delete | kv::Operation::Purge
            ) {
                return Err(CatgaError::new(
                    ErrorCode::NotFound,
                    "NATS subscription does not exist",
                ));
            }
            let record = decode_record(&entry.value)?;
            let mut subscription = decode::<StoredSubscription>(record.payload())?;
            subscription.save_checkpoint(checkpoint.clone());
            if self
                .compare_and_set(
                    &key,
                    record.with_payload(&encode(&subscription)?),
                    entry.revision,
                )
                .await?
            {
                return Ok(());
            }
        }
        Err(cas_error("save checkpoint"))
    }

    async fn load_checkpoint(
        &self,
        subscription_name: &str,
        stream_id: &str,
    ) -> CatgaResult<Option<SubscriptionCheckpoint>> {
        let Some(entry) = self.entry(&definition_key(subscription_name)).await? else {
            return Ok(None);
        };
        if matches!(
            entry.operation,
            kv::Operation::Delete | kv::Operation::Purge
        ) {
            return Ok(None);
        }
        let subscription = decode::<StoredSubscription>(decode_record(&entry.value)?.payload())?;
        Ok(subscription
            .checkpoints
            .into_iter()
            .find(|checkpoint| checkpoint.stream_id.as_ref() == stream_id)
            .map(|checkpoint| {
                SubscriptionCheckpoint::new(subscription_name, stream_id, checkpoint.version)
            }))
    }

    async fn try_acquire(&self, subscription_name: &str, consumer_id: &str) -> CatgaResult<bool> {
        let key = lease_key(subscription_name);
        let lease = LeaseRecord::new(consumer_id, self.lease_ttl);
        let created = create_record(&encode(&lease)?);
        for _ in 0..MAX_CAS_RETRIES {
            match self
                .store
                .update(&key, created.value().to_vec().into(), 0)
                .await
            {
                Ok(_) => return Ok(true),
                Err(error) if is_revision_conflict(&error) => {}
                Err(error) => {
                    let reported = map_error(error);
                    let committed = match self.store.entry(&key).await {
                        Ok(Some(entry)) if matches!(entry.operation, kv::Operation::Put) => {
                            created.matches(&decode_record(&entry.value)?)
                        }
                        _ => false,
                    };
                    if committed {
                        return Ok(true);
                    }
                    return Err(reported);
                }
            }
            let Some(entry) = self.entry(&key).await? else {
                continue;
            };
            if matches!(
                entry.operation,
                kv::Operation::Delete | kv::Operation::Purge
            ) {
                if self.revive_lease(&key, &created, entry.revision).await? {
                    return Ok(true);
                }
                continue;
            }
            let record = decode_record(&entry.value)?;
            let current = decode::<LeaseRecord>(record.payload())?;
            if !current.is_expired() {
                return Ok(false);
            }
            if self
                .compare_and_set(&key, record.with_payload(&encode(&lease)?), entry.revision)
                .await?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn release(&self, subscription_name: &str, consumer_id: &str) -> CatgaResult<()> {
        let key = lease_key(subscription_name);
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
            let current = decode::<LeaseRecord>(decode_record(&entry.value)?.payload())?;
            if current.owner.as_ref() != consumer_id {
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
        Err(cas_error("release lease"))
    }
}

#[derive(Clone, Deserialize, MemoryPackable, Serialize)]
struct StoredSubscription {
    name: Box<str>,
    stream_pattern: Box<str>,
    event_types: Vec<Box<str>>,
    checkpoints: Vec<StoredCheckpoint>,
}

impl StoredSubscription {
    fn save_checkpoint(&mut self, checkpoint: SubscriptionCheckpoint) {
        if let Some(existing) = self
            .checkpoints
            .iter_mut()
            .find(|existing| existing.stream_id.as_ref() == checkpoint.stream_id())
        {
            existing.version = checkpoint.version();
        } else {
            self.checkpoints.push(StoredCheckpoint {
                stream_id: checkpoint.stream_id().into(),
                version: checkpoint.version(),
            });
        }
    }
}

impl From<PersistentSubscription> for StoredSubscription {
    fn from(subscription: PersistentSubscription) -> Self {
        Self {
            name: subscription.name().into(),
            stream_pattern: subscription.stream_pattern().into(),
            event_types: subscription.event_types().to_vec(),
            checkpoints: Vec::new(),
        }
    }
}

impl From<StoredSubscription> for PersistentSubscription {
    fn from(subscription: StoredSubscription) -> Self {
        PersistentSubscription::new(subscription.name, subscription.stream_pattern)
            .with_event_types(subscription.event_types)
    }
}

#[derive(Clone, Deserialize, MemoryPackable, Serialize)]
struct StoredCheckpoint {
    stream_id: Box<str>,
    version: i64,
}

#[derive(Deserialize, MemoryPackable, Serialize)]
struct LeaseRecord {
    owner: Box<str>,
    expires_at_unix_ms: u64,
}

impl LeaseRecord {
    fn new(owner: impl Into<Box<str>>, ttl: Duration) -> Self {
        Self {
            owner: owner.into(),
            expires_at_unix_ms: now_millis()
                .saturating_add(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX).max(1)),
        }
    }

    fn is_expired(&self) -> bool {
        self.expires_at_unix_ms <= now_millis()
    }
}

fn definition_key(subscription_name: &str) -> String {
    format!("d{:x}", Sha256::digest(subscription_name.as_bytes()))
}

fn lease_key(subscription_name: &str) -> String {
    format!("l{:x}", Sha256::digest(subscription_name.as_bytes()))
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
        format!("NATS subscription {operation} compare-and-set did not stabilize"),
    )
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

fn map_error(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}
