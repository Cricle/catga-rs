use std::{
    collections::BinaryHeap,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(feature = "test-hooks")]
use std::sync::{Arc, Barrier, Mutex};

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, DEFAULT_IDEMPOTENCY_RETENTION, DEFAULT_OUTBOX_CLAIM_LEASE, ErrorCode,
    OutboxMessage, OutboxState, OutboxStore, outbox_claim_expires_at, telemetry,
    validate_completed_retention, validate_outbox_claim_limit, validate_outbox_message_id,
};
use dashmap::{DashMap, mapref::entry::Entry};

use crate::{
    DEFAULT_MEMORY_RECORD_CAPACITY,
    capacity::{OPPORTUNISTIC_CLEANUP_LIMIT, RecordCapacity, RecordSequence},
};

struct StoredMessage {
    identity: u64,
    message: OutboxMessage,
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct PublishedRecord {
    published_at: u64,
    record_identity: u64,
}

/// A shard-locked, process-local outbox for development and deterministic tests.
///
/// ```
/// use catga_memory::MemoryOutbox;
///
/// let outbox = MemoryOutbox::default();
/// # let _ = outbox;
/// ```
pub struct MemoryOutbox {
    messages: DashMap<u64, StoredMessage>,
    published: DashMap<u64, PublishedRecord>,
    claim_sequence: AtomicU64,
    record_sequence: RecordSequence,
    capacity: RecordCapacity,
    published_retention: Duration,
    #[cfg(feature = "test-hooks")]
    cleanup_after_snapshot: Mutex<Option<(Arc<Barrier>, Arc<Barrier>)>>,
}

impl Default for MemoryOutbox {
    fn default() -> Self {
        Self {
            messages: DashMap::new(),
            published: DashMap::new(),
            claim_sequence: AtomicU64::new(0),
            record_sequence: RecordSequence::new(),
            capacity: RecordCapacity::fixed(DEFAULT_MEMORY_RECORD_CAPACITY),
            published_retention: DEFAULT_IDEMPOTENCY_RETENTION,
            #[cfg(feature = "test-hooks")]
            cleanup_after_snapshot: Mutex::new(None),
        }
    }
}

impl MemoryOutbox {
    /// Creates an outbox with a fixed maximum number of retained records.
    pub fn new(capacity: usize) -> CatgaResult<Self> {
        Self::with_published_retention_and_capacity(DEFAULT_IDEMPOTENCY_RETENTION, capacity)
    }

    /// Creates an outbox retaining published records for `retention` within `capacity` records.
    pub fn with_published_retention_and_capacity(
        retention: Duration,
        capacity: usize,
    ) -> CatgaResult<Self> {
        validate_completed_retention(retention)?;
        Ok(Self {
            messages: DashMap::with_capacity(capacity),
            // Published records are created only after an acknowledgement. Avoid reserving a
            // second full index for the common pending-only backlog; DashMap grows this index if
            // acknowledgements later need it, without changing the shared record capacity.
            published: DashMap::new(),
            claim_sequence: AtomicU64::new(0),
            record_sequence: RecordSequence::new(),
            capacity: RecordCapacity::new(capacity)?,
            published_retention: retention,
            #[cfg(feature = "test-hooks")]
            cleanup_after_snapshot: Mutex::new(None),
        })
    }

    fn next_claim_token(&self) -> Box<str> {
        format!(
            "memory-{}",
            self.claim_sequence.fetch_add(1, Ordering::Relaxed)
        )
        .into()
    }

    #[doc(hidden)]
    #[cfg(feature = "test-hooks")]
    pub fn pause_next_cleanup_after_snapshot(
        &self,
        observed: Arc<Barrier>,
        resume: Arc<Barrier>,
    ) -> CatgaResult<()> {
        let mut hook = self.cleanup_after_snapshot.lock().map_err(|_| {
            CatgaError::new(
                ErrorCode::Internal,
                "memory outbox cleanup test hook is poisoned",
            )
        })?;
        *hook = Some((observed, resume));
        Ok(())
    }

    #[cfg(feature = "test-hooks")]
    fn wait_after_cleanup_snapshot(&self) -> CatgaResult<()> {
        let barriers = self
            .cleanup_after_snapshot
            .lock()
            .map_err(|_| {
                CatgaError::new(
                    ErrorCode::Internal,
                    "memory outbox cleanup test hook is poisoned",
                )
            })?
            .take();
        if let Some((observed, resume)) = barriers {
            observed.wait();
            resume.wait();
        }
        Ok(())
    }
}

#[async_trait]
impl OutboxStore for MemoryOutbox {
    async fn enqueue(&self, message: OutboxMessage) -> CatgaResult<()> {
        let mut operation = telemetry::persistence_operation("memory", "outbox", "enqueue");
        let id = message.id();
        if let Err(error) = validate_outbox_message_id(id) {
            let result = Err(error);
            operation.complete(&result);
            return result;
        }
        if !self.capacity.reserve() {
            if let Err(error) = self
                .cleanup_published(self.published_retention, OPPORTUNISTIC_CLEANUP_LIMIT)
                .await
            {
                let result = Err(error);
                operation.complete(&result);
                return result;
            }
            if !self.capacity.reserve() {
                let result = Err(CatgaError::new(
                    ErrorCode::Unavailable,
                    "memory outbox record capacity is exhausted",
                ));
                operation.complete(&result);
                return result;
            }
        }
        let result = match self.messages.entry(id) {
            Entry::Vacant(entry) => match self.record_sequence.next() {
                Ok(identity) => {
                    entry.insert(StoredMessage { identity, message });
                    Ok(())
                }
                Err(error) => {
                    self.capacity.release();
                    Err(error)
                }
            },
            Entry::Occupied(_) => {
                self.capacity.release();
                Err(CatgaError::new(
                    ErrorCode::Conflict,
                    "an outbox message with this identifier already exists",
                ))
            }
        };
        operation.complete(&result);
        result
    }

    async fn claim(&self, owner: &str, limit: usize) -> CatgaResult<Vec<OutboxMessage>> {
        self.claim_for(owner, limit, DEFAULT_OUTBOX_CLAIM_LEASE)
            .await
    }

    async fn claim_for(
        &self,
        owner: &str,
        limit: usize,
        lease: Duration,
    ) -> CatgaResult<Vec<OutboxMessage>> {
        let mut operation = telemetry::persistence_operation("memory", "outbox", "claim");
        let result = (|| {
            validate_outbox_claim_limit(limit)?;
            let expires_at = outbox_claim_expires_at(lease)?;
            if limit == 0 {
                return Ok(Vec::new());
            }

            let now = SystemTime::now();
            let now_unix_ms = current_unix_ms()?;
            // Retain only the oldest eligible records. The heap's largest key is
            // discarded whenever an earlier candidate is found, avoiding a full
            // backlog copy merely to honor the source outbox creation ordering.
            let mut candidates = BinaryHeap::with_capacity(limit);
            for entry in self.messages.iter() {
                let message = &entry.value().message;
                if !message.is_claimable_at(now_unix_ms) || !message.is_due_at(now) {
                    continue;
                }
                let order = (
                    message.envelope().sent_at_unix_ms().unwrap_or(0),
                    message.id(),
                );
                if candidates.len() < limit {
                    candidates.push(order);
                } else if candidates.peek().is_some_and(|latest| order < *latest) {
                    candidates.pop();
                    candidates.push(order);
                }
            }

            let mut claimed = Vec::with_capacity(candidates.len());
            for (_, id) in candidates.into_sorted_vec() {
                let Some(mut entry) = self.messages.get_mut(&id) else {
                    continue;
                };
                if entry.message.is_claimable_at(now_unix_ms) && entry.message.is_due_at(now) {
                    entry.message.claim_until_with_token(
                        owner,
                        self.next_claim_token(),
                        expires_at,
                    );
                    claimed.push(entry.message.clone());
                }
            }
            Ok(claimed)
        })();
        operation.complete(&result);
        result
    }

    async fn ack(&self, owner: &str, id: u64, claim_token: &str) -> CatgaResult<()> {
        let mut operation = telemetry::persistence_operation("memory", "outbox", "ack");
        let result = (|| {
            let published_at = current_unix_ms()?;
            if let Some(mut message) = self.messages.get_mut(&id)
                && message.message.owner() == Some(owner)
                && message.message.claim_token() == Some(claim_token)
            {
                message.message.mark_published(published_at);
                self.published.insert(
                    id,
                    PublishedRecord {
                        published_at,
                        record_identity: message.identity,
                    },
                );
            }
            Ok(())
        })();
        operation.complete(&result);
        result
    }

    async fn release(&self, owner: &str, id: u64, claim_token: &str) -> CatgaResult<()> {
        let mut operation = telemetry::persistence_operation("memory", "outbox", "release");
        if let Some(mut message) = self.messages.get_mut(&id)
            && message.message.owner() == Some(owner)
            && message.message.claim_token() == Some(claim_token)
        {
            message.message.release();
        }
        let result = Ok(());
        operation.complete(&result);
        result
    }

    async fn record_failure(
        &self,
        owner: &str,
        id: u64,
        claim_token: &str,
        reason: &str,
    ) -> CatgaResult<()> {
        let mut operation = telemetry::persistence_operation("memory", "outbox", "failure");
        if let Some(mut message) = self.messages.get_mut(&id)
            && message.message.owner() == Some(owner)
            && message.message.claim_token() == Some(claim_token)
        {
            message.message.record_failure(reason);
        }
        let result = Ok(());
        operation.complete(&result);
        result
    }

    async fn cancel(&self, id: u64) -> CatgaResult<bool> {
        let mut operation = telemetry::persistence_operation("memory", "outbox", "cancel");
        let result = (|| {
            let Entry::Occupied(entry) = self.messages.entry(id) else {
                return Ok(false);
            };
            if entry.get().message.state() != OutboxState::Pending {
                return Ok(false);
            }
            entry.remove();
            self.capacity.release();
            Ok(true)
        })();
        operation.complete(&result);
        result
    }

    async fn list_published(&self, limit: usize) -> CatgaResult<Vec<OutboxMessage>> {
        let mut operation = telemetry::persistence_operation("memory", "outbox", "list_published");
        let result = (|| {
            validate_outbox_claim_limit(limit)?;
            let mut records: Vec<_> = self
                .published
                .iter()
                .take(limit)
                .filter_map(|entry| {
                    self.messages
                        .get(entry.key())
                        .map(|message| message.message.clone())
                })
                .collect();
            records.sort_unstable_by_key(OutboxMessage::published_at_unix_ms);
            Ok(records)
        })();
        operation.complete(&result);
        result
    }

    async fn cleanup_published(&self, retention: Duration, limit: usize) -> CatgaResult<usize> {
        let mut operation =
            telemetry::persistence_operation("memory", "outbox", "cleanup_published");
        let result = (|| {
            catga_core::validate_retention_cleanup_limit(limit)?;
            let retention = u64::try_from(retention.as_millis()).map_err(|_| {
                CatgaError::new(
                    ErrorCode::Validation,
                    "outbox retention exceeds the supported millisecond range",
                )
            })?;
            let now = current_unix_ms()?;
            let candidates: Vec<(u64, PublishedRecord)> = self
                .published
                .iter()
                .take(limit)
                .filter(|entry| now.saturating_sub(entry.value().published_at) >= retention)
                .map(|entry| (*entry.key(), *entry.value()))
                .collect();
            #[cfg(feature = "test-hooks")]
            self.wait_after_cleanup_snapshot()?;
            let mut removed = 0;
            for (id, candidate) in candidates {
                if self
                    .messages
                    .remove_if(&id, |_, message| {
                        message.identity == candidate.record_identity
                            && message.message.state() == OutboxState::Published
                    })
                    .is_some()
                {
                    self.capacity.release();
                    removed += 1;
                }
                self.published
                    .remove_if(&id, |_, current| *current == candidate);
            }
            Ok(removed)
        })();
        operation.complete(&result);
        result
    }
}

fn current_unix_ms() -> CatgaResult<u64> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| {
        CatgaError::new(ErrorCode::Internal, "system clock precedes the Unix epoch")
    })?;
    u64::try_from(elapsed.as_millis()).map_err(|_| {
        CatgaError::new(
            ErrorCode::Internal,
            "system clock exceeds the supported millisecond range",
        )
    })
}
