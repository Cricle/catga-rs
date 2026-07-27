use std::{
    collections::BinaryHeap,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, DEFAULT_OUTBOX_CLAIM_LEASE, ErrorCode, OutboxMessage, OutboxState,
    OutboxStore, outbox_claim_expires_at, telemetry, validate_outbox_claim_limit,
    validate_outbox_message_id,
};
use dashmap::{DashMap, mapref::entry::Entry};

use crate::{DEFAULT_MEMORY_RECORD_CAPACITY, capacity::RecordCapacity};

/// A shard-locked, process-local outbox for development and deterministic tests.
pub struct MemoryOutbox {
    messages: DashMap<u64, OutboxMessage>,
    published: DashMap<u64, u64>,
    claim_sequence: AtomicU64,
    capacity: RecordCapacity,
}

impl Default for MemoryOutbox {
    fn default() -> Self {
        Self {
            messages: DashMap::new(),
            published: DashMap::new(),
            claim_sequence: AtomicU64::new(0),
            capacity: RecordCapacity::fixed(DEFAULT_MEMORY_RECORD_CAPACITY),
        }
    }
}

impl MemoryOutbox {
    /// Creates an outbox with a fixed maximum number of retained records.
    pub fn new(capacity: usize) -> CatgaResult<Self> {
        Ok(Self {
            messages: DashMap::with_capacity(capacity),
            published: DashMap::with_capacity(capacity),
            claim_sequence: AtomicU64::new(0),
            capacity: RecordCapacity::new(capacity)?,
        })
    }

    fn next_claim_token(&self) -> Box<str> {
        format!(
            "memory-{}",
            self.claim_sequence.fetch_add(1, Ordering::Relaxed)
        )
        .into()
    }
}

#[async_trait]
impl OutboxStore for MemoryOutbox {
    async fn enqueue(&self, message: OutboxMessage) -> CatgaResult<()> {
        let mut operation = telemetry::persistence_operation("memory", "outbox", "enqueue");
        let id = message.id();
        let result = validate_outbox_message_id(id).and_then(|()| match self.messages.entry(id) {
            Entry::Vacant(entry) => {
                if !self.capacity.reserve() {
                    Err(CatgaError::new(
                        ErrorCode::Unavailable,
                        "memory outbox record capacity is exhausted",
                    ))
                } else {
                    entry.insert(message);
                    Ok(())
                }
            }
            Entry::Occupied(_) => Err(CatgaError::new(
                ErrorCode::Conflict,
                "an outbox message with this identifier already exists",
            )),
        });
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
                let message = entry.value();
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
                if entry.is_claimable_at(now_unix_ms) && entry.is_due_at(now) {
                    entry.claim_until_with_token(owner, self.next_claim_token(), expires_at);
                    claimed.push(entry.clone());
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
                && message.owner() == Some(owner)
                && message.claim_token() == Some(claim_token)
            {
                message.mark_published(published_at);
                self.published.insert(id, published_at);
            }
            Ok(())
        })();
        operation.complete(&result);
        result
    }

    async fn release(&self, owner: &str, id: u64, claim_token: &str) -> CatgaResult<()> {
        let mut operation = telemetry::persistence_operation("memory", "outbox", "release");
        if let Some(mut message) = self.messages.get_mut(&id)
            && message.owner() == Some(owner)
            && message.claim_token() == Some(claim_token)
        {
            message.release();
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
            && message.owner() == Some(owner)
            && message.claim_token() == Some(claim_token)
        {
            message.record_failure(reason);
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
            if entry.get().state() != OutboxState::Pending {
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
                        .map(|message| message.clone())
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
            let candidates: Vec<u64> = self
                .published
                .iter()
                .take(limit)
                .filter(|entry| now.saturating_sub(*entry.value()) >= retention)
                .map(|entry| *entry.key())
                .collect();
            let mut removed = 0;
            for id in candidates {
                if self
                    .messages
                    .get(&id)
                    .is_some_and(|message| message.state() == OutboxState::Published)
                    && self.messages.remove(&id).is_some()
                {
                    self.capacity.release();
                    removed += 1;
                }
                self.published.remove(&id);
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
