use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(feature = "test-hooks")]
use std::sync::{Barrier, Mutex};

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, DEFAULT_IDEMPOTENCY_RETENTION, DEFAULT_INBOX_CLAIM_LEASE, ErrorCode,
    InboxClaim, InboxStore, ProcessingState, inbox_claim_expires_at, telemetry,
    validate_completed_retention,
};
use dashmap::{DashMap, mapref::entry::Entry};

use crate::{
    DEFAULT_MEMORY_RECORD_CAPACITY,
    capacity::{OPPORTUNISTIC_CLEANUP_LIMIT, RecordCapacity, RecordSequence},
    claim::ClaimRecord,
};

#[derive(Clone, Copy, Eq, PartialEq)]
struct CompletedRecord {
    completed_at: u64,
    record_identity: u64,
}

/// A process-local inbox using atomic per-message claim transitions.
pub struct MemoryInbox {
    records: DashMap<u64, ClaimRecord>,
    completed: DashMap<u64, CompletedRecord>,
    capacity: RecordCapacity,
    record_sequence: RecordSequence,
    retention: Duration,
    #[cfg(feature = "test-hooks")]
    cleanup_after_snapshot: Mutex<Option<(Arc<Barrier>, Arc<Barrier>)>>,
}

impl Default for MemoryInbox {
    fn default() -> Self {
        Self {
            records: DashMap::new(),
            completed: DashMap::new(),
            capacity: RecordCapacity::fixed(DEFAULT_MEMORY_RECORD_CAPACITY),
            record_sequence: RecordSequence::new(),
            retention: DEFAULT_IDEMPOTENCY_RETENTION,
            #[cfg(feature = "test-hooks")]
            cleanup_after_snapshot: Mutex::new(None),
        }
    }
}

impl MemoryInbox {
    /// Creates an inbox with a fixed maximum number of retained records.
    pub fn new(capacity: usize) -> CatgaResult<Self> {
        Self::with_retention_and_capacity(DEFAULT_IDEMPOTENCY_RETENTION, capacity)
    }

    /// Creates an inbox retaining completed records for `retention` within `capacity` records.
    pub fn with_retention_and_capacity(retention: Duration, capacity: usize) -> CatgaResult<Self> {
        validate_completed_retention(retention)?;
        Ok(Self {
            records: DashMap::with_capacity(capacity),
            completed: DashMap::with_capacity(capacity),
            capacity: RecordCapacity::new(capacity)?,
            record_sequence: RecordSequence::new(),
            retention,
            #[cfg(feature = "test-hooks")]
            cleanup_after_snapshot: Mutex::new(None),
        })
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
                "memory inbox cleanup test hook is poisoned",
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
                    "memory inbox cleanup test hook is poisoned",
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
impl InboxStore for MemoryInbox {
    async fn try_claim(&self, message_id: u64) -> CatgaResult<Option<InboxClaim>> {
        self.try_claim_for(message_id, DEFAULT_INBOX_CLAIM_LEASE)
            .await
    }

    async fn try_claim_for(
        &self,
        message_id: u64,
        lease: Duration,
    ) -> CatgaResult<Option<InboxClaim>> {
        let mut operation = telemetry::persistence_operation("memory", "inbox", "try_claim");
        let expires_at = inbox_claim_expires_at(lease)?;
        let now = now_millis();
        let result = if let Some(record) = self.records.get(&message_id) {
            Ok(record
                .try_claim_generation_until(expires_at, now)
                .and_then(|generation| InboxClaim::new(message_id, generation)))
        } else {
            if !self.capacity.reserve() {
                if let Err(error) = self
                    .cleanup_completed(self.retention, OPPORTUNISTIC_CLEANUP_LIMIT)
                    .await
                {
                    let result = Err(error);
                    operation.complete_optional_claim(&result);
                    return result;
                }
                if !self.capacity.reserve() {
                    let result = Err(CatgaError::new(
                        ErrorCode::Unavailable,
                        "memory inbox record capacity is exhausted",
                    ));
                    operation.complete_optional_claim(&result);
                    return result;
                }
            }
            match self.records.entry(message_id) {
                Entry::Occupied(record) => {
                    self.capacity.release();
                    Ok(record
                        .get()
                        .try_claim_generation_until(expires_at, now)
                        .and_then(|generation| InboxClaim::new(message_id, generation)))
                }
                Entry::Vacant(entry) => match self.record_sequence.next() {
                    Ok(identity) => {
                        let record = ClaimRecord::claimed(identity);
                        let claimed = record
                            .try_claim_generation_until(expires_at, now)
                            .and_then(|generation| InboxClaim::new(message_id, generation));
                        entry.insert(record);
                        Ok(claimed)
                    }
                    Err(error) => {
                        self.capacity.release();
                        Err(error)
                    }
                },
            }
        };
        operation.complete_optional_claim(&result);
        result
    }

    async fn complete(&self, claim: InboxClaim, result: Option<Arc<[u8]>>) -> CatgaResult<()> {
        let message_id = claim.message_id();
        let mut operation = telemetry::persistence_operation("memory", "inbox", "complete");
        let mut record_identity = None;
        let outcome = self
            .records
            .get(&message_id)
            .ok_or_else(|| CatgaError::new(ErrorCode::NotFound, "inbox message is not claimed"))
            .and_then(|record| {
                if record.complete_for(claim.generation(), result) {
                    record_identity = Some(record.identity());
                    Ok(())
                } else {
                    Err(CatgaError::new(
                        ErrorCode::Conflict,
                        "inbox message is not currently claimed",
                    ))
                }
            });
        if let Some(record_identity) = record_identity {
            self.completed.insert(
                message_id,
                CompletedRecord {
                    completed_at: now_millis(),
                    record_identity,
                },
            );
        }
        operation.complete(&outcome);
        outcome
    }

    async fn fail(&self, claim: InboxClaim) -> CatgaResult<()> {
        let message_id = claim.message_id();
        let mut operation = telemetry::persistence_operation("memory", "inbox", "fail");
        let outcome = self
            .records
            .get(&message_id)
            .ok_or_else(|| CatgaError::new(ErrorCode::NotFound, "inbox message is not claimed"))
            .and_then(|record| {
                if record.fail_for(claim.generation()) {
                    Ok(())
                } else {
                    Err(CatgaError::new(
                        ErrorCode::Conflict,
                        "inbox message is not currently claimed",
                    ))
                }
            });
        operation.complete(&outcome);
        outcome
    }

    async fn state(&self, message_id: u64) -> CatgaResult<Option<ProcessingState>> {
        let mut operation = telemetry::persistence_operation("memory", "inbox", "state");
        let result = Ok(self.records.get(&message_id).map(|record| record.state()));
        operation.complete(&result);
        result
    }

    async fn result(&self, message_id: u64) -> CatgaResult<Option<Arc<[u8]>>> {
        let mut operation = telemetry::persistence_operation("memory", "inbox", "result");
        let outcome = Ok(self
            .records
            .get(&message_id)
            .and_then(|record| record.result()));
        operation.complete(&outcome);
        outcome
    }

    async fn cleanup_completed(&self, retention: Duration, limit: usize) -> CatgaResult<usize> {
        let mut operation = telemetry::persistence_operation("memory", "inbox", "cleanup");
        let outcome = (|| {
            catga_core::validate_retention_cleanup_limit(limit)?;
            let retention = u64::try_from(retention.as_millis()).map_err(|_| {
                CatgaError::new(
                    ErrorCode::Validation,
                    "inbox retention exceeds the supported millisecond range",
                )
            })?;
            let now = now_millis();
            let candidates: Vec<(u64, CompletedRecord)> = self
                .completed
                .iter()
                .take(limit)
                .filter(|entry| now.saturating_sub(entry.value().completed_at) >= retention)
                .map(|entry| (*entry.key(), *entry.value()))
                .collect();
            #[cfg(feature = "test-hooks")]
            self.wait_after_cleanup_snapshot()?;
            let mut removed = 0;
            for (id, candidate) in candidates {
                if self
                    .records
                    .remove_if(&id, |_, record| {
                        record.identity() == candidate.record_identity
                            && record.state() == ProcessingState::Completed
                    })
                    .is_some()
                {
                    self.capacity.release();
                    removed += 1;
                }
                self.completed
                    .remove_if(&id, |_, current| *current == candidate);
            }
            Ok(removed)
        })();
        operation.complete(&outcome);
        outcome
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}
