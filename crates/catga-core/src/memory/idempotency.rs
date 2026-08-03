use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(feature = "test-hooks")]
use std::sync::{Barrier, Mutex};

use crate::{
    CatgaError, CatgaResult, DEFAULT_IDEMPOTENCY_RETENTION, ErrorCode, IdempotencyStore,
    ProcessingState, telemetry, validate_completed_retention, validate_retention_cleanup_limit,
};
use async_trait::async_trait;
use dashmap::{DashMap, mapref::entry::Entry};

use crate::memory::{
    DEFAULT_MEMORY_RECORD_CAPACITY,
    capacity::{OPPORTUNISTIC_CLEANUP_LIMIT, RecordCapacity, RecordSequence},
    claim::ClaimRecord,
};

#[derive(Clone, Copy, Eq, PartialEq)]
struct CompletedRecord {
    completed_at: u64,
    record_identity: u64,
}

/// A process-local idempotency store using atomic per-key claim transitions.
pub struct MemoryIdempotency {
    records: DashMap<Box<str>, ClaimRecord>,
    completed: DashMap<Box<str>, CompletedRecord>,
    retention: Duration,
    capacity: RecordCapacity,
    record_sequence: RecordSequence,
    #[cfg(feature = "test-hooks")]
    cleanup_after_snapshot: Mutex<Option<(Arc<Barrier>, Arc<Barrier>)>>,
}

impl Default for MemoryIdempotency {
    fn default() -> Self {
        Self {
            records: DashMap::new(),
            completed: DashMap::new(),
            retention: DEFAULT_IDEMPOTENCY_RETENTION,
            capacity: RecordCapacity::fixed(DEFAULT_MEMORY_RECORD_CAPACITY),
            record_sequence: RecordSequence::new(),
            #[cfg(feature = "test-hooks")]
            cleanup_after_snapshot: Mutex::new(None),
        }
    }
}

impl MemoryIdempotency {
    /// Creates a store with default completed-record retention and a fixed record capacity.
    pub fn new(capacity: usize) -> CatgaResult<Self> {
        Self::with_retention_and_capacity(DEFAULT_IDEMPOTENCY_RETENTION, capacity)
    }

    /// Creates a store retaining completed idempotency records for `retention`.
    pub fn with_retention(retention: Duration) -> CatgaResult<Self> {
        Self::with_retention_and_capacity(retention, DEFAULT_MEMORY_RECORD_CAPACITY)
    }

    /// Creates a store retaining completed records for `retention` within `capacity` records.
    pub fn with_retention_and_capacity(retention: Duration, capacity: usize) -> CatgaResult<Self> {
        validate_completed_retention(retention)?;
        Ok(Self {
            records: DashMap::new(),
            completed: DashMap::new(),
            retention,
            capacity: RecordCapacity::new(capacity)?,
            record_sequence: RecordSequence::new(),
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
                "memory idempotency cleanup test hook is poisoned",
            )
        })?;
        *hook = Some((observed, resume));
        Ok(())
    }

    #[doc(hidden)]
    #[cfg(feature = "test-hooks")]
    pub fn expire_completed_index_for_test(&self, key: &str) -> CatgaResult<()> {
        let mut completed = self.completed.get_mut(key).ok_or_else(|| {
            CatgaError::new(
                ErrorCode::NotFound,
                "memory idempotency completion index is not retained",
            )
        })?;
        completed.completed_at = 0;
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
                    "memory idempotency cleanup test hook is poisoned",
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
impl IdempotencyStore for MemoryIdempotency {
    async fn try_claim(&self, key: &str) -> CatgaResult<bool> {
        let mut operation = telemetry::persistence_operation("memory", "idempotency", "try_claim");
        let result = if let Some(record) = self.records.get(key) {
            Ok(record.try_claim())
        } else {
            if !self.capacity.reserve() {
                if let Err(error) = self.cleanup_completed(OPPORTUNISTIC_CLEANUP_LIMIT).await {
                    let result = Err(error);
                    operation.complete_claim(&result);
                    return result;
                }
                if !self.capacity.reserve() {
                    let result = Err(CatgaError::new(
                        ErrorCode::Unavailable,
                        "memory idempotency record capacity is exhausted",
                    ));
                    operation.complete_claim(&result);
                    return result;
                }
            }
            match self.records.entry(key.into()) {
                Entry::Occupied(record) => {
                    self.capacity.release();
                    Ok(record.get().try_claim())
                }
                Entry::Vacant(entry) => match self.record_sequence.next() {
                    Ok(identity) => {
                        entry.insert(ClaimRecord::claimed(identity));
                        Ok(true)
                    }
                    Err(error) => {
                        self.capacity.release();
                        Err(error)
                    }
                },
            }
        };
        operation.complete_claim(&result);
        result
    }

    async fn complete(&self, key: &str, result: Option<Arc<[u8]>>) -> CatgaResult<()> {
        let mut operation = telemetry::persistence_operation("memory", "idempotency", "complete");
        let mut record_identity = None;
        let outcome = self
            .records
            .get(key)
            .ok_or_else(|| CatgaError::new(ErrorCode::NotFound, "idempotency key is not claimed"))
            .and_then(|record| {
                if record.complete(result) {
                    record_identity = Some(record.identity());
                    Ok(())
                } else {
                    Err(CatgaError::new(
                        ErrorCode::Conflict,
                        "idempotency key is not currently claimed",
                    ))
                }
            });
        if let Some(record_identity) = record_identity {
            self.completed.insert(
                key.into(),
                CompletedRecord {
                    completed_at: now_millis(),
                    record_identity,
                },
            );
        }
        operation.complete(&outcome);
        outcome
    }

    async fn fail(&self, key: &str) -> CatgaResult<()> {
        let mut operation = telemetry::persistence_operation("memory", "idempotency", "fail");
        let outcome = self
            .records
            .get(key)
            .ok_or_else(|| CatgaError::new(ErrorCode::NotFound, "idempotency key is not claimed"))
            .and_then(|record| {
                if record.fail() {
                    Ok(())
                } else {
                    Err(CatgaError::new(
                        ErrorCode::Conflict,
                        "idempotency key is not currently claimed",
                    ))
                }
            });
        operation.complete(&outcome);
        outcome
    }

    async fn state(&self, key: &str) -> CatgaResult<Option<ProcessingState>> {
        let mut operation = telemetry::persistence_operation("memory", "idempotency", "state");
        let result = Ok(self.records.get(key).map(|record| record.state()));
        operation.complete(&result);
        result
    }

    async fn result(&self, key: &str) -> CatgaResult<Option<Arc<[u8]>>> {
        let mut operation = telemetry::persistence_operation("memory", "idempotency", "result");
        let outcome = Ok(self.records.get(key).and_then(|record| record.result()));
        operation.complete(&outcome);
        outcome
    }

    async fn cleanup_completed(&self, limit: usize) -> CatgaResult<usize> {
        let mut operation = telemetry::persistence_operation("memory", "idempotency", "cleanup");
        let outcome = (|| {
            validate_retention_cleanup_limit(limit)?;
            let now = now_millis();
            let retention = u64::try_from(self.retention.as_millis()).map_err(|_| {
                CatgaError::new(
                    ErrorCode::Validation,
                    "idempotency retention exceeds the supported millisecond range",
                )
            })?;
            let candidates: Vec<(Box<str>, CompletedRecord)> = self
                .completed
                .iter()
                .take(limit)
                .filter(|entry| now.saturating_sub(entry.value().completed_at) >= retention)
                .map(|entry| (entry.key().clone(), *entry.value()))
                .collect();
            #[cfg(feature = "test-hooks")]
            self.wait_after_cleanup_snapshot()?;
            let mut removed = 0;
            for (key, candidate) in candidates {
                if self
                    .records
                    .remove_if(&key, |_, record| {
                        record.identity() == candidate.record_identity
                            && record.state() == ProcessingState::Completed
                    })
                    .is_some()
                {
                    self.capacity.release();
                    removed += 1;
                }
                self.completed
                    .remove_if(&key, |_, current| *current == candidate);
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
