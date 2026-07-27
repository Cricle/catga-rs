use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, DEFAULT_IDEMPOTENCY_RETENTION, ErrorCode, IdempotencyStore,
    ProcessingState, telemetry, validate_completed_retention, validate_retention_cleanup_limit,
};
use dashmap::{DashMap, mapref::entry::Entry};

use crate::{
    DEFAULT_MEMORY_RECORD_CAPACITY,
    capacity::{OPPORTUNISTIC_CLEANUP_LIMIT, RecordCapacity},
    claim::ClaimRecord,
};

/// A process-local idempotency store using atomic per-key claim transitions.
pub struct MemoryIdempotency {
    records: DashMap<Box<str>, ClaimRecord>,
    completed: DashMap<Box<str>, u64>,
    retention: Duration,
    capacity: RecordCapacity,
}

impl Default for MemoryIdempotency {
    fn default() -> Self {
        Self {
            records: DashMap::new(),
            completed: DashMap::new(),
            retention: DEFAULT_IDEMPOTENCY_RETENTION,
            capacity: RecordCapacity::fixed(DEFAULT_MEMORY_RECORD_CAPACITY),
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
        })
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
                Entry::Vacant(entry) => {
                    entry.insert(ClaimRecord::claimed());
                    Ok(true)
                }
            }
        };
        operation.complete_claim(&result);
        result
    }

    async fn complete(&self, key: &str, result: Option<Arc<[u8]>>) -> CatgaResult<()> {
        let mut operation = telemetry::persistence_operation("memory", "idempotency", "complete");
        let outcome = self
            .records
            .get(key)
            .ok_or_else(|| CatgaError::new(ErrorCode::NotFound, "idempotency key is not claimed"))
            .and_then(|record| {
                if record.complete(result) {
                    Ok(())
                } else {
                    Err(CatgaError::new(
                        ErrorCode::Conflict,
                        "idempotency key is not currently claimed",
                    ))
                }
            });
        if outcome.is_ok() {
            self.completed.insert(key.into(), now_millis());
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
            let candidates: Vec<Box<str>> = self
                .completed
                .iter()
                .take(limit)
                .filter(|entry| now.saturating_sub(*entry.value()) >= retention)
                .map(|entry| entry.key().clone())
                .collect();
            let mut removed = 0;
            for key in candidates {
                if self
                    .records
                    .get(&key)
                    .is_some_and(|record| record.state() == ProcessingState::Completed)
                    && self.records.remove(&key).is_some()
                {
                    self.capacity.release();
                    removed += 1;
                }
                self.completed.remove(&key);
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
