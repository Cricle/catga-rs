use std::sync::Arc;

use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ErrorCode, IdempotencyStore, ProcessingState};
use dashmap::{DashMap, mapref::entry::Entry};

use crate::claim::ClaimRecord;

/// A process-local idempotency store using atomic per-key claim transitions.
#[derive(Default)]
pub struct MemoryIdempotency {
    records: DashMap<Box<str>, ClaimRecord>,
}

#[async_trait]
impl IdempotencyStore for MemoryIdempotency {
    async fn try_claim(&self, key: &str) -> CatgaResult<bool> {
        match self.records.entry(key.into()) {
            Entry::Occupied(record) => Ok(record.get().try_claim()),
            Entry::Vacant(entry) => {
                entry.insert(ClaimRecord::claimed());
                Ok(true)
            }
        }
    }

    async fn complete(&self, key: &str, result: Option<Arc<[u8]>>) -> CatgaResult<()> {
        let record = self.records.get(key).ok_or_else(|| {
            CatgaError::new(ErrorCode::NotFound, "idempotency key is not claimed")
        })?;
        if record.complete(result) {
            Ok(())
        } else {
            Err(CatgaError::new(
                ErrorCode::Conflict,
                "idempotency key is not currently claimed",
            ))
        }
    }

    async fn fail(&self, key: &str) -> CatgaResult<()> {
        let record = self.records.get(key).ok_or_else(|| {
            CatgaError::new(ErrorCode::NotFound, "idempotency key is not claimed")
        })?;
        if record.fail() {
            Ok(())
        } else {
            Err(CatgaError::new(
                ErrorCode::Conflict,
                "idempotency key is not currently claimed",
            ))
        }
    }

    async fn state(&self, key: &str) -> CatgaResult<Option<ProcessingState>> {
        Ok(self.records.get(key).map(|record| record.state()))
    }

    async fn result(&self, key: &str) -> CatgaResult<Option<Arc<[u8]>>> {
        Ok(self.records.get(key).and_then(|record| record.result()))
    }
}
