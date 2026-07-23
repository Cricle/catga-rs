use std::sync::Arc;

use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ErrorCode, InboxStore, ProcessingState};
use dashmap::{DashMap, mapref::entry::Entry};

use crate::claim::ClaimRecord;

/// A process-local inbox using atomic per-message claim transitions.
#[derive(Default)]
pub struct MemoryInbox {
    records: DashMap<u64, ClaimRecord>,
}

#[async_trait]
impl InboxStore for MemoryInbox {
    async fn try_claim(&self, message_id: u64) -> CatgaResult<bool> {
        match self.records.entry(message_id) {
            Entry::Occupied(record) => Ok(record.get().try_claim()),
            Entry::Vacant(entry) => {
                entry.insert(ClaimRecord::claimed());
                Ok(true)
            }
        }
    }

    async fn complete(&self, message_id: u64, result: Option<Arc<[u8]>>) -> CatgaResult<()> {
        let record = self
            .records
            .get(&message_id)
            .ok_or_else(|| CatgaError::new(ErrorCode::NotFound, "inbox message is not claimed"))?;
        if record.complete(result) {
            Ok(())
        } else {
            Err(CatgaError::new(
                ErrorCode::Conflict,
                "inbox message is not currently claimed",
            ))
        }
    }

    async fn fail(&self, message_id: u64) -> CatgaResult<()> {
        let record = self
            .records
            .get(&message_id)
            .ok_or_else(|| CatgaError::new(ErrorCode::NotFound, "inbox message is not claimed"))?;
        if record.fail() {
            Ok(())
        } else {
            Err(CatgaError::new(
                ErrorCode::Conflict,
                "inbox message is not currently claimed",
            ))
        }
    }

    async fn state(&self, message_id: u64) -> CatgaResult<Option<ProcessingState>> {
        Ok(self.records.get(&message_id).map(|record| record.state()))
    }

    async fn result(&self, message_id: u64) -> CatgaResult<Option<Arc<[u8]>>> {
        Ok(self
            .records
            .get(&message_id)
            .and_then(|record| record.result()))
    }
}
