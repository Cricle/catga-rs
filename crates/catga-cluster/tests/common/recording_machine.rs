use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

use catga_cluster::{RaftCommittedEntry, RaftStateMachine};
use catga_core::{CatgaError, CatgaResult, ErrorCode};

/// A state machine that sums little-endian u64 commands so tests can observe
/// exactly which entries were applied and how many snapshots were taken.
pub(crate) struct RecordingMachine {
    pub(crate) applied: Arc<AtomicU64>,
    pub(crate) snapshot_calls: Arc<AtomicUsize>,
}

impl RecordingMachine {
    pub(crate) fn new(applied: Arc<AtomicU64>, snapshot_calls: Arc<AtomicUsize>) -> Self {
        Self {
            applied,
            snapshot_calls,
        }
    }
}

impl RaftStateMachine for RecordingMachine {
    fn apply(&mut self, entry: &RaftCommittedEntry) -> CatgaResult<()> {
        let bytes: [u8; 8] = entry.data.as_slice().try_into().map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "recording state-machine commands must contain eight bytes",
            )
        })?;
        self.applied
            .fetch_add(u64::from_le_bytes(bytes), Ordering::AcqRel);
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        self.snapshot_calls.fetch_add(1, Ordering::AcqRel);
        Ok(self.applied.load(Ordering::Acquire).to_le_bytes().to_vec())
    }

    fn restore(&mut self, bytes: &[u8]) -> CatgaResult<()> {
        let bytes: [u8; 8] = bytes.try_into().map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "recording state-machine snapshots must contain eight bytes",
            )
        })?;
        self.applied
            .store(u64::from_le_bytes(bytes), Ordering::Release);
        Ok(())
    }
}
