use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use catga_core::{CatgaError, CatgaResult, ConsensusStateMachine, ErrorCode};

/// A state machine that sums little-endian u64 commands so tests can observe
/// exactly which entries were applied and verify snapshot/restore round-trips.
pub(crate) struct RecordingMachine {
    sum: Arc<AtomicU64>,
    applied: Arc<AtomicU64>,
    snapshot_calls: Arc<AtomicU64>,
}

impl RecordingMachine {
    pub(crate) fn new(
        sum: Arc<AtomicU64>,
        applied: Arc<AtomicU64>,
        snapshot_calls: Arc<AtomicU64>,
    ) -> Self {
        Self {
            sum,
            applied,
            snapshot_calls,
        }
    }
}

impl ConsensusStateMachine for RecordingMachine {
    fn apply(&mut self, index: u64, data: &[u8]) -> CatgaResult<()> {
        let bytes: [u8; 8] = data.try_into().map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "recording state-machine commands must contain eight bytes",
            )
        })?;
        self.sum
            .fetch_add(u64::from_le_bytes(bytes), Ordering::AcqRel);
        self.applied.fetch_max(index, Ordering::AcqRel);
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        self.snapshot_calls.fetch_add(1, Ordering::AcqRel);
        Ok(self.sum.load(Ordering::Acquire).to_le_bytes().to_vec())
    }

    fn restore(&mut self, data: &[u8]) -> CatgaResult<()> {
        let bytes: [u8; 8] = data.try_into().map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "recording state-machine snapshots must contain eight bytes",
            )
        })?;
        self.sum.store(u64::from_le_bytes(bytes), Ordering::Release);
        Ok(())
    }
}
