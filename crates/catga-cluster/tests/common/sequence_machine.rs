use std::sync::{Arc, Mutex};

use catga_cluster::{RaftCommittedEntry, RaftStateMachine};
use catga_core::{CatgaError, CatgaResult, ErrorCode};

/// A state machine that records applied little-endian u64 commands in order,
/// so tests can assert on the exact applied sequence — for example that every
/// entry applied before a leader kill is still a prefix after re-election.
pub(crate) struct SequenceMachine {
    values: Arc<Mutex<Vec<u64>>>,
}

impl SequenceMachine {
    pub(crate) fn new(values: Arc<Mutex<Vec<u64>>>) -> Self {
        Self { values }
    }
}

impl RaftStateMachine for SequenceMachine {
    fn apply(&mut self, entry: &RaftCommittedEntry) -> CatgaResult<()> {
        let bytes: [u8; 8] = entry.data.as_slice().try_into().map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "sequence state-machine commands must contain eight bytes",
            )
        })?;
        self.values
            .lock()
            .expect("applied sequence poisoned")
            .push(u64::from_le_bytes(bytes));
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        let values = self.values.lock().expect("applied sequence poisoned");
        let mut bytes = Vec::with_capacity(values.len() * 8);
        for value in values.iter() {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        Ok(bytes)
    }

    fn restore(&mut self, bytes: &[u8]) -> CatgaResult<()> {
        if !bytes.len().is_multiple_of(8) {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "sequence state-machine snapshots must be eight-byte aligned",
            ));
        }
        let restored: Vec<u64> = bytes
            .chunks_exact(8)
            .map(|chunk| u64::from_le_bytes(chunk.try_into().expect("chunks_exact yields 8 bytes")))
            .collect();
        *self.values.lock().expect("applied sequence poisoned") = restored;
        Ok(())
    }
}
