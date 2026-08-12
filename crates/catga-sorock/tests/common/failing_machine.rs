//! A state machine that fails selected operations, used to verify error
//! propagation through `SorockApp`.

use catga_core::{CatgaError, CatgaResult, ConsensusStateMachine, ErrorCode};

/// Each flag makes the matching [`ConsensusStateMachine`] operation return a
/// distinctive error so tests can assert exactly which failure propagated.
pub(crate) struct FailingMachine {
    pub(crate) fail_apply: bool,
    pub(crate) fail_snapshot: bool,
    pub(crate) fail_restore: bool,
}

impl ConsensusStateMachine for FailingMachine {
    fn apply(&mut self, _index: u64, _data: &[u8]) -> CatgaResult<()> {
        if self.fail_apply {
            return Err(CatgaError::new(
                ErrorCode::Internal,
                "injected apply failure",
            ));
        }
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        if self.fail_snapshot {
            return Err(CatgaError::new(
                ErrorCode::PersistenceFailed,
                "injected snapshot failure",
            ));
        }
        Ok(Vec::new())
    }

    fn restore(&mut self, _data: &[u8]) -> CatgaResult<()> {
        if self.fail_restore {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "injected restore failure",
            ));
        }
        Ok(())
    }
}
