use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicU8, Ordering},
};

use catga_core::ProcessingState;

const PENDING: u8 = 0;
const CLAIMED: u8 = 1;
const COMPLETED: u8 = 2;
const FAILED: u8 = 3;
const COMPLETING: u8 = 4;

pub(crate) struct ClaimRecord {
    state: AtomicU8,
    result: OnceLock<Option<Arc<[u8]>>>,
}

impl ClaimRecord {
    pub(crate) fn claimed() -> Self {
        Self {
            state: AtomicU8::new(CLAIMED),
            result: OnceLock::new(),
        }
    }

    pub(crate) fn try_claim(&self) -> bool {
        for state in [PENDING, FAILED] {
            if self
                .state
                .compare_exchange(state, CLAIMED, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return true;
            }
        }
        false
    }

    pub(crate) fn complete(&self, result: Option<Arc<[u8]>>) -> bool {
        if self
            .state
            .compare_exchange(CLAIMED, COMPLETING, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        self.result
            .set(result)
            .expect("a record can only complete once");
        self.state.store(COMPLETED, Ordering::Release);
        true
    }

    pub(crate) fn fail(&self) -> bool {
        self.state
            .compare_exchange(CLAIMED, FAILED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub(crate) fn result(&self) -> Option<Arc<[u8]>> {
        (self.state.load(Ordering::Acquire) == COMPLETED)
            .then(|| self.result.get().cloned())
            .flatten()
            .flatten()
    }

    pub(crate) fn state(&self) -> ProcessingState {
        match self.state.load(Ordering::Acquire) {
            PENDING => ProcessingState::Pending,
            CLAIMED => ProcessingState::Claimed,
            COMPLETED => ProcessingState::Completed,
            FAILED => ProcessingState::Failed,
            COMPLETING => ProcessingState::Claimed,
            _ => unreachable!("claim state is always valid"),
        }
    }
}
