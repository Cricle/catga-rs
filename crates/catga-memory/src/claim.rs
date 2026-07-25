use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicU8, AtomicU64, Ordering},
};

use catga_core::ProcessingState;

const PENDING: u8 = 0;
const CLAIMED: u8 = 1;
const COMPLETED: u8 = 2;
const FAILED: u8 = 3;
const COMPLETING: u8 = 4;
const CLAIMING: u8 = 5;

pub(crate) struct ClaimRecord {
    state: AtomicU8,
    expires_at_millis: AtomicU64,
    result: OnceLock<Option<Arc<[u8]>>>,
}

impl ClaimRecord {
    pub(crate) fn claimed() -> Self {
        Self {
            state: AtomicU8::new(CLAIMED),
            expires_at_millis: AtomicU64::new(0),
            result: OnceLock::new(),
        }
    }

    pub(crate) fn try_claim(&self) -> bool {
        self.try_claim_inner(None)
    }

    pub(crate) fn try_claim_until(&self, expires_at_millis: u64, now_millis: u64) -> bool {
        self.try_claim_inner(Some((expires_at_millis, now_millis)))
    }

    fn try_claim_inner(&self, lease: Option<(u64, u64)>) -> bool {
        loop {
            let state = self.state.load(Ordering::Acquire);
            let reclaimable = matches!(state, PENDING | FAILED)
                || (state == CLAIMED
                    && lease.is_some_and(|(_, now)| {
                        self.expires_at_millis.load(Ordering::Acquire) <= now
                    }));
            if !reclaimable {
                return false;
            }
            if self
                .state
                .compare_exchange(state, CLAIMING, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.expires_at_millis.store(
                    lease.map_or(0, |(expires_at, _)| expires_at),
                    Ordering::Release,
                );
                self.state.store(CLAIMED, Ordering::Release);
                return true;
            }
        }
    }

    pub(crate) fn complete(&self, result: Option<Arc<[u8]>>) -> bool {
        if self
            .state
            .compare_exchange(CLAIMED, COMPLETING, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        if self.result.set(result).is_err() {
            self.state.store(FAILED, Ordering::Release);
            return false;
        }
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
            COMPLETING | CLAIMING => ProcessingState::Claimed,
            _ => ProcessingState::Failed,
        }
    }
}
