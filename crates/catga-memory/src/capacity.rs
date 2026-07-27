//! Lock-free record-capacity admission shared by in-memory durable stores.

use std::sync::atomic::{AtomicUsize, Ordering};

use catga_core::{CatgaError, CatgaResult, ErrorCode};

/// Default maximum number of records retained by one in-memory durable store.
pub const DEFAULT_MEMORY_RECORD_CAPACITY: usize = 65_536;

/// Maximum expired records inspected when capacity pressure triggers cleanup.
pub(crate) const OPPORTUNISTIC_CLEANUP_LIMIT: usize = 64;

pub(crate) struct RecordCapacity {
    limit: usize,
    used: AtomicUsize,
}

impl RecordCapacity {
    pub(crate) const fn fixed(limit: usize) -> Self {
        Self {
            limit,
            used: AtomicUsize::new(0),
        }
    }

    pub(crate) fn new(limit: usize) -> CatgaResult<Self> {
        if limit == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "memory store record capacity must be greater than zero",
            ));
        }
        Ok(Self::fixed(limit))
    }

    pub(crate) fn reserve(&self) -> bool {
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            if used >= self.limit {
                return false;
            }
            match self.used.compare_exchange_weak(
                used,
                used + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(observed) => used = observed,
            }
        }
    }

    pub(crate) fn release(&self) {
        let mut used = self.used.load(Ordering::Acquire);
        loop {
            if used == 0 {
                return;
            }
            match self.used.compare_exchange_weak(
                used,
                used - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => used = observed,
            }
        }
    }
}
