//! Lock-free record-capacity admission shared by in-memory durable stores.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::{CatgaError, CatgaResult, ErrorCode};

/// Default maximum number of records retained by one in-memory durable store.
pub const DEFAULT_MEMORY_RECORD_CAPACITY: usize = 65_536;

/// Maximum expired records inspected when capacity pressure triggers cleanup.
pub(crate) const OPPORTUNISTIC_CLEANUP_LIMIT: usize = 64;

pub(crate) struct RecordSequence {
    next: AtomicU64,
}

impl RecordSequence {
    pub(crate) const fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
        }
    }

    pub(crate) fn next(&self) -> CatgaResult<u64> {
        self.next
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |identity| {
                identity.checked_add(1)
            })
            .map_err(|_| {
                CatgaError::new(
                    ErrorCode::Internal,
                    "memory record identity space is exhausted",
                )
            })
    }
}

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
