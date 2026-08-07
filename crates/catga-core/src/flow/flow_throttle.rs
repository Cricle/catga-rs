//! Shared concurrency budget for throttled flow actions.

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{CatgaError, CatgaResult, ErrorCode};

/// Shared concurrency budget for throttled flow actions.
#[derive(Clone)]
pub struct FlowThrottle {
    permits: Arc<Semaphore>,
}

impl FlowThrottle {
    /// Creates a throttle that permits at most `limit` actions at once.
    pub fn new(limit: usize) -> CatgaResult<Self> {
        if limit == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "flow throttle limit must be greater than zero",
            ));
        }
        Ok(Self {
            permits: Arc::new(Semaphore::new(limit)),
        })
    }

    /// Acquires a permit from the throttle.
    pub async fn acquire(&self) -> CatgaResult<OwnedSemaphorePermit> {
        Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| CatgaError::new(ErrorCode::Cancelled, "flow throttle is closed"))
    }
}
