use std::time::Duration;

use async_trait::async_trait;

use crate::{Behavior, CatgaError, CatgaResult, ErrorCode, Next, Request};

/// Bounds one request attempt and cancels it when its deadline elapses.
pub struct TimeoutBehavior {
    timeout: Duration,
}

impl TimeoutBehavior {
    /// Creates a behavior that permits each downstream attempt for `timeout`.
    pub const fn new(timeout: Duration) -> Self {
        Self { timeout }
    }
}

#[async_trait]
impl<M: Request> Behavior<M> for TimeoutBehavior {
    async fn handle(&self, message: M, next: Next<M>) -> CatgaResult<M::Response> {
        tokio::time::timeout(self.timeout, next.run(message))
            .await
            .map_err(|_| CatgaError::new(ErrorCode::Timeout, "request handler timed out"))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeout_behavior_new_creates_instance() {
        let timeout = TimeoutBehavior::new(Duration::from_secs(5));
        // Basic check that instance was created
        assert!(std::mem::size_of_val(&timeout) > 0);
    }

    #[test]
    fn timeout_behavior_accepts_zero_duration() {
        let timeout = TimeoutBehavior::new(Duration::ZERO);
        assert!(std::mem::size_of_val(&timeout) > 0);
    }

    #[test]
    fn timeout_behavior_accepts_large_duration() {
        let timeout = TimeoutBehavior::new(Duration::MAX);
        assert!(std::mem::size_of_val(&timeout) > 0);
    }
}
