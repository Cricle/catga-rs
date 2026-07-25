use std::time::Duration;

use async_trait::async_trait;

use crate::{
    Behavior, CatgaError, CatgaResult, ErrorCode, Next, Request, telemetry::RESILIENCE_RETRIES,
};

/// Retries transient request failures with bounded exponential backoff.
pub struct RetryBehavior {
    max_retries: usize,
    initial_delay: Duration,
}

impl RetryBehavior {
    /// Creates a retry behavior with at most `max_retries` additional attempts.
    pub const fn new(max_retries: usize, initial_delay: Duration) -> Self {
        Self {
            max_retries,
            initial_delay,
        }
    }

    fn delay_for(&self, retry: usize) -> Duration {
        let multiplier = u32::try_from(retry)
            .ok()
            .and_then(|retry| 1_u32.checked_shl(retry))
            .unwrap_or(u32::MAX);
        self.initial_delay.saturating_mul(multiplier)
    }
}

#[async_trait]
impl<M> Behavior<M> for RetryBehavior
where
    M: Request + Clone,
{
    async fn handle(&self, message: M, next: Next<M>) -> CatgaResult<M::Response> {
        for retry in 0..=self.max_retries {
            match next.run(message.clone()).await {
                Err(error) if error.code() == ErrorCode::Transient && retry < self.max_retries => {
                    let delay = self.delay_for(retry);
                    metrics::counter!(RESILIENCE_RETRIES).increment(1);
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                }
                result => return result,
            }
        }
        Err(CatgaError::new(
            ErrorCode::Internal,
            "retry loop completed without a handler result",
        ))
    }
}
