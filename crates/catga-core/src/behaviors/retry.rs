use std::time::Duration;

use async_trait::async_trait;

use crate::{
    Behavior, CatgaError, CatgaResult, ErrorCode, Next, Request, RetryJitter,
    retry_jitter::RetryJitterState,
    telemetry::{RESILIENCE_RETRIES, retry_pending},
};

/// Retries transient request failures with bounded exponential backoff.
pub struct RetryBehavior {
    max_retries: usize,
    initial_delay: Duration,
    jitter: RetryJitterState,
}

impl RetryBehavior {
    /// Creates a retry behavior with at most `max_retries` additional attempts.
    pub const fn new(max_retries: usize, initial_delay: Duration) -> Self {
        Self::with_jitter(max_retries, initial_delay, RetryJitter::None)
    }

    /// Creates a retry behavior with an explicit bounded jitter policy.
    pub const fn with_jitter(
        max_retries: usize,
        initial_delay: Duration,
        jitter: RetryJitter,
    ) -> Self {
        Self {
            max_retries,
            initial_delay,
            jitter: RetryJitterState::new(jitter),
        }
    }

    fn delay_for(&self, retry: usize) -> Duration {
        let multiplier = u32::try_from(retry)
            .ok()
            .and_then(|retry| 1_u32.checked_shl(retry))
            .unwrap_or(u32::MAX);
        self.jitter
            .delay(self.initial_delay.saturating_mul(multiplier))
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
                        let _pending = retry_pending();
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
