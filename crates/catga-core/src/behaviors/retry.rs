use std::time::Duration;

use async_trait::async_trait;

use crate::{
    Behavior, CatgaError, CatgaResult, ErrorCode, Next, Request, RetryJitter,
    retry_jitter::RetryJitterState,
    telemetry::{RESILIENCE_RETRIES, retry_pending},
};

/// Retries retryable request failures with bounded exponential backoff.
pub struct RetryBehavior {
    max_retries: usize,
    initial_delay: Duration,
    jitter: RetryJitterState,
}

impl RetryBehavior {
    /// Creates a retry behavior with at most `max_retries` additional attempts.
    pub const fn new(max_retries: usize, initial_delay: Duration) -> Self {
        Self::with_jitter(
            max_retries,
            initial_delay,
            RetryJitter::production_default(),
        )
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

    /// Returns this behavior's configured retry-jitter policy without sampling it.
    pub const fn jitter_policy(&self) -> RetryJitter {
        self.jitter.policy()
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
                Err(error)
                    if error.code() != ErrorCode::Cancelled
                        && error.is_retryable()
                        && retry < self.max_retries =>
                {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_behavior_new_creates_instance() {
        let retry = RetryBehavior::new(3, Duration::from_millis(100));
        assert_eq!(retry.jitter_policy(), RetryJitter::production_default());
    }

    #[test]
    fn retry_behavior_with_jitter_accepts_custom_jitter() {
        let retry = RetryBehavior::with_jitter(
            5,
            Duration::from_secs(1),
            RetryJitter::fixed(Duration::from_millis(50)),
        );
        assert!(matches!(retry.jitter_policy(), RetryJitter::Fixed { .. }));
    }

    #[test]
    fn retry_behavior_delay_for_exponential_backoff() {
        let retry = RetryBehavior::with_jitter(3, Duration::from_millis(100), RetryJitter::none());

        // Delay should double with each retry (multiplier is 1 << retry)
        assert_eq!(retry.delay_for(0), Duration::from_millis(100)); // 100 * 1
        assert_eq!(retry.delay_for(1), Duration::from_millis(200)); // 100 * 2
        assert_eq!(retry.delay_for(2), Duration::from_millis(400)); // 100 * 4
        assert_eq!(retry.delay_for(3), Duration::from_millis(800)); // 100 * 8
    }

    #[test]
    fn retry_behavior_delay_for_respects_overflow() {
        let retry =
            RetryBehavior::with_jitter(100, Duration::from_millis(100), RetryJitter::none());

        // Large retry numbers saturate multiplier to u32::MAX
        // The calculation is: base * (1 << min(retry, 31))
        // For retry >= 31, multiplier = 1 << 31 = u32::MAX / 2 + 1
        let saturated = retry.delay_for(100);
        // Duration::saturating_mul would saturate, but check the actual behavior
        assert!(saturated > Duration::from_secs(100));

        let saturated_max = retry.delay_for(u32::MAX as usize);
        assert!(saturated_max > Duration::from_secs(100));
    }

    #[test]
    fn retry_behavior_delay_for_with_zero_initial_delay() {
        let retry = RetryBehavior::with_jitter(3, Duration::ZERO, RetryJitter::none());

        // All delays should be zero
        assert_eq!(retry.delay_for(0), Duration::ZERO);
        assert_eq!(retry.delay_for(1), Duration::ZERO);
        assert_eq!(retry.delay_for(2), Duration::ZERO);
    }

    #[test]
    fn retry_behavior_fixed_jitter_returns_configured_delay() {
        let retry = RetryBehavior::with_jitter(
            3,
            Duration::from_millis(100),
            RetryJitter::fixed(Duration::from_millis(50)),
        );

        // Fixed jitter ignores base delay and returns configured value
        assert_eq!(retry.delay_for(0), Duration::from_millis(50));
        assert_eq!(retry.delay_for(1), Duration::from_millis(50));
        assert_eq!(retry.delay_for(100), Duration::from_millis(50));
    }

    #[test]
    fn retry_behavior_production_default_uses_full_jitter() {
        let retry = RetryBehavior::new(3, Duration::from_millis(100));
        assert!(matches!(retry.jitter_policy(), RetryJitter::Full { .. }));
    }
}
