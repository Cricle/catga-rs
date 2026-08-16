use std::time::Duration;

use async_trait::async_trait;

use crate::{
    Behavior, CatgaError, CatgaResult, ErrorCode, Next, Request, RetryJitter,
    retry_jitter::RetryJitterState,
    telemetry::{RESILIENCE_RETRIES, retry_pending},
};

/// Retries retryable request failures with bounded exponential backoff.
///
/// Only errors whose [`CatgaError::is_retryable`] is true are retried; [`ErrorCode::Cancelled`]
/// always short-circuits without another attempt. The first dispatch is the initial attempt, so
/// `max_retries` adds up to that many *additional* attempts. Delay growth is exponential in the
/// retry index and bounded by the configured [`RetryJitter`] policy.
///
/// ```
/// use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
/// use std::time::Duration;
/// use catga_core::{
///     CatgaError, CatgaResult, ErrorCode, Mediator, Message, Pipeline, Registry,
///     Request, RetryBehavior, RetryJitter, request_handler,
/// };
///
///
/// #[derive(Clone)]
/// struct Ping;
/// impl Message for Ping {}
/// impl Request for Ping { type Response = u64; }
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> CatgaResult<()> {
/// let attempts = Arc::new(AtomicU64::new(0));
/// let observed = Arc::clone(&attempts);
/// let mut registry = Registry::new();
/// registry.register_request::<Ping, _>(request_handler(move |_: Ping| {
///     let observed = Arc::clone(&observed);
///     async move {
///         if observed.fetch_add(1, Ordering::SeqCst) == 0 {
///             Err(CatgaError::new(ErrorCode::Transient, "try again"))
///         } else {
///             Ok(42)
///         }
///     }
/// }))?;
/// let mediator = Mediator::new(registry);
/// let pipeline = Pipeline::<Ping>::new().with(RetryBehavior::with_jitter(
///     2,
///     Duration::ZERO,
///     RetryJitter::fixed(Duration::ZERO),
/// ));
/// assert_eq!(mediator.send_with(Ping, &pipeline).await?, 42);
/// assert_eq!(attempts.load(Ordering::SeqCst), 2);
/// # Ok(())
/// # }
/// ```
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
