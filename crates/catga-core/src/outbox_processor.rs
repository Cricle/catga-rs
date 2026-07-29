use std::{sync::Arc, time::Duration};

use futures::{StreamExt, stream};
use tokio_util::sync::CancellationToken;

use crate::{
    CatgaError, CatgaResult, ErrorCode, MessageTransport, OutboxStore,
    telemetry::{self, OUTBOX_FAILED, OUTBOX_PUBLISHED},
    validate_outbox_claim_limit,
};

/// Counts outcomes from one outbox scan.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OutboxRun {
    published: usize,
    failed: usize,
}

impl OutboxRun {
    /// Returns how many messages were published and acknowledged.
    pub const fn published(self) -> usize {
        self.published
    }

    /// Returns how many delivery attempts or outbox loop operations failed.
    ///
    /// Delivery failures include terminal messages; loop failures include store
    /// and transport errors that prevented a scan from completing.
    pub const fn failed(self) -> usize {
        self.failed
    }

    fn combine(&mut self, other: Self) {
        self.published = self.published.saturating_add(other.published);
        self.failed = self.failed.saturating_add(other.failed);
    }

    fn record_loop_failure(&mut self) {
        self.failed = self.failed.saturating_add(1);
    }
}

/// Timing configuration for a long-running [`OutboxProcessor`].
///
/// ```
/// use std::time::Duration;
/// use catga_core::OutboxLoopOptions;
///
/// let options = OutboxLoopOptions::new(
///     Duration::from_secs(1),
///     Duration::from_millis(500),
/// ).expect("valid intervals");
/// assert_eq!(options.scan_interval(), Duration::from_secs(1));
/// assert!(OutboxLoopOptions::new(Duration::ZERO, Duration::from_secs(1)).is_err());
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutboxLoopOptions {
    scan_interval: Duration,
    error_delay: Duration,
}

impl OutboxLoopOptions {
    /// Creates a background scan schedule with retry delay after store failures.
    pub fn new(scan_interval: Duration, error_delay: Duration) -> CatgaResult<Self> {
        if scan_interval.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "outbox scan interval must be greater than zero",
            ));
        }
        if error_delay.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "outbox error delay must be greater than zero",
            ));
        }
        Ok(Self {
            scan_interval,
            error_delay,
        })
    }

    /// Returns the delay between successful scans.
    pub const fn scan_interval(self) -> Duration {
        self.scan_interval
    }

    /// Returns the delay after an unexpected store error.
    pub const fn error_delay(self) -> Duration {
        self.error_delay
    }
}

impl Default for OutboxLoopOptions {
    fn default() -> Self {
        Self {
            scan_interval: Duration::from_secs(1),
            error_delay: Duration::from_secs(1),
        }
    }
}

/// Claims, publishes, and acknowledges bounded batches from an outbox store.
pub struct OutboxProcessor<S, T> {
    store: Arc<S>,
    transport: Arc<T>,
    owner: Box<str>,
    batch_size: usize,
    concurrency_limit: usize,
}

impl<S, T> OutboxProcessor<S, T>
where
    S: OutboxStore,
    T: MessageTransport,
{
    /// Creates a processor owned by `owner` that claims at most `batch_size` messages per scan.
    pub fn new(
        store: Arc<S>,
        transport: Arc<T>,
        owner: impl Into<Box<str>>,
        batch_size: usize,
    ) -> CatgaResult<Self> {
        Self::new_with_concurrency(store, transport, owner, batch_size, 1)
    }

    /// Creates a processor with a bounded number of concurrent publish-and-acknowledge attempts.
    ///
    /// A limit of one is equivalent to [`Self::new`]. Each claimed message remains independently
    /// acknowledged or released, so a failed publish never affects another message's claim.
    pub fn new_with_concurrency(
        store: Arc<S>,
        transport: Arc<T>,
        owner: impl Into<Box<str>>,
        batch_size: usize,
        concurrency_limit: usize,
    ) -> CatgaResult<Self> {
        if batch_size == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "outbox batch size must be greater than zero",
            ));
        }
        validate_outbox_claim_limit(batch_size)?;
        if concurrency_limit == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "outbox concurrency limit must be greater than zero",
            ));
        }
        Ok(Self {
            store,
            transport,
            owner: owner.into(),
            batch_size,
            concurrency_limit,
        })
    }

    /// Processes one bounded batch and records each failed delivery attempt.
    pub async fn flush_once(&self) -> CatgaResult<OutboxRun> {
        let mut operation = telemetry::persistence_operation("core", "outbox", "flush");
        let result = async {
            let messages = self.store.claim(&self.owner, self.batch_size).await?;
            let mut run = OutboxRun::default();
            let mut deliveries = stream::iter(messages)
                .map(|message| async move {
                    let id = message.id();
                    let claim_token = message.claim_token().ok_or_else(|| {
                        CatgaError::new(
                            ErrorCode::Internal,
                            "outbox store returned a claimed message without a claim token",
                        )
                    })?;
                    match self.transport.publish(message.envelope().clone()).await {
                        Ok(()) => match self.store.ack(&self.owner, id, claim_token).await {
                            Ok(()) => Ok(OutboxRun {
                                published: 1,
                                failed: 0,
                            }),
                            Err(error) => {
                                self.store
                                    .record_failure(
                                        &self.owner,
                                        id,
                                        claim_token,
                                        &failure_reason("outbox acknowledgement failed: ", &error),
                                    )
                                    .await?;
                                Ok(OutboxRun {
                                    published: 0,
                                    failed: 1,
                                })
                            }
                        },
                        Err(error) => {
                            self.store
                                .record_failure(
                                    &self.owner,
                                    id,
                                    claim_token,
                                    &failure_reason("outbox publication failed: ", &error),
                                )
                                .await?;
                            Ok(OutboxRun {
                                published: 0,
                                failed: 1,
                            })
                        }
                    }
                })
                .buffer_unordered(self.concurrency_limit);
            while let Some(delivery) = deliveries.next().await {
                run.combine(delivery?);
            }
            Ok(run)
        }
        .await;
        operation.complete(&result);
        if let Ok(run) = &result {
            metrics::counter!(OUTBOX_PUBLISHED).increment(run.published() as u64);
            metrics::counter!(OUTBOX_FAILED).increment(run.failed() as u64);
        }
        result
    }

    /// Repeatedly flushes bounded batches until `shutdown` is cancelled.
    ///
    /// Cancellation is observed between batches so a claimed message is never
    /// abandoned halfway through a publish-and-acknowledge attempt. Store
    /// failures are counted and retried after `options.error_delay`.
    pub async fn run_until_cancelled(
        &self,
        options: OutboxLoopOptions,
        shutdown: CancellationToken,
    ) -> CatgaResult<OutboxRun> {
        let mut total = OutboxRun::default();
        while !shutdown.is_cancelled() {
            let delay = match self.flush_once().await {
                Ok(run) => {
                    total.combine(run);
                    options.scan_interval()
                }
                Err(_) => {
                    total.record_loop_failure();
                    options.error_delay()
                }
            };
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(delay) => {}
            }
        }
        Ok(total)
    }
}

fn failure_reason(context: &str, error: &CatgaError) -> Box<str> {
    let available = crate::MAX_OUTBOX_FAILURE_ERROR_BYTES.saturating_sub(context.len());
    let message = error.message();
    let end = message
        .char_indices()
        .take_while(|(index, character)| index.saturating_add(character.len_utf8()) <= available)
        .map(|(index, character)| index + character.len_utf8())
        .last()
        .unwrap_or(0);
    let mut reason = String::with_capacity(context.len().saturating_add(end));
    reason.push_str(context);
    reason.push_str(&message[..end]);
    reason.into_boxed_str()
}
