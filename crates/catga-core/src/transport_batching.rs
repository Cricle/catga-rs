//! Caller-supervised automatic batching for transport publication.

use std::{collections::VecDeque, future::Future, pin::Pin, sync::Arc, time::Duration};

use tokio::{
    sync::{mpsc, oneshot},
    time::{Instant, sleep_until},
};
use tokio_util::sync::CancellationToken;

use crate::{
    CatgaError, CatgaResult, DEFAULT_TRANSPORT_BATCH_CONCURRENCY, Envelope, ErrorCode,
    MessageTransport,
};

type FlushFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Runtime limits for [`TransportBatcher`].
///
/// Each limit is validated during construction. The queue bounds admitted but
/// unstarted publications; `publish_concurrency` is passed unchanged to the
/// underlying [`MessageTransport::publish_batch_with_concurrency`] call.
#[derive(Clone, Debug)]
pub struct TransportBatchOptions {
    /// Number of envelopes that immediately starts a batch publication.
    pub max_batch_size: usize,
    /// Maximum time the oldest queued envelope waits before publication starts.
    pub batch_timeout: Duration,
    /// Maximum number of unstarted envelopes accepted by the runner.
    pub max_queue_length: usize,
    /// Maximum number of individual publishes active within one transport batch.
    pub publish_concurrency: usize,
}

impl Default for TransportBatchOptions {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            batch_timeout: Duration::from_millis(100),
            max_queue_length: 10_000,
            publish_concurrency: DEFAULT_TRANSPORT_BATCH_CONCURRENCY,
        }
    }
}

impl TransportBatchOptions {
    fn validate(&self) -> CatgaResult<()> {
        if self.max_batch_size == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "transport batch max size must be greater than zero",
            ));
        }
        if self.batch_timeout.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "transport batch timeout must be greater than zero",
            ));
        }
        if self.max_queue_length == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "transport batch queue length must be greater than zero",
            ));
        }
        if self.publish_concurrency == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "transport batch publish concurrency must be greater than zero",
            ));
        }
        Ok(())
    }
}

/// Accepts envelope publications for a caller-owned [`TransportBatchRunner`].
///
/// Construction returns this sender together with its single-use runner. This
/// type never starts a background task: applications must supervise the runner
/// future themselves. Admission uses a bounded Tokio channel and returns
/// [`ErrorCode::Unavailable`] when the runner has stopped or its queue is full.
#[derive(Clone)]
pub struct TransportBatcher {
    sender: mpsc::Sender<QueuedEnvelope>,
}

/// Executes queued transport publications for one [`TransportBatcher`].
///
/// The runner is consumed by [`Self::run_until_cancelled`]. Cancellation
/// rejects unstarted envelopes with [`ErrorCode::Unavailable`] and then waits
/// for the already-started batch publication to reply to its callers.
pub struct TransportBatchRunner {
    receiver: mpsc::Receiver<QueuedEnvelope>,
    transport: Arc<dyn MessageTransport>,
    options: TransportBatchOptions,
}

impl TransportBatcher {
    /// Creates a bounded batcher and its caller-owned runner for `transport`.
    pub fn new(
        transport: Arc<dyn MessageTransport>,
        options: TransportBatchOptions,
    ) -> CatgaResult<(Self, TransportBatchRunner)> {
        options.validate()?;
        let (sender, receiver) = mpsc::channel(options.max_queue_length);
        Ok((
            Self { sender },
            TransportBatchRunner {
                receiver,
                transport,
                options,
            },
        ))
    }

    /// Queues `envelope` and waits for its enclosing transport batch to finish.
    ///
    /// A full or stopped runner returns [`ErrorCode::Unavailable`] immediately.
    pub async fn publish(&self, envelope: Envelope) -> CatgaResult<()> {
        let (reply, response) = oneshot::channel();
        self.sender
            .try_send(QueuedEnvelope {
                envelope,
                reply,
                enqueued_at: Instant::now(),
            })
            .map_err(map_admission_error)?;
        response.await.map_err(|_| {
            CatgaError::new(
                ErrorCode::Unavailable,
                "transport batch runner stopped before replying",
            )
        })?
    }
}

impl TransportBatchRunner {
    /// Processes publication batches until `shutdown` is cancelled.
    ///
    /// The runner starts no detached work. At most one batch is active at a
    /// time, while the transport enforces `publish_concurrency` for envelopes
    /// inside that batch. Cancellation rejects all unstarted envelopes before
    /// draining the active batch.
    pub async fn run_until_cancelled(mut self, shutdown: CancellationToken) -> CatgaResult<()> {
        let mut pending = VecDeque::new();
        let mut flush: Option<FlushFuture> = None;

        loop {
            if flush.is_none() && batch_is_ready(&pending, &self.options) {
                let batch = take_batch(&mut pending, self.options.max_batch_size);
                flush = Some(Box::pin(flush_batch(
                    self.transport.clone(),
                    self.options.publish_concurrency,
                    batch,
                )));
                continue;
            }

            if shutdown.is_cancelled() {
                break;
            }

            if let Some(active_flush) = flush.as_mut() {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    () = active_flush => flush = None,
                }
                continue;
            }

            match batch_deadline(&pending, self.options.batch_timeout) {
                Some(deadline) => {
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        queued = self.receiver.recv() => match queued {
                            Some(queued) => pending.push_back(queued),
                            None => break,
                        },
                        () = sleep_until(deadline) => {},
                    }
                }
                None => {
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        queued = self.receiver.recv() => match queued {
                            Some(queued) => pending.push_back(queued),
                            None => break,
                        },
                    }
                }
            }
        }

        reject_unstarted(&mut self.receiver, &mut pending);
        if let Some(active_flush) = flush {
            active_flush.await;
        }
        Ok(())
    }
}

struct QueuedEnvelope {
    envelope: Envelope,
    reply: oneshot::Sender<CatgaResult<()>>,
    enqueued_at: Instant,
}

fn map_admission_error(error: mpsc::error::TrySendError<QueuedEnvelope>) -> CatgaError {
    match error {
        mpsc::error::TrySendError::Full(_) => CatgaError::new(
            ErrorCode::Unavailable,
            "transport batch runner queue is full",
        ),
        mpsc::error::TrySendError::Closed(_) => CatgaError::new(
            ErrorCode::Unavailable,
            "transport batch runner is unavailable",
        ),
    }
}

fn batch_is_ready(pending: &VecDeque<QueuedEnvelope>, options: &TransportBatchOptions) -> bool {
    pending.len() >= options.max_batch_size
        || batch_deadline(pending, options.batch_timeout)
            .is_some_and(|deadline| deadline <= Instant::now())
}

fn batch_deadline(pending: &VecDeque<QueuedEnvelope>, timeout: Duration) -> Option<Instant> {
    pending
        .front()
        .and_then(|queued| queued.enqueued_at().checked_add(timeout))
}

fn take_batch(
    pending: &mut VecDeque<QueuedEnvelope>,
    max_batch_size: usize,
) -> Vec<QueuedEnvelope> {
    let take = pending.len().min(max_batch_size);
    pending.drain(..take).collect()
}

fn reject_unstarted(
    receiver: &mut mpsc::Receiver<QueuedEnvelope>,
    pending: &mut VecDeque<QueuedEnvelope>,
) {
    while let Ok(queued) = receiver.try_recv() {
        reject_unavailable(queued);
    }
    while let Some(queued) = pending.pop_front() {
        reject_unavailable(queued);
    }
}

fn reject_unavailable(queued: QueuedEnvelope) {
    let _ = queued.reply.send(Err(CatgaError::new(
        ErrorCode::Unavailable,
        "transport batch runner is unavailable",
    )));
}

async fn flush_batch(
    transport: Arc<dyn MessageTransport>,
    publish_concurrency: usize,
    batch: Vec<QueuedEnvelope>,
) {
    let (envelopes, replies): (Vec<_>, Vec<_>) = batch
        .into_iter()
        .map(|queued| (queued.envelope, queued.reply))
        .unzip();
    let result = transport
        .publish_batch_with_concurrency(envelopes, publish_concurrency)
        .await;
    for reply in replies {
        let _ = reply.send(result.clone());
    }
}

impl QueuedEnvelope {
    fn enqueued_at(&self) -> Instant {
        self.enqueued_at
    }
}
