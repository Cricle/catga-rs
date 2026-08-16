//! TiKV-style Pipeline Manager for async batch replication.
//!
//! This module implements the Pipeline pattern from TiKV for high-throughput
//! Raft replication. Instead of waiting for each propose to return, proposals
//! are batched and sent asynchronously.
//!
//! # In-flight accounting invariant
//!
//! `inflight` counts exactly the proposals that live in batches already
//! handed to the batch channel but not yet drained (and proposed into raft)
//! by the owner loop. It is incremented **only after** a batch was
//! successfully sent, and the owner loop must call
//! [`PipelineManager::batch_completed`] with the length of **every** batch
//! it drains. When this invariant holds, `inflight` can never leak and
//! `propose` can never wedge once `max_inflight` entries have flowed
//! through the pipeline.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crossbeam::channel::{Receiver, Sender, TrySendError, bounded};
use parking_lot::{Mutex, MutexGuard};
use tokio::time::{Instant, interval};

use crate::config::PipelineConfig;
use crate::error::{CatgaRaftError, CatgaRaftResult};

/// A proposal entry waiting to be sent through the pipeline.
struct Proposal {
    /// Attribution context matched against committed entries (empty for
    /// fire-and-forget proposes).
    ctx: Vec<u8>,
    /// The raw bytes to propose.
    data: Vec<u8>,
}

/// A batch of proposals ready to be sent.
pub struct ProposalBatch {
    /// The proposals in this batch.
    proposals: Vec<Proposal>,
    /// When this batch was created.
    #[allow(dead_code)]
    created_at: Instant,
}

impl ProposalBatch {
    fn new(proposals: Vec<Proposal>) -> Self {
        Self {
            proposals,
            created_at: Instant::now(),
        }
    }

    /// Consume the batch and return (context, payload) pairs in order.
    /// Contexts are empty for fire-and-forget proposals.
    pub fn into_items(self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.proposals
            .into_iter()
            .map(|p| (p.ctx, p.data))
            .collect()
    }

    /// Number of proposals in this batch.
    ///
    /// The owner loop must pass this to [`PipelineManager::batch_completed`]
    /// after draining the batch so the in-flight counter stays exact.
    pub fn len(&self) -> usize {
        self.proposals.len()
    }

    /// Whether the batch contains no proposals.
    pub fn is_empty(&self) -> bool {
        self.proposals.is_empty()
    }
}

/// Producer state shared between `propose`, `flush` and the flusher task:
/// the pending queue plus the in-flight counter.
///
/// One lock guards both so `propose` performs admission, batching and the
/// batch-size flush in a single lock acquisition, and so the
/// "channel full" check and the `try_send` are serialized across all
/// producers.
struct PipelineState {
    /// Pending proposals waiting to be batched.
    pending: Vec<Proposal>,
    /// Proposals currently in-flight (flushed, not yet consumed by the
    /// owner loop via [`PipelineManager::batch_completed`]).
    inflight: usize,
}

/// TiKV-style Pipeline Manager for async batch replication.
///
/// This manager collects proposals and batches them for efficient Raft
/// replication. Key features:
/// - Async batch propose: returns immediately without waiting for replication
/// - Configurable batch size and flush interval
/// - Background flusher for automatic batch dispatch
///
/// # Example
///
/// ```ignore
/// let config = PipelineConfig {
///     batch_size: 64,
///     flush_interval: Duration::from_millis(1),
///     max_inflight: 1024,
/// };
/// let manager = PipelineManager::new(config);
/// manager.start();
/// ```
pub struct PipelineManager {
    /// Pending proposals + in-flight counter, guarded by one lock.
    state: Arc<Mutex<PipelineState>>,
    /// Batch sender to the background worker.
    batch_tx: Sender<ProposalBatch>,
    /// Batch receiver in the background worker.
    batch_rx: Receiver<ProposalBatch>,
    /// Configuration.
    config: PipelineConfig,
    /// Whether the manager is running.
    running: Arc<AtomicBool>,
    /// Waker pinged on every flushed batch so the raft owner loop drains
    /// immediately instead of waiting for its next tick.
    flush_notify: Arc<tokio::sync::Notify>,
    /// Waker pinged by `propose` (and `stop`) so the flusher task handles
    /// sub-batch "solo" writes immediately instead of waiting out
    /// `flush_interval` for its next tick.
    flusher_notify: Arc<tokio::sync::Notify>,
}

impl PipelineManager {
    /// Create a new PipelineManager with the given configuration.
    pub fn new(config: PipelineConfig) -> Self {
        let (batch_tx, batch_rx) = bounded(config.max_inflight);
        Self {
            state: Arc::new(Mutex::new(PipelineState {
                pending: Vec::with_capacity(config.batch_size),
                inflight: 0,
            })),
            batch_tx,
            batch_rx,
            config,
            running: Arc::new(AtomicBool::new(false)),
            flush_notify: Arc::new(tokio::sync::Notify::new()),
            flusher_notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Waker pinged whenever a batch is flushed; the raft owner loop awaits
    /// it instead of waiting for its next tick.
    pub fn flush_notify(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.flush_notify)
    }

    /// Start the background flush task.
    ///
    /// This spawns a tokio task that periodically flushes pending proposals.
    /// Must be called before proposing.
    pub fn start(&self) {
        self.running.store(true, Ordering::Release);

        let state = Arc::clone(&self.state);
        let batch_tx = self.batch_tx.clone();
        let config = self.config.clone();
        let running = Arc::clone(&self.running);
        let flush_notify = Arc::clone(&self.flush_notify);
        let flusher_notify = Arc::clone(&self.flusher_notify);

        tokio::spawn(async move {
            Self::flush_loop(
                state,
                batch_tx,
                config,
                running,
                flush_notify,
                flusher_notify,
            )
            .await;
        });
    }

    /// Stop the pipeline manager.
    ///
    /// The flusher task wakes, performs one final flush of whatever is still
    /// pending, and exits.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
        // Wake the flusher so it notices the stop without waiting out a
        // whole flush interval (matters when the interval is long).
        self.flusher_notify.notify_one();
    }

    /// Async batch propose - returns immediately without waiting.
    ///
    /// The payload is queued into the batch pipeline and handed to the raft
    /// owner loop, which proposes it to the raft group. This is
    /// fire-and-forget: commit status is not reported back to the caller.
    ///
    /// Hot path: a single lock acquisition covers the running/in-flight
    /// admission checks, the push into the pending queue, and — when the
    /// batch size is reached — the flush itself.
    ///
    /// # Errors
    ///
    /// Returns `CatgaRaftError::NotLeader` if the manager is not running.
    /// Returns `CatgaRaftError::Backpressure` if the pipeline is at
    /// capacity — either `max_inflight` proposals are in flight or the
    /// batch channel to the owner loop is full. Nothing is queued in that
    /// case (no silent drop); the caller may retry later.
    pub fn propose(&self, data: Vec<u8>) -> CatgaRaftResult<()> {
        self.push_proposal(data, Vec::new())
    }

    /// Proposes with an attribution context echoed in the committed entry,
    /// letting the owner loop resolve `propose_and_wait` callers.
    ///
    /// # Errors
    ///
    /// Same admission rules as [`Self::propose`].
    pub fn propose_with_context(&self, data: Vec<u8>, ctx: Vec<u8>) -> CatgaRaftResult<()> {
        self.push_proposal(data, ctx)
    }

    fn push_proposal(&self, data: Vec<u8>, ctx: Vec<u8>) -> CatgaRaftResult<()> {
        // Check if we're running
        if !self.running.load(Ordering::Acquire) {
            return Err(CatgaRaftError::NotLeader);
        }

        let mut state = self.state.lock();

        // Admission: in-flight limit.
        if state.inflight >= self.config.max_inflight {
            return Err(CatgaRaftError::Backpressure);
        }
        // Admission: the batch channel is the only way into the owner loop;
        // if it is full nothing can move, so fail fast instead of silently
        // dropping or piling up unbounded backlog.
        if self.batch_tx.is_full() {
            return Err(CatgaRaftError::Backpressure);
        }

        // Add to pending queue
        state.pending.push(Proposal { ctx, data });

        // Flush immediately once a full batch is accumulated; otherwise wake
        // the flusher so a sub-batch (e.g. solo) write does not have to wait
        // out `flush_interval` for the periodic tick.
        if state.pending.len() >= self.config.batch_size {
            if let Err(e) = Self::flush_locked(&mut state, &self.batch_tx, &self.flush_notify) {
                // The batch could not be shipped. `flush_locked` never drops:
                // it restored the drained proposals to `pending`, so withdraw
                // the one we just added and report the failure — a caller
                // retry must not produce a duplicate.
                state.pending.pop();
                return Err(e);
            }
        } else {
            self.flusher_notify.notify_one();
        }

        Ok(())
    }

    /// Flush pending proposals immediately.
    ///
    /// Called automatically when the batch size is reached and by the
    /// periodic flusher.
    ///
    /// # Errors
    ///
    /// Returns `CatgaRaftError::Backpressure` when the batch channel is
    /// full; nothing is dropped in that case — the proposals stay in the
    /// pending queue and are retried on the next flush.
    pub fn flush(&self) -> CatgaRaftResult<()> {
        let mut state = self.state.lock();
        Self::flush_locked(&mut state, &self.batch_tx, &self.flush_notify)
    }

    /// Drain `pending` into one batch and ship it through the batch channel.
    ///
    /// Must be called while holding the state lock: that serializes every
    /// producer, which keeps the `is_full` check and the `try_send` atomic
    /// with respect to other flushes.
    ///
    /// Accounting: `inflight` is incremented **only after** the send
    /// succeeds, so it never covers a proposal that was not actually queued.
    /// On failure the proposals are restored to the front of `pending` and
    /// nothing is counted — batches are never dropped silently.
    fn flush_locked(
        state: &mut MutexGuard<'_, PipelineState>,
        batch_tx: &Sender<ProposalBatch>,
        flush_notify: &tokio::sync::Notify,
    ) -> CatgaRaftResult<()> {
        if state.pending.is_empty() {
            return Ok(());
        }
        if batch_tx.is_full() {
            return Err(CatgaRaftError::Backpressure);
        }

        let batch = ProposalBatch::new(state.pending.drain(..).collect());
        let count = batch.len();
        match batch_tx.try_send(batch) {
            Ok(()) => {
                state.inflight += count;
                // The batch is on its way; wake the owner loop right now.
                flush_notify.notify_one();
                Ok(())
            }
            Err(err) => {
                // Unreachable while all producers hold the state lock (the
                // consumer only ever frees capacity), but stay honest: put
                // the proposals back and count nothing.
                let batch = match err {
                    TrySendError::Full(batch) | TrySendError::Disconnected(batch) => batch,
                };
                state.pending.splice(0..0, batch.proposals);
                Err(CatgaRaftError::Backpressure)
            }
        }
    }

    /// Background flush loop.
    ///
    /// Wakes on the periodic tick *and* whenever `propose` signals a
    /// sub-batch write (or `stop` requests shutdown). On backpressure the
    /// pending queue is left intact and retried on the next wake — batches
    /// are never dropped. On shutdown one final flush runs before exiting.
    async fn flush_loop(
        state: Arc<Mutex<PipelineState>>,
        batch_tx: Sender<ProposalBatch>,
        config: PipelineConfig,
        running: Arc<AtomicBool>,
        flush_notify: Arc<tokio::sync::Notify>,
        flusher_notify: Arc<tokio::sync::Notify>,
    ) {
        let mut flush_interval = interval(config.flush_interval);
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = flush_interval.tick() => {}
                // Solo/sub-batch proposes (and `stop`) wake us directly so
                // they never wait out the flush interval.
                _ = flusher_notify.notified() => {}
            }

            {
                let mut st = state.lock();
                // Backpressure here is not an error for the caller: the
                // proposals stay pending and the next wake retries.
                let _ = Self::flush_locked(&mut st, &batch_tx, &flush_notify);
            }

            // Check if we should stop (after the flush, so shutdown gets one
            // final flush of whatever was still pending).
            if !running.load(Ordering::Acquire) {
                break;
            }
        }
    }

    /// Get the number of pending proposals.
    pub fn pending_count(&self) -> usize {
        self.state.lock().pending.len()
    }

    /// Get the number of in-flight proposals.
    pub fn inflight_count(&self) -> usize {
        self.state.lock().inflight
    }

    /// Get the batcher receiver for external processing.
    ///
    /// This allows integration with the actual Raft node for sending batches.
    pub fn batch_receiver(&self) -> Receiver<ProposalBatch> {
        self.batch_rx.clone()
    }

    /// Mark `count` proposals as completed, releasing in-flight capacity.
    ///
    /// The owner loop must call this with [`ProposalBatch::len`] for every
    /// batch it drains; that is what closes the accounting loop and keeps
    /// `propose` from wedging at `max_inflight` cumulative proposals.
    /// Saturates at zero so an over-acknowledgement cannot underflow.
    pub fn batch_completed(&self, count: usize) {
        let mut state = self.state.lock();
        state.inflight = state.inflight.saturating_sub(count);
    }
}

impl Default for PipelineManager {
    fn default() -> Self {
        Self::new(PipelineConfig::default())
    }
}

impl Drop for PipelineManager {
    fn drop(&mut self) {
        self.stop();
    }
}
