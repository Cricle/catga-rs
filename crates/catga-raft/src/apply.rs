//! ApplyThread: asynchronously applies committed entries to the state machine.
//!
//! This module implements the apply component of TiKV-style Raft,
//! where committed entries are applied to the application state machine
//! in a separate task from the Raft consensus logic.
//!
//! The owner loop hands committed normal entries to a dedicated apply worker
//! over a bounded channel ([`ApplyThread::spawn_worker`] + [`ApplySender`])
//! instead of applying them inline, so the raft loop never stalls on the
//! state-machine mutex. The worker applies entries strictly in channel order
//! (which is strictly increasing by index) and advances `applied_index` only
//! after the state machine accepted an entry.
//!
//! # Snapshot installs vs. queued entries
//!
//! A snapshot replaces the machine state wholesale at its metadata index, so
//! any entry still queued at that moment is superseded by it. The race is
//! closed with an apply *epoch*: [`ApplyThread::install_snapshot`] bumps the
//! epoch before restoring, and the worker discards every message stamped
//! with an older epoch — without applying it. The state-machine mutex
//! serializes the one entry that may be in flight during the bump: it either
//! finished before the restore (its effect is overwritten by the restore) or
//! it re-checks the epoch under the lock and is dropped. Entries sent after
//! the install stamp the new epoch and apply on top of the snapshot.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use catga_core::{CatgaResult, ConsensusStateMachine};
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

/// Default capacity of the bounded apply channel.
///
/// The channel backs up only when the state machine is slower than
/// consensus; when full, the owner loop blocks on send rather than dropping
/// committed entries (see [`ApplySender::send_entry`]).
pub const DEFAULT_APPLY_CHANNEL_CAPACITY: usize = 4096;

/// One message to the apply worker: a committed entry to apply, or a
/// barrier that resolves once everything queued before it was consumed.
enum ApplyMsg {
    Entry {
        index: u64,
        data: Bytes,
        /// Apply epoch at send time; messages from an epoch older than the
        /// machine's current one were superseded by a snapshot install.
        epoch: u64,
    },
    Barrier(oneshot::Sender<()>),
}

/// Sending half of the apply channel, held by the raft owner loop (the sole
/// producer).
///
/// Every entry is stamped with the machine's current apply epoch so a
/// concurrent snapshot install can invalidate everything still queued.
pub struct ApplySender {
    tx: mpsc::Sender<ApplyMsg>,
    epoch: Arc<AtomicU64>,
}

impl ApplySender {
    /// Queues one committed entry for application by the worker.
    ///
    /// Backpressure: when the channel is full — the state machine is slower
    /// than consensus — this awaits capacity instead of dropping the entry.
    /// Dropping committed data would silently diverge the state machine, so
    /// stalling the raft loop is the honest response; capacity returns as
    /// soon as the worker drains an entry.
    ///
    /// # Errors
    ///
    /// [`crate::CatgaRaftError::Apply`] when the worker task is gone
    /// (receiver dropped), which only happens during shutdown.
    pub async fn send_entry(
        &self,
        index: u64,
        data: impl Into<Bytes>,
    ) -> crate::CatgaRaftResult<()> {
        let epoch = self.epoch.load(Ordering::Acquire);
        self.tx
            .send(ApplyMsg::Entry {
                index,
                data: data.into(),
                epoch,
            })
            .await
            .map_err(|_| crate::CatgaRaftError::Apply("apply worker is gone".into()))
    }

    /// Resolves once the worker has consumed every message queued before
    /// this call (applied, or discarded as superseded by a snapshot).
    ///
    /// The owner uses this before advancing the apply frontier for a
    /// configuration-change entry, so the frontier never runs past an
    /// unapplied normal entry.
    pub async fn flush(&self) {
        let (reply_tx, reply_rx) = oneshot::channel();
        if self.tx.send(ApplyMsg::Barrier(reply_tx)).await.is_err() {
            return; // Worker gone during shutdown; nothing left to flush.
        }
        let _ = reply_rx.await;
    }
}

/// ApplyThread asynchronously applies committed entries to the state machine.
///
/// The ApplyThread is responsible for:
/// - Tracking the commit index (highest index committed by Raft)
/// - Tracking the applied index (highest index applied to state machine)
/// - Applying entries in order when they are committed
pub struct ApplyThread<S: ConsensusStateMachine> {
    /// The state machine to apply entries to.
    state_machine: Arc<Mutex<S>>,
    /// The highest index committed by Raft.
    commit_index: AtomicU64,
    /// The highest index applied to the state machine.
    applied_index: AtomicU64,
    /// Apply generation, bumped every time a snapshot is installed. Queued
    /// worker messages stamped with an older generation are discarded: they
    /// are covered by the snapshot.
    epoch: Arc<AtomicU64>,
}

impl<S: ConsensusStateMachine> ApplyThread<S> {
    /// Creates a new ApplyThread with the given state machine.
    pub fn new(state_machine: S) -> Self {
        Self {
            state_machine: Arc::new(Mutex::new(state_machine)),
            commit_index: AtomicU64::new(0),
            applied_index: AtomicU64::new(0),
            epoch: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Spawns the dedicated apply worker fed by a bounded channel of
    /// `capacity` pending entries (see [`DEFAULT_APPLY_CHANNEL_CAPACITY`]).
    ///
    /// Returns the send half for the owner loop and the worker's
    /// `JoinHandle`. The worker exits once the sender is dropped and every
    /// queued entry was applied, so joining it after dropping the sender
    /// drains all committed work handed to it.
    pub fn spawn_worker(
        self: &Arc<Self>,
        capacity: usize,
    ) -> (ApplySender, tokio::task::JoinHandle<()>)
    where
        S: 'static,
    {
        let (tx, rx) = mpsc::channel::<ApplyMsg>(capacity);
        let apply = Arc::clone(self);
        let handle = tokio::spawn(async move {
            apply_worker(apply, rx).await;
        });
        (
            ApplySender {
                tx,
                epoch: Arc::clone(&self.epoch),
            },
            handle,
        )
    }

    /// Updates the commit index.
    ///
    /// This should be called when Raft advances the commit index.
    /// The apply thread will automatically apply any entries between
    /// the current applied index and the new commit index.
    pub fn update_commit_index(&self, index: u64) {
        self.commit_index.store(index, Ordering::SeqCst);
    }

    /// Returns the current commit index.
    pub fn commit_index(&self) -> u64 {
        self.commit_index.load(Ordering::Acquire)
    }

    /// Advances the apply thread by applying all committed but unapplied entries.
    ///
    /// This should be called periodically or when the commit index changes.
    /// It will apply entries in order from `applied_index + 1` to `commit_index`.
    ///
    /// # Arguments
    ///
    /// * `entries` - An iterator yielding (index, data) pairs for entries to apply.
    ///
    ///   Entries should be in ascending order by index.
    ///
    /// # Returns
    ///
    /// Returns the new applied index after advancing.
    pub fn advance(&self, entries: impl Iterator<Item = (u64, Vec<u8>)>) -> CatgaResult<u64> {
        let mut sm = self.state_machine.lock();
        let _commit = self.commit_index.load(Ordering::Acquire);
        let mut applied = self.applied_index.load(Ordering::Acquire);

        for (index, data) in entries {
            // Apply each entry to the state machine
            sm.apply(index, &data)?;
            applied = index;
        }

        // Update the applied index
        self.applied_index.store(applied, Ordering::Release);
        let _ = _commit; // Used for debug assertions in full implementation
        Ok(applied)
    }

    /// Applies a single entry to the state machine.
    ///
    /// This is a convenience method for applying a single entry.
    pub fn apply_entry(&self, index: u64, data: &[u8]) -> CatgaResult<()> {
        let mut sm = self.state_machine.lock();
        sm.apply(index, data)?;
        self.applied_index.fetch_max(index, Ordering::Release);
        Ok(())
    }

    /// Applies one worker-delivered entry, guarded by the apply epoch.
    ///
    /// The epoch is re-checked under the state-machine mutex: between the
    /// worker's fast-path check and lock acquisition, [`Self::install_snapshot`]
    /// may have bumped it and completed the restore; applying a superseded
    /// entry on top of the snapshot would corrupt state, so it is dropped.
    /// `applied_index` advances under the same lock, preserving the
    /// consistent `(state, applied_index)` pair snapshot providers rely on.
    fn apply_queued_entry(&self, index: u64, data: &[u8], epoch: u64) -> CatgaResult<()> {
        let mut sm = self.state_machine.lock();
        if epoch != self.epoch.load(Ordering::Acquire) {
            return Ok(()); // Superseded by a snapshot install; discard.
        }
        sm.apply(index, data)?;
        self.applied_index.fetch_max(index, Ordering::Release);
        Ok(())
    }

    /// Installs a snapshot into the state machine and rebases the apply
    /// frontier at the snapshot's log index.
    ///
    /// Called when a follower receives a snapshot from the leader: `data`
    /// wholesale replaces the machine state (via
    /// [`ConsensusStateMachine::restore`]) and `index` is the snapshot
    /// metadata index, i.e. the last log entry covered by it. Committed
    /// entries with indexes strictly greater than `index` are afterwards
    /// applied on top through [`Self::apply_entry`]; that path tracks the
    /// frontier with `fetch_max`, so the gap between the previous applied
    /// index and `index` is harmless.
    ///
    /// The machine mutex is held across restore and the frontier update, so
    /// a concurrent snapshot provider ([`Self::state_machine`] +
    /// [`Self::applied_index`]) always observes a consistent
    /// `(state, applied_index)` pair.
    pub fn restore(&self, data: &[u8], index: u64) -> CatgaResult<()> {
        let mut sm = self.state_machine.lock();
        sm.restore(data)?;
        self.applied_index.store(index, Ordering::Release);
        // The snapshot covers every entry up to `index`, so commit is at
        // least that high; keep the tracked commit index from lagging the
        // applied frontier.
        self.commit_index.fetch_max(index, Ordering::AcqRel);
        Ok(())
    }

    /// Snapshot install for a machine fed by an apply worker: bumps the
    /// apply epoch so the worker discards every still-queued entry (all of
    /// it is covered by the snapshot), then delegates to [`Self::restore`].
    ///
    /// The epoch bump alone closes the race with the worker:
    /// - queued messages stamp the old epoch and are dropped unapplied;
    /// - the at-most-one in-flight apply is serialized with the restore on
    ///   the state-machine mutex — either it finished first and its effect
    ///   is overwritten by the restore, or it re-checks the epoch under the
    ///   lock ([`Self::apply_queued_entry`]) and is dropped.
    ///
    /// Entries the owner sends afterwards stamp the new epoch and apply on
    /// top of the snapshot in order.
    ///
    /// # Errors
    ///
    /// Propagates [`Self::restore`] failures; the epoch stays bumped, which
    /// is harmless (a retry re-restores, and dropped entries remain covered
    /// by the snapshot).
    pub fn install_snapshot(&self, data: &[u8], index: u64) -> CatgaResult<()> {
        self.epoch.fetch_add(1, Ordering::AcqRel);
        self.restore(data, index)
    }

    /// Advances the applied index past an entry that carries no application
    /// data (a configuration-change entry applies to the raft group itself,
    /// not to the state machine). Without this the applied index would never
    /// pass such an entry, leaving raft's `has_pending_conf` latched and
    /// stalling read barriers behind it.
    pub fn advance_applied_index(&self, index: u64) {
        self.applied_index.fetch_max(index, Ordering::Release);
    }

    /// Returns the current applied index.
    pub fn applied_index(&self) -> u64 {
        self.applied_index.load(Ordering::Acquire)
    }

    /// Returns a reference to the state machine.
    pub fn state_machine(&self) -> &Arc<Mutex<S>> {
        &self.state_machine
    }
}

/// The dedicated apply worker: consumes the apply channel in order and
/// applies each entry to the state machine.
///
/// Order guarantees come from the channel itself: the owner loop is the sole
/// producer and sends committed entries strictly by increasing index, and
/// this loop consumes them FIFO, so entries apply exactly once and strictly
/// in index order. The worker exits when the sender is dropped and the queue
/// has drained, which the owner relies on to join it at shutdown.
async fn apply_worker<S>(apply: Arc<ApplyThread<S>>, mut rx: mpsc::Receiver<ApplyMsg>)
where
    S: ConsensusStateMachine + 'static,
{
    while let Some(msg) = rx.recv().await {
        handle_apply_msg(&apply, msg);
        // Group-drain whatever piled up behind the first message so a burst
        // costs one recv await per batch, not per entry.
        while let Ok(msg) = rx.try_recv() {
            handle_apply_msg(&apply, msg);
        }
    }
}

/// Processes one apply-channel message on the worker.
fn handle_apply_msg<S: ConsensusStateMachine>(apply: &ApplyThread<S>, msg: ApplyMsg) {
    match msg {
        ApplyMsg::Entry { index, data, epoch } => {
            // Fast path: a snapshot install bumped the epoch, so this entry
            // is covered by it and must not touch the machine. The
            // authoritative re-check happens under the machine lock.
            if epoch != apply.epoch.load(Ordering::Acquire) {
                return;
            }
            if let Err(e) = apply.apply_queued_entry(index, &data, epoch) {
                // Same semantics as the historical inline path: warn and move
                // on. The frontier does not advance past a failed entry.
                warn!(target: "catga_raft::apply", index, error = %e, "apply failed");
            }
        }
        ApplyMsg::Barrier(reply) => {
            // Everything queued before the barrier was consumed by now.
            let _ = reply.send(());
        }
    }
}
