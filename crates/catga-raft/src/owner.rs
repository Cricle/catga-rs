//! Owner loop: drives the raft tick/ready cycle for one consensus group.
//!
//! The loop ticks the raft clock, forwards pipeline batches into the raft
//! log, ships wire messages through the transport, applies committed entries
//! to the state machine, and mirrors leadership into the coordinator. On a
//! slow cadence it also compacts the raft log prefix that lies safely behind
//! the apply frontier (see [`maybe_compact_log`]) so long-running clusters
//! do not grow the log without bound.
//!
//! Persistence is asynchronous and split in two phases. Phase 1 happens in
//! the loop right after a `Ready` is taken: hard-state-relevant entries and
//! snapshots are made *readable* in storage without any fsync (raft-rs
//! requires the updates to be readable from `Storage` before
//! `advance_append_async`), and the raft node is advanced immediately so the
//! loop never blocks on disk. Phase 2 happens on a dedicated persist worker:
//! it fsyncs the visible log tail and the hard state in strict ready-number
//! order (group-committing whatever queued up meanwhile), and only then does
//! the owner call `on_persist_ready` and release that ready's persisted
//! messages.
//!
//! Applying committed entries is likewise off the loop: the owner *sends*
//! committed normal entries (index + data) to a dedicated apply worker over
//! a bounded channel instead of locking the state machine inline. The worker
//! applies strictly in channel order (strictly increasing by index) and
//! advances `applied_index` only after the machine accepted an entry, which
//! read barriers and `wait_applied` rely on. When the channel is full the
//! loop blocks on send rather than dropping committed data. Snapshot
//! installs invalidate whatever is still queued through the apply epoch
//! (see [`crate::apply::ApplyThread::install_snapshot`]).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use crossbeam::channel::Receiver as CrossbeamReceiver;
use parking_lot::Mutex;
use protobuf::Message as ProtobufMessage;
use raft::prelude::{
    ConfChange, ConfChangeType, ConfChangeV2, Entry, EntryType, HardState, Message, RawNode,
};
use raft::{INVALID_ID, StateRole};
use tokio::sync::{mpsc, watch};
use tokio::time::{MissedTickBehavior, interval};
use tracing::{debug, warn};

use catga_core::ConsensusStateMachine;

use crate::apply::{ApplySender, ApplyThread};
use crate::coordinator::CatgaRaftCoordinator;
use crate::pipeline::{PipelineManager, ProposalBatch};
use crate::storage::CatgaStorage;
use crate::transport::GrpcTransport;
use crate::{CatgaRaftError, CatgaRaftResult};

const TICK_INTERVAL: Duration = Duration::from_millis(100);

/// Capacity of the bounded channel feeding the apply worker.
///
/// Backpressure choice: when the channel is full — the state machine applies
/// slower than consensus commits — the owner loop blocks on
/// [`ApplySender::send_entry`] instead of dropping entries. Losing committed
/// data would silently corrupt the machine, while stalling the loop is
/// recoverable: capacity returns as soon as the worker drains an entry.
/// 4096 deep keeps even bursty commits off the loop's critical path while
/// bounding queued memory at roughly 4096 entry payloads.
const APPLY_CHANNEL_CAPACITY: usize = 4096;

/// Maximum number of unanswered ReadIndex requests held by the owner loop.
///
/// Without a quorum (no leader, partition) raft never emits `ReadState`s, so
/// queued reads would otherwise accumulate forever (~200B each). Once the cap
/// is reached, new reads are rejected immediately with `Timeout` over their
/// reply channel instead of being queued.
const MAX_PENDING_READS: usize = 4096;

/// Pending ReadIndex requests older than this are failed with `Timeout`.
///
/// A read can only be answered while a leader can confirm its quorum; one
/// that sits longer than this is effectively orphaned, and its oneshot must
/// resolve rather than leak.
const PENDING_READ_TTL: Duration = Duration::from_secs(10);

/// Maximum `propose_and_wait` attributions tracked at once.
const MAX_PENDING_PROPOSES: usize = 4096;

/// Attribution TTL for `propose_and_wait`; expired waits fail with Timeout.
const PENDING_PROPOSE_TTL: Duration = Duration::from_secs(10);

/// One queued ReadIndex request, stamped with the moment it was queued so the
/// tick handler can expire it when no `ReadState` arrives in time.
/// One in-flight `propose_and_wait` attribution: matched against committed
/// entry contexts in `apply_committed`.
struct PendingPropose {
    ctx: Vec<u8>,
    queued_at: Instant,
    reply: tokio::sync::oneshot::Sender<crate::CatgaRaftResult<u64>>,
}

struct PendingRead {
    ctx: Vec<u8>,
    queued_at: Instant,
    reply: tokio::sync::oneshot::Sender<CatgaRaftResult<u64>>,
}

/// Maximum number of unanswered membership-change requests held by the owner
/// loop.
///
/// Raft processes one conf change at a time (a proposal made while another is
/// still pending is dropped), so a deep queue could not drain any faster; past
/// the cap new requests are rejected immediately with `Timeout` over their
/// reply channel instead of being queued.
const MAX_PENDING_CONFS: usize = 16;

/// Pending membership-change requests older than this are failed with
/// `Timeout`.
///
/// A conf change only completes while this node leads a healthy quorum; one
/// that sits longer than this is effectively orphaned, and its oneshot must
/// resolve rather than leak.
const PENDING_CONF_TTL: Duration = Duration::from_secs(10);

/// Tag identifying an add-member request inside [`ConfChangeCtx`].
const CONF_OP_ADD: u8 = 0;
/// Tag identifying a remove-member request inside [`ConfChangeCtx`].
const CONF_OP_REMOVE: u8 = 1;

/// One queued membership-change request, stamped with the moment it was
/// queued so the tick handler can expire it when the conf entry never lands.
struct PendingConfChange {
    ctx: Vec<u8>,
    queued_at: Instant,
    reply: tokio::sync::oneshot::Sender<CatgaRaftResult<()>>,
}

/// Payload carried in both the raft propose context (entry `context`) and the
/// `ConfChange.context` field of a membership-change entry.
///
/// It holds everything any node needs to apply the change — the target member
/// id, the endpoint used to wire the transport on add, and the operation —
/// plus `seq`, which makes every proposal unique so the proposing leader can
/// match the committed entry back to its pending request. Because the bytes
/// travel inside the replicated entry, followers wire the same endpoint
/// without any out-of-band coordination.
#[derive(serde::Serialize, serde::Deserialize)]
struct ConfChangeCtx {
    seq: u64,
    op: u8,
    node_id: u64,
    endpoint: String,
}

fn encode_conf_ctx(ctx: &ConfChangeCtx) -> CatgaRaftResult<Vec<u8>> {
    bincode::serde::encode_to_vec(ctx, bincode::config::standard())
        .map_err(|e| CatgaRaftError::Codec(e.to_string()))
}

fn decode_conf_ctx(bytes: &[u8]) -> Option<ConfChangeCtx> {
    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .ok()
        .map(|(ctx, _)| ctx)
}

/// One unit of durable work handed to the persist worker.
///
/// `number` is the originating `Ready::number` and is used to acknowledge
/// persistence via `on_persist_ready`. The entries and snapshot themselves
/// were already made readable in storage (phase 1); the worker makes them
/// durable. `tail_hint` is the last entry index this ready appended (0 when
/// it appended none) and `snapshot_index` the snapshot metadata index (0
/// when the ready carried no snapshot). `persisted_messages` may only be
/// sent after the worker acknowledges.
struct PersistTask {
    number: u64,
    hard_state: Option<HardState>,
    tail_hint: u64,
    snapshot_index: u64,
    persisted_messages: Vec<Message>,
}

/// Completion signal from the persist worker: the ready `number` is durable
/// and its `messages` are now safe to send.
struct PersistDone {
    number: u64,
    messages: Vec<Message>,
}

/// Runs the raft owner loop until the shutdown watch fires.
///
/// The raft state machine is driven exclusively by this loop: peer messages
/// arrive on `msg_rx` and are stepped here, never from the gRPC callback,
/// because `RawNode` must only be touched by a single driver.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run<S>(
    mut shutdown_rx: watch::Receiver<()>,
    raw_node: Arc<Mutex<RawNode<CatgaStorage>>>,
    storage: CatgaStorage,
    transport: Arc<GrpcTransport>,
    mut batch_rx: CrossbeamReceiver<ProposalBatch>,
    pipeline: Arc<PipelineManager>,
    mut msg_rx: tokio::sync::mpsc::UnboundedReceiver<Message>,
    mut read_rx: tokio::sync::mpsc::UnboundedReceiver<crate::runtime::ReadRequest>,
    mut conf_rx: tokio::sync::mpsc::UnboundedReceiver<crate::runtime::ConfChangeRequest>,
    mut prop_wait_rx: tokio::sync::mpsc::UnboundedReceiver<crate::runtime::ProposeWait>,
    flush_notify: Arc<tokio::sync::Notify>,
    apply: Arc<ApplyThread<S>>,
    coordinator: Arc<CatgaRaftCoordinator>,
    node_id: u64,
    self_endpoint: Option<String>,
    mut peers: HashMap<u64, String>,
) where
    S: ConsensusStateMachine + 'static,
{
    let mut ticker = interval(TICK_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    // Slow, separate cadence for raft log compaction. The check itself is
    // cheap (a couple of index comparisons inside `maybe_compact`); the
    // interval only bounds how often we reclaim the freed prefix. Reading the
    // interval here lets tests shorten it via `set_compaction_interval`.
    let mut compaction_ticker = interval(CatgaStorage::compaction_interval());
    compaction_ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let mut pending_reads: Vec<PendingRead> = Vec::new();
    let mut pending_props: Vec<PendingPropose> = Vec::new();

    // Membership-change requests proposed by this leader and not yet applied.
    // `conf_seq` makes every proposal's context unique so the committed entry
    // can be matched back to its request.
    let mut pending_confs: Vec<PendingConfChange> = Vec::new();
    let mut conf_seq: u64 = 1;

    // Dedicated persist worker: raft-engine fsync happens here, off the raft
    // loop's critical path. It processes tasks in ready-number order. The
    // worker and the loop share the same underlying storage (both variants
    // are Arc-backed), so entries the loop makes visible are exactly what
    // the worker makes durable.
    let (persist_tx, persist_rx) = mpsc::unbounded_channel::<PersistTask>();
    let (persist_done_tx, mut persist_done_rx) = mpsc::unbounded_channel::<PersistDone>();
    let persist_handle = tokio::spawn(persist_worker(storage.clone(), persist_rx, persist_done_tx));

    // Dedicated apply worker: committed normal entries are applied to the
    // state machine there, off the raft loop's critical path (the loop only
    // sends them over a bounded channel; see APPLY_CHANNEL_CAPACITY for the
    // backpressure contract). Conf-change entries stay on the loop — they
    // reconfigure the raft group itself and are not application entries.
    let (apply_tx, apply_handle) = apply.spawn_worker(APPLY_CHANNEL_CAPACITY);

    // Number of persist tasks handed to the worker whose completion has not
    // been observed yet. `process_ready` may only acknowledge an empty ready
    // inline while this is zero (see the ordering rule there).
    let mut persist_in_flight: u64 = 0;

    // Last (role, leader_id) mirrored into the coordinator. The loop checks
    // raft's view every iteration but only touches the coordinator's locks
    // when the view actually changed.
    let mut last_leadership: Option<(StateRole, u64)> = None;

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => break,
            _ = ticker.tick() => {
                raw_node.lock().tick();
                expire_pending_reads(&mut pending_reads);
                expire_pending_confs(&mut pending_confs);
                expire_pending_proposes(&mut pending_props);
            }
            _ = compaction_ticker.tick() => {
                maybe_compact_log(&storage, &apply, node_id);
            }
            msg = msg_rx.recv() => {
                match msg {
                    Some(m) => step_message(&raw_node, m),
                    None => break,
                }
            }
            _ = flush_notify.notified() => {}
            req = read_rx.recv() => {
                match req {
                    Some(r) => queue_read(&raw_node, &mut pending_reads, r),
                    None => break,
                }
            }
            req = conf_rx.recv() => {
                match req {
                    Some(r) => handle_conf_request(&raw_node, &mut pending_confs, &mut conf_seq, r),
                    None => break,
                }
            }
            req = prop_wait_rx.recv() => {
                match req {
                    Some((ctx, reply)) => queue_propose_wait(&mut pending_props, ctx, reply),
                    None => break,
                }
            }
            done = persist_done_rx.recv() => {
                match done {
                    Some(d) => {
                        persist_in_flight = persist_in_flight.saturating_sub(1);
                        handle_persist_done(&raw_node, &transport, d);
                    }
                    None => {
                        // The worker only exits on a persist failure; without
                        // persistence this node cannot make progress.
                        warn!(target: "catga_raft::owner", node_id, "persist worker exited; shutting down owner loop");
                        break;
                    }
                }
            }
        }

        while let Ok(m) = msg_rx.try_recv() {
            step_message(&raw_node, m);
        }
        while let Ok(r) = read_rx.try_recv() {
            queue_read(&raw_node, &mut pending_reads, r);
        }
        while let Ok(r) = conf_rx.try_recv() {
            handle_conf_request(&raw_node, &mut pending_confs, &mut conf_seq, r);
        }
        while let Ok((ctx, reply)) = prop_wait_rx.try_recv() {
            queue_propose_wait(&mut pending_props, ctx, reply);
        }
        while let Ok(d) = persist_done_rx.try_recv() {
            persist_in_flight = persist_in_flight.saturating_sub(1);
            handle_persist_done(&raw_node, &transport, d);
        }

        drain_and_propose(&raw_node, &mut batch_rx, &pipeline);
        if let Err(e) = process_ready(
            &raw_node,
            &storage,
            &transport,
            &apply,
            &apply_tx,
            &mut pending_reads,
            &coordinator,
            &mut peers,
            &mut pending_confs,
            &mut pending_props,
            &persist_tx,
            &mut persist_in_flight,
        )
        .await
        {
            warn!(target: "catga_raft::owner", error = %e, "raft ready processing failed");
        }

        let leadership = {
            let rn = raw_node.lock();
            (rn.raft.state, rn.raft.leader_id)
        };
        if last_leadership != Some(leadership) {
            let (state, leader_id) = leadership;
            update_leadership(&coordinator, state, leader_id, &self_endpoint, &peers);
            last_leadership = Some(leadership);
        }
    }

    // Drain the persist worker so no task leaks and every queued persist lands
    // before we return. Dropping the sender closes the channel once the worker
    // has consumed what is already queued.
    drop(persist_tx);
    let _ = persist_handle.await;

    // Drain the apply worker the same way: dropping the sender closes the
    // channel once the queued entries are consumed, and joining guarantees
    // every committed entry handed to the worker was applied before the
    // owner returns (clean shutdown, no apply loss).
    drop(apply_tx);
    let _ = apply_handle.await;

    debug!(target: "catga_raft::owner", node_id, "owner loop exited");
}

/// Sequential persist worker with group commit (async persist phase 2).
///
/// Processes [`PersistTask`]s strictly in arrival (ready-number) order. After
/// receiving one task it drains everything already queued and makes the whole
/// batch durable behind a single round of fsyncs: one storage drain covers
/// every queued log tail (a raft-engine `write` with `sync = true` fsyncs the
/// entire append queue), snapshot metadata is persisted in ready order, and
/// the batch's last hard state wins (hard state is monotonic). The heavy part
/// runs on the blocking thread pool so it never occupies an async runtime
/// thread.
///
/// The log tail is drained before the hard state is written, so a crash never
/// leaves the companion file claiming a tail that raft-engine does not
/// durably hold. On success every ready number in the batch is acknowledged
/// in order on `done_tx`; on failure the worker stops, which the owner treats
/// as fatal.
async fn persist_worker(
    storage: CatgaStorage,
    mut rx: mpsc::UnboundedReceiver<PersistTask>,
    done_tx: mpsc::UnboundedSender<PersistDone>,
) {
    while let Some(first) = rx.recv().await {
        // Group commit: everything already queued rides the same fsync(s).
        let mut tasks = vec![first];
        while let Ok(task) = rx.try_recv() {
            tasks.push(task);
        }

        let mut hard_state: Option<HardState> = None;
        let mut up_to: u64 = 0;
        let mut snapshot_indexes: Vec<u64> = Vec::new();
        for task in &mut tasks {
            if let Some(hs) = task.hard_state.take() {
                hard_state = Some(hs);
            }
            up_to = up_to.max(task.tail_hint);
            if task.snapshot_index > 0 {
                snapshot_indexes.push(task.snapshot_index);
            }
        }

        let storage = storage.clone();
        let result = tokio::task::spawn_blocking(move || -> CatgaRaftResult<()> {
            if up_to > 0 {
                storage.drain_visible(up_to)?;
            }
            for snapshot_index in snapshot_indexes {
                storage.persist_snapshot_state(snapshot_index)?;
            }
            if let Some(hs) = hard_state {
                storage.persist_hard_state(hs)?;
            }
            Ok(())
        })
        .await;
        match result {
            Ok(Ok(())) => {
                for task in tasks {
                    let _ = done_tx.send(PersistDone {
                        number: task.number,
                        messages: task.persisted_messages,
                    });
                }
            }
            Ok(Err(e)) => {
                warn!(target: "catga_raft::owner", error = %e, "persist failed; halting persist worker");
                break;
            }
            Err(e) => {
                warn!(target: "catga_raft::owner", error = %e, "persist task panicked; halting persist worker");
                break;
            }
        }
    }
}

/// Acknowledges a persisted ready and releases its persisted messages.
fn handle_persist_done(
    raw_node: &Arc<Mutex<RawNode<CatgaStorage>>>,
    transport: &Arc<GrpcTransport>,
    done: PersistDone,
) {
    raw_node.lock().on_persist_ready(done.number);
    spawn_send(Arc::clone(transport), done.messages);
}

fn queue_read(
    raw_node: &Arc<Mutex<RawNode<CatgaStorage>>>,
    pending_reads: &mut Vec<PendingRead>,
    request: crate::runtime::ReadRequest,
) {
    // Bounded queue: without a quorum these reads can never be answered, so
    // beyond the cap reject immediately instead of accumulating forever.
    if pending_reads.len() >= MAX_PENDING_READS {
        warn!(
            target: "catga_raft::owner",
            cap = MAX_PENDING_READS,
            "pending read queue full; rejecting ReadIndex request"
        );
        let _ = request.1.send(Err(CatgaRaftError::Timeout));
        return;
    }
    raw_node.lock().read_index(request.0.clone());
    pending_reads.push(PendingRead {
        ctx: request.0,
        queued_at: Instant::now(),
        reply: request.1,
    });
}

/// Fails every pending read older than [`PENDING_READ_TTL`] with `Timeout`.
///
/// Reads are queued in order, so staleness is a prefix of the vec; only the
/// expired prefix is drained. Dropping a read here resolves its oneshot (via
/// the explicit send), so a caller awaiting `read_index` on a quorum-less
/// node gets `Err(Timeout)` instead of waiting forever.
fn expire_pending_reads(pending_reads: &mut Vec<PendingRead>) {
    if pending_reads.is_empty() {
        return;
    }
    let stale = pending_reads.partition_point(|r| r.queued_at.elapsed() >= PENDING_READ_TTL);
    if stale == 0 {
        return;
    }
    for read in pending_reads.drain(..stale) {
        let _ = read.reply.send(Err(CatgaRaftError::Timeout));
    }
}

fn queue_propose_wait(
    pending_props: &mut Vec<PendingPropose>,
    ctx: Vec<u8>,
    reply: tokio::sync::oneshot::Sender<crate::CatgaRaftResult<u64>>,
) {
    if pending_props.len() >= MAX_PENDING_PROPOSES {
        let _ = reply.send(Err(crate::CatgaRaftError::Timeout));
        return;
    }
    pending_props.push(PendingPropose {
        ctx,
        queued_at: Instant::now(),
        reply,
    });
}

/// Resolves a waiting proposer when its context appears on a committed entry.
/// Returns true when the context matched (so other matchers skip it).
fn resolve_propose_wait(pending_props: &mut Vec<PendingPropose>, ctx: &[u8], index: u64) -> bool {
    if ctx.is_empty() {
        return false;
    }
    if let Some(pos) = pending_props.iter().position(|p| p.ctx == ctx) {
        let pending = pending_props.swap_remove(pos);
        let _ = pending.reply.send(Ok(index));
        true
    } else {
        false
    }
}

/// Fails every pending propose-wait older than [`PENDING_PROPOSE_TTL`].
fn expire_pending_proposes(pending_props: &mut Vec<PendingPropose>) {
    if pending_props.is_empty() {
        return;
    }
    // FIFO insertion keeps staleness a prefix.
    let stale = pending_props.partition_point(|p| p.queued_at.elapsed() >= PENDING_PROPOSE_TTL);
    if stale == 0 {
        return;
    }
    for pending in pending_props.drain(..stale) {
        let _ = pending.reply.send(Err(crate::CatgaRaftError::Timeout));
    }
}

/// Handles one membership-change request from the runtime.
///
/// Leaders encode the request into a unique context, propose it as a raft
/// conf change, and track it in `pending_confs`; the matching committed entry
/// resolves the reply (see [`apply_conf_entry`]). Non-leaders reject with
/// `NotLeader`, and raft-level proposal failures surface over the reply
/// channel instead of being swallowed.
fn handle_conf_request(
    raw_node: &Arc<Mutex<RawNode<CatgaStorage>>>,
    pending_confs: &mut Vec<PendingConfChange>,
    conf_seq: &mut u64,
    request: crate::runtime::ConfChangeRequest,
) {
    let (op, reply) = request;

    // Bounded queue: raft commits at most one conf change at a time, so past
    // the cap reject immediately instead of accumulating forever.
    if pending_confs.len() >= MAX_PENDING_CONFS {
        warn!(
            target: "catga_raft::owner",
            cap = MAX_PENDING_CONFS,
            "pending conf-change queue full; rejecting membership request"
        );
        let _ = reply.send(Err(CatgaRaftError::Timeout));
        return;
    }

    let (change_type, op_tag, target_id, endpoint) = match &op {
        crate::runtime::ConfChangeOp::Add { node_id, endpoint } => (
            ConfChangeType::AddNode,
            CONF_OP_ADD,
            *node_id,
            endpoint.clone(),
        ),
        crate::runtime::ConfChangeOp::Remove { node_id } => (
            ConfChangeType::RemoveNode,
            CONF_OP_REMOVE,
            *node_id,
            String::new(),
        ),
    };

    let mut rn = raw_node.lock();
    if rn.raft.state != StateRole::Leader {
        drop(rn);
        let _ = reply.send(Err(CatgaRaftError::NotLeader));
        return;
    }

    let ctx_bytes = match encode_conf_ctx(&ConfChangeCtx {
        seq: *conf_seq,
        op: op_tag,
        node_id: target_id,
        endpoint,
    }) {
        Ok(bytes) => bytes,
        Err(e) => {
            drop(rn);
            let _ = reply.send(Err(e));
            return;
        }
    };
    *conf_seq += 1;

    // The same bytes ride the entry context (for matching the reply below)
    // and the ConfChange context (so every node applying the entry can wire
    // the new peer's transport endpoint).
    let mut cc = ConfChange::default();
    cc.set_change_type(change_type);
    cc.set_node_id(target_id);
    cc.set_context(ctx_bytes.clone().into());

    if let Err(e) = rn.propose_conf_change(ctx_bytes.clone(), cc) {
        drop(rn);
        warn!(target: "catga_raft::owner", node_id = target_id, error = %e, "conf change propose failed");
        let _ = reply.send(Err(CatgaRaftError::Raft(e.to_string())));
        return;
    }
    drop(rn);

    pending_confs.push(PendingConfChange {
        ctx: ctx_bytes,
        queued_at: Instant::now(),
        reply,
    });
}

/// Fails every pending membership-change request older than
/// [`PENDING_CONF_TTL`] with `Timeout`.
///
/// Requests are queued in order, so staleness is a prefix of the vec; only
/// the expired prefix is drained. Dropping a request here resolves its
/// oneshot, so a caller awaiting `add_member`/`remove_member` on a node that
/// lost leadership (or whose proposal was superseded) gets `Err(Timeout)`
/// instead of waiting forever.
fn expire_pending_confs(pending_confs: &mut Vec<PendingConfChange>) {
    if pending_confs.is_empty() {
        return;
    }
    let stale = pending_confs.partition_point(|r| r.queued_at.elapsed() >= PENDING_CONF_TTL);
    if stale == 0 {
        return;
    }
    for conf in pending_confs.drain(..stale) {
        let _ = conf.reply.send(Err(CatgaRaftError::Timeout));
    }
}

fn step_message(raw_node: &Arc<Mutex<RawNode<CatgaStorage>>>, msg: Message) {
    if let Err(e) = raw_node.lock().step(msg) {
        warn!(target: "catga_raft::owner", error = %e, "raft step failed");
    }
}

/// Drain every queued pipeline batch into the raft log and acknowledge it.
///
/// Each batch is acknowledged with [`PipelineManager::batch_completed`] using
/// its exact length, closing the in-flight accounting loop: a proposal counts
/// as in flight from the moment its batch is flushed until the owner hands it
/// to raft here. Without this acknowledgement the in-flight counter only ever
/// grew and `propose` wedged forever once `max_inflight` entries had flowed
/// through the pipeline.
///
/// The full batch length is acknowledged even if raft rejects individual
/// entries (logged below): a rejected entry is dropped and no longer in the
/// pipeline either way, so withholding its acknowledgement would leak
/// capacity just the same.
fn drain_and_propose(
    raw_node: &Arc<Mutex<RawNode<CatgaStorage>>>,
    batch_rx: &mut CrossbeamReceiver<ProposalBatch>,
    pipeline: &PipelineManager,
) {
    while let Ok(batch) = batch_rx.try_recv() {
        let count = batch.len();
        for (ctx, data) in batch.into_items() {
            if let Err(e) = raw_node.lock().propose(ctx, data) {
                warn!(target: "catga_raft::owner", error = %e, "raft propose dropped");
            }
        }
        pipeline.batch_completed(count);
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_ready<S>(
    raw_node: &Arc<Mutex<RawNode<CatgaStorage>>>,
    storage: &CatgaStorage,
    transport: &Arc<GrpcTransport>,
    apply: &Arc<ApplyThread<S>>,
    apply_tx: &ApplySender,
    pending_reads: &mut Vec<PendingRead>,
    coordinator: &Arc<CatgaRaftCoordinator>,
    peers: &mut HashMap<u64, String>,
    pending_confs: &mut Vec<PendingConfChange>,
    pending_props: &mut Vec<PendingPropose>,
    persist_tx: &mpsc::UnboundedSender<PersistTask>,
    persist_in_flight: &mut u64,
) -> CatgaRaftResult<()>
where
    S: ConsensusStateMachine + 'static,
{
    let mut rd = {
        let mut rn = raw_node.lock();
        if !rn.has_ready() {
            return Ok(());
        }
        rn.ready()
    };

    let number = rd.number();
    let hard_state = rd.hs().cloned();
    let tail_hint = rd.entries().last().map(|e| e.index).unwrap_or(0);
    let snapshot_index = rd.snapshot().get_metadata().index;

    // Phase 1 (no fsync): make the ready's updates readable from storage,
    // which raft-rs requires before `advance_append_async`. The memory
    // variant is durable by construction; the engine variant parks the
    // entries in its pending overlay for the persist worker. If this fails,
    // bail out before advancing so the ready is regenerated next iteration.
    if !rd.entries().is_empty() {
        storage.append_visible(rd.entries())?;
    }
    if snapshot_index > 0 {
        let snapshot = rd.snapshot().clone();
        storage.apply_snapshot_visible(snapshot.clone())?;
        // Install the snapshot's state: the machine is replaced wholesale
        // and the apply frontier jumps to the snapshot index. `install_snapshot`
        // first bumps the apply epoch, so every entry still queued on the
        // apply channel is discarded by the worker without being applied —
        // it is all covered by the snapshot, and applying it on top would
        // replay stale writes. Committed entries with higher indexes
        // (delivered from the next ready on, stamped with the new epoch)
        // apply on top of it. If the restore fails, bail out before
        // advancing so the ready is regenerated and retried next iteration;
        // both storage halves tolerate the re-application, and the bumped
        // epoch stays harmless (the dropped entries remain covered).
        if let Err(e) = apply.install_snapshot(snapshot.get_data(), snapshot_index) {
            return Err(CatgaRaftError::Storage(format!(
                "restore snapshot at index {snapshot_index}: {e}"
            )));
        }
    }

    // Pull the remaining pieces out of the ready before it is consumed by
    // `advance_append_async`.
    let persisted_messages = rd.take_persisted_messages();
    let messages = rd.take_messages();
    let read_states = rd.take_read_states();
    let committed = rd.take_committed_entries();

    // Let raft keep moving without waiting on fsync; durability is
    // acknowledged later through `on_persist_ready`.
    raw_node.lock().advance_append_async(rd);

    // Durability work. A ready that carries nothing durable (no hard state,
    // no entries, no snapshot, no persisted messages — e.g. a leader's
    // heartbeat or a read-state-only ready) needs no worker round-trip: it
    // is acknowledged inline.
    //
    // Ordering rule: `on_persist_ready(n)` implicitly claims every smaller
    // ready number is durable, so the inline acknowledgement must not run
    // ahead of an earlier real persist still queued in the worker. Skipping
    // is therefore only allowed while `persist_in_flight == 0`; whenever any
    // real task is in flight the empty ready rides the normal round-trip so
    // acknowledgements stay in ready-number order.
    let nothing_to_persist = hard_state.is_none()
        && tail_hint == 0
        && snapshot_index == 0
        && persisted_messages.is_empty();
    if nothing_to_persist && *persist_in_flight == 0 {
        // Nothing durable outstanding anywhere: acknowledge inline and skip
        // the channel round-trip. The raft record for this ready carries no
        // entries or snapshot, so the ack only drains bookkeeping.
        raw_node.lock().on_persist_ready(number);
    } else {
        if persist_tx
            .send(PersistTask {
                number,
                hard_state,
                tail_hint,
                snapshot_index,
                persisted_messages,
            })
            .is_err()
        {
            warn!(target: "catga_raft::owner", number, "persist worker is gone; ready not persisted");
        } else {
            *persist_in_flight += 1;
        }
    }

    for rs in read_states {
        if let Some(pos) = pending_reads.iter().position(|r| r.ctx == rs.request_ctx) {
            let read = pending_reads.swap_remove(pos);
            let _ = read.reply.send(Ok(rs.index));
        }
    }

    // Non-persisted messages (a leader's appends) go out immediately; sends are
    // spawned so the raft loop never blocks on RPC round-trips. Persisted
    // messages travel with the worker completion instead.
    spawn_send(Arc::clone(transport), messages);
    apply_committed(
        raw_node,
        storage,
        transport,
        apply,
        apply_tx,
        coordinator,
        peers,
        pending_confs,
        pending_props,
        committed,
    )
    .await;

    let applied = apply.applied_index();
    if applied > 0 {
        let mut rn = raw_node.lock();
        // raft-rs rejects an applied advance past min(committed, persisted).
        // Right after installing a snapshot the apply thread already sits at
        // the snapshot index while raft's `persisted` still waits for that
        // ready's persist acknowledgement, so clamp the target until the ack
        // lands (normally `applied` never runs ahead of `persisted`, making
        // the clamp a no-op).
        let target = applied.min(rn.raft.raft_log.persisted);
        if target > rn.raft.raft_log.applied {
            rn.advance_apply_to(target);
        }
    }

    Ok(())
}

fn spawn_send(transport: Arc<GrpcTransport>, messages: Vec<Message>) {
    let grouped = group_by_to(messages);
    if grouped.is_empty() {
        return;
    }
    tokio::spawn(async move {
        if let Err(e) = transport.send_grouped(grouped).await {
            warn!(target: "catga_raft::owner", error = %e, "raft message send failed");
        }
    });
}

fn group_by_to(messages: Vec<Message>) -> HashMap<u64, Vec<Bytes>> {
    let mut grouped: HashMap<u64, Vec<Bytes>> = HashMap::new();
    for msg in messages {
        let to = msg.to;
        match ProtobufMessage::write_to_bytes(&msg) {
            Ok(bytes) => grouped.entry(to).or_default().push(bytes.into()),
            Err(e) => {
                warn!(target: "catga_raft::owner", to, error = %e, "raft message encode failed")
            }
        }
    }
    grouped
}

/// Applies committed entries.
///
/// Normal entries are *sent* to the apply worker (index + data), which
/// applies them strictly in order off the raft loop; conf-change entries
/// reconfigure the raft group itself (see [`apply_conf_entry`]) and stay on
/// the loop.
#[allow(clippy::too_many_arguments)]
async fn apply_committed<S>(
    raw_node: &Arc<Mutex<RawNode<CatgaStorage>>>,
    storage: &CatgaStorage,
    transport: &Arc<GrpcTransport>,
    apply: &Arc<ApplyThread<S>>,
    apply_tx: &ApplySender,
    coordinator: &Arc<CatgaRaftCoordinator>,
    peers: &mut HashMap<u64, String>,
    pending_confs: &mut Vec<PendingConfChange>,
    pending_props: &mut Vec<PendingPropose>,
    entries: Vec<Entry>,
) where
    S: ConsensusStateMachine + 'static,
{
    for entry in entries {
        match entry.get_entry_type() {
            EntryType::EntryNormal => {
                // Attribute the committed entry to a propose_and_wait caller
                // before anything else: contexts are unique per request, and
                // attribution resolves at COMMIT here on the owner loop —
                // independent of when the worker applies the entry.
                resolve_propose_wait(pending_props, &entry.context, entry.index);
                if entry.data.is_empty() {
                    continue;
                }
                // Hand the entry to the apply worker. Backpressure: a full
                // channel makes this await rather than drop committed data
                // (see APPLY_CHANNEL_CAPACITY). Apply failures keep their
                // historical warn-and-move-on semantics inside the worker.
                if let Err(e) = apply_tx.send_entry(entry.index, entry.data).await {
                    warn!(target: "catga_raft::owner", index = entry.index, error = %e, "apply enqueue failed");
                }
            }
            EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => {
                // A conf entry advances the apply frontier itself
                // (`advance_applied_index`); drain the apply queue first so
                // the frontier never runs past a normal entry that is still
                // queued at the worker. Conf changes are rare, so the
                // barrier round-trip costs nothing on the hot path.
                apply_tx.flush().await;
                apply_conf_entry(
                    raw_node,
                    storage,
                    transport,
                    apply,
                    coordinator,
                    peers,
                    pending_confs,
                    entry,
                )
                .await;
            }
        }
    }
}

/// A decoded conf-change entry, kept in its wire type so `apply_conf_change`
/// receives the exact message raft expects for each entry type.
enum DecodedConf {
    V1(ConfChange),
    V2(ConfChangeV2),
}

/// Applies one committed conf-change entry on this node.
///
/// Tells raft about the new membership (`RawNode::apply_conf_change`),
/// persists the resulting conf state, wires or unwires the transport peer
/// described by the entry's context, mirrors the membership into the
/// coordinator, advances the apply frontier past the entry, and resolves the
/// leader-side pending request that proposed it, if any.
///
/// Every node runs this path — leaders and followers alike — so all members
/// wire the same transport endpoint purely from the replicated entry.
#[allow(clippy::too_many_arguments)]
async fn apply_conf_entry<S>(
    raw_node: &Arc<Mutex<RawNode<CatgaStorage>>>,
    storage: &CatgaStorage,
    transport: &Arc<GrpcTransport>,
    apply: &Arc<ApplyThread<S>>,
    coordinator: &Arc<CatgaRaftCoordinator>,
    peers: &mut HashMap<u64, String>,
    pending_confs: &mut Vec<PendingConfChange>,
    entry: Entry,
) where
    S: ConsensusStateMachine + 'static,
{
    let index = entry.index;
    let entry_ctx = entry.get_context().to_vec();

    // Decode the protobuf payload. Empty data is legal for an auto-leaving
    // `EntryConfChangeV2` and decodes to a default (change-less) message.
    let decoded = if entry.get_entry_type() == EntryType::EntryConfChange {
        let mut cc = ConfChange::default();
        if let Err(e) = ProtobufMessage::merge_from_bytes(&mut cc, &entry.data) {
            warn!(target: "catga_raft::owner", index, error = %e, "conf change decode failed");
            return;
        }
        DecodedConf::V1(cc)
    } else {
        let mut cc = ConfChangeV2::default();
        if let Err(e) = ProtobufMessage::merge_from_bytes(&mut cc, &entry.data) {
            warn!(target: "catga_raft::owner", index, error = %e, "conf change v2 decode failed");
            return;
        }
        DecodedConf::V2(cc)
    };

    // Normalize to a single description of the change. `target_id == 0`
    // marks entries with no single wireable peer (multi-change joint
    // proposals), which still apply to raft below but skip transport wiring.
    let (change_type, target_id, cc_ctx) = match &decoded {
        DecodedConf::V1(cc) => (
            cc.get_change_type(),
            cc.get_node_id(),
            cc.get_context().to_vec(),
        ),
        DecodedConf::V2(cc) => {
            let changes = cc.get_changes();
            if changes.len() == 1 {
                (
                    changes[0].get_change_type(),
                    changes[0].get_node_id(),
                    cc.get_context().to_vec(),
                )
            } else {
                warn!(
                    target: "catga_raft::owner",
                    index,
                    changes = changes.len(),
                    "multi-change conf entry applied without transport wiring"
                );
                (ConfChangeType::AddNode, 0, cc.get_context().to_vec())
            }
        }
    };

    // Apply to the raft group; the owner loop is the sole RawNode driver.
    let conf_state = {
        let mut rn = raw_node.lock();
        match &decoded {
            DecodedConf::V1(cc) => rn.apply_conf_change(cc),
            DecodedConf::V2(cc) => rn.apply_conf_change(cc),
        }
    };
    let conf_state = match conf_state {
        Ok(cs) => cs,
        Err(e) => {
            warn!(target: "catga_raft::owner", index, error = %e, "apply_conf_change failed");
            resolve_pending(
                pending_confs,
                &entry_ctx,
                Some(CatgaRaftError::Raft(e.to_string())),
            );
            // Advance anyway so one broken entry cannot wedge the frontier.
            apply.advance_applied_index(index);
            return;
        }
    };

    if let Err(e) = storage.set_conf_state(conf_state) {
        warn!(target: "catga_raft::owner", index, error = %e, "conf state persist failed");
    }

    // Wire or unwire the transport peer carried in the entry's context.
    if target_id != 0 {
        let endpoint = decode_conf_ctx(&cc_ctx)
            .map(|ctx| ctx.endpoint)
            .unwrap_or_default();
        match change_type {
            ConfChangeType::AddNode | ConfChangeType::AddLearnerNode => {
                if endpoint.is_empty() {
                    warn!(
                        target: "catga_raft::owner",
                        index,
                        node_id = target_id,
                        "conf entry carries no endpoint; transport peer not added"
                    );
                } else {
                    if target_id != transport.local_node_id()
                        && let Err(e) = transport.add_peer(target_id, endpoint.clone()).await
                    {
                        warn!(
                            target: "catga_raft::owner",
                            index,
                            node_id = target_id,
                            error = %e,
                            "transport add_peer failed"
                        );
                    }
                    peers.insert(target_id, endpoint);
                }
            }
            ConfChangeType::RemoveNode => {
                if transport.has_peer(target_id)
                    && let Err(e) = transport.remove_peer(target_id)
                {
                    warn!(
                        target: "catga_raft::owner",
                        index,
                        node_id = target_id,
                        error = %e,
                        "transport remove_peer failed"
                    );
                }
                peers.remove(&target_id);
            }
        }

        // Mirror the new membership into the coordinator (peer endpoints
        // only; the self endpoint stays excluded, matching the builder).
        coordinator.set_members(peers.values().cloned().collect());
    }

    // Raft keys "a conf change is pending" off its applied index, so advance
    // the apply frontier past this entry; otherwise the next conf change
    // would be dropped and read barriers behind it would stall.
    apply.advance_applied_index(index);

    // Resolve the leader-side request that proposed this entry, if any.
    resolve_pending(pending_confs, &entry_ctx, None);
}

/// Resolves the pending membership request whose propose context matches
/// `ctx`, if this node proposed one. `error` is `None` for a successful
/// apply, `Some` when the entry applied but the change itself failed.
fn resolve_pending(
    pending_confs: &mut Vec<PendingConfChange>,
    ctx: &[u8],
    error: Option<CatgaRaftError>,
) {
    if ctx.is_empty() {
        return;
    }
    if let Some(pos) = pending_confs.iter().position(|p| p.ctx == ctx) {
        let pending = pending_confs.swap_remove(pos);
        let _ = pending.reply.send(match error {
            None => Ok(()),
            Some(e) => Err(e),
        });
    }
}

/// Mirrors a changed raft leadership view into the coordinator.
///
/// The owner loop reads `(state, leader_id)` every iteration but only calls
/// this when the pair changed, so the coordinator's two `RwLock` writes and
/// the `Arc<str>` allocation happen only on real transitions instead of on
/// every loop turn. The endpoint mapping itself is unchanged.
fn update_leadership(
    coordinator: &Arc<CatgaRaftCoordinator>,
    state: StateRole,
    leader_id: u64,
    self_endpoint: &Option<String>,
    peers: &HashMap<u64, String>,
) {
    let endpoint = if state == StateRole::Leader {
        self_endpoint.clone()
    } else if leader_id != INVALID_ID && leader_id != 0 {
        peers.get(&leader_id).cloned()
    } else {
        None
    };
    coordinator.set_leader(endpoint);
}

/// Slow-cadence raft log compaction driven from the owner loop.
///
/// Compacts the storage log prefix that lies strictly below
/// `applied_index - COMPACTION_SAFETY_MARGIN`. The applied index is the only
/// input; [`CatgaStorage::maybe_compact`] enforces the conservative bound, so
/// this never deletes an entry raft itself, a temporarily lagging follower,
/// or a restart replay may still read. HardState and ConfState are untouched.
///
/// raft-rs does not need to be told about storage-side compaction: it reads
/// every entry through the `Storage` trait, clamps the entries it applies to
/// `max(applied + 1, first_index)`, and falls back to the snapshot path for a
/// peer whose `next_idx` drops below the compaction boundary. The safety
/// margin keeps that fallback out of reach for peers that are merely lagging.
fn maybe_compact_log<S>(storage: &CatgaStorage, apply: &Arc<ApplyThread<S>>, node_id: u64)
where
    S: ConsensusStateMachine + 'static,
{
    let applied = apply.applied_index();
    let before = raft::Storage::first_index(storage).unwrap_or(0);
    if let Err(e) = storage.maybe_compact(applied) {
        warn!(target: "catga_raft::owner", node_id, applied, error = %e, "raft log compaction failed");
        return;
    }
    let after = raft::Storage::first_index(storage).unwrap_or(before);
    if after > before {
        debug!(target: "catga_raft::owner", node_id, applied, first_index = after, "compacted raft log prefix");
    }
}
