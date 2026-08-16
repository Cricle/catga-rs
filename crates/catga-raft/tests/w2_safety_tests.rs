//! W2 safety tests: deterministic behavior checks for wave-2 fixes.
//!
//! Covers the owner-loop memory and perf changes that are observable through
//! the public runtime API:
//!
//! - **Bounded, expiring pending reads** (owner.rs). Without a quorum raft
//!   never emits `ReadState`s, so the owner loop must neither grow the queue
//!   without bound nor hold reads forever:
//!   - reads beyond the pending-read cap are rejected with `Timeout`
//!     immediately;
//!   - a queued read expires with `Timeout` after the TTL instead of
//!     hanging on its oneshot forever.
//! - **Empty-ready persist skip** (owner.rs). Readies with nothing durable
//!   (read-state-only, heartbeat-only) are acknowledged inline instead of a
//!   persist-worker round-trip; writes and linearizable reads must keep
//!   working on both a single node and a real 3-node cluster, where empty
//!   and real persists interleave and the ordering rule matters.
//!
//! Ports: 16100 (single node), 16500-16700 (healthy 3-node cluster),
//! 16300/16400 (no-quorum nodes).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use catga_core::{CatgaResult, ConsensusRuntime, ConsensusStateMachine};
use catga_raft::{CatgaRaftError, CatgaRaftRuntimeBuilder};
use parking_lot::Mutex;

/// The owner-loop cap on unanswered ReadIndex requests; mirrored here so the
/// test fails loudly if the constant moves.
const MAX_PENDING_READS: usize = 4096;

/// Reads queued beyond the cap must be rejected well inside this window.
/// Must stay below the owner's 10s pending-read TTL so the queued (not
/// rejected) reads cannot expire into the counters meanwhile.
const CAP_OBSERVATION_WINDOW: Duration = Duration::from_secs(6);

/// State machine that records every applied entry.
#[derive(Clone)]
struct RecordingMachine {
    applied: Arc<Mutex<Vec<(u64, Vec<u8>)>>>,
}

impl RecordingMachine {
    fn new() -> Self {
        Self {
            applied: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn applied_payloads(&self) -> Vec<Vec<u8>> {
        self.applied
            .lock()
            .iter()
            .map(|(_, data)| data.clone())
            .collect()
    }

    fn applied_count(&self) -> usize {
        self.applied.lock().len()
    }
}

impl ConsensusStateMachine for RecordingMachine {
    fn apply(&mut self, index: u64, data: &[u8]) -> CatgaResult<()> {
        self.applied.lock().push((index, data.to_vec()));
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        Ok(Vec::new())
    }

    fn restore(&mut self, _bytes: &[u8]) -> CatgaResult<()> {
        Ok(())
    }
}

/// Polls `condition` until it holds or the deadline passes.
async fn eventually<F>(timeout: Duration, mut condition: F) -> bool
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        if condition() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ============================================================================
// Bounded, expiring pending reads (item 1)
// ============================================================================

/// Without a quorum no leader is ever elected, so raft drops every
/// `MsgReadIndex` and no queued read can be answered. Once the owner loop's
/// pending-read queue reaches its cap, further reads must be rejected
/// promptly with `Timeout` instead of piling up forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_beyond_pending_cap_reject_with_timeout() {
    const EXTRA: usize = 8;
    const TOTAL: usize = MAX_PENDING_READS + EXTRA;

    // One voter of a 3-voter group, peers never started: no quorum, no
    // leader, reads cannot resolve.
    let runtime = Arc::new(
        CatgaRaftRuntimeBuilder::from_cli(16300, 0, 3)
            .expect("builder")
            .start(RecordingMachine::new())
            .await
            .expect("start no-quorum node"),
    );

    let timeouts = Arc::new(AtomicUsize::new(0));
    let successes = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(TOTAL);
    for _ in 0..TOTAL {
        let rt = Arc::clone(&runtime);
        let timeouts = Arc::clone(&timeouts);
        let successes = Arc::clone(&successes);
        handles.push(tokio::spawn(async move {
            match rt.read_index().await {
                Ok(_) => {
                    successes.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    timeouts.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    // Exactly `EXTRA` reads exceed the cap. The channel is FIFO and every
    // request is eventually pulled, so exactly `EXTRA` rejections must occur
    // (all promptly), while the queued majority keeps waiting on the TTL.
    let rejected = eventually(CAP_OBSERVATION_WINDOW, || {
        timeouts.load(Ordering::Relaxed) >= EXTRA
    })
    .await;
    assert!(
        rejected,
        "reads beyond the cap must be rejected with Timeout; observed {} of {EXTRA}",
        timeouts.load(Ordering::Relaxed)
    );
    assert_eq!(
        successes.load(Ordering::Relaxed),
        0,
        "no read can succeed without a quorum"
    );

    // Shutdown drops the owner loop's pending reads, resolving the rest of
    // the oneshots; every spawned reader must then finish instead of leaking.
    runtime.shutdown_and_join().await.expect("shutdown");
    for handle in handles {
        let _ = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("reader task must resolve after shutdown");
    }
}

/// A queued read on a quorum-less node must not hang forever: the owner
/// expires it after the pending-read TTL and resolves its oneshot with
/// `Timeout`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_read_expires_without_quorum() {
    let runtime = CatgaRaftRuntimeBuilder::from_cli(16400, 0, 3)
        .expect("builder")
        .start(RecordingMachine::new())
        .await
        .expect("start no-quorum node");

    let started = Instant::now();
    // Generous outer bound: the TTL is 10s, expiry lands on the next 100ms
    // tick, so ~10.2s is the expected resolution time.
    let result = tokio::time::timeout(Duration::from_secs(25), runtime.read_index()).await;
    let elapsed = started.elapsed();

    match result {
        Ok(Err(CatgaRaftError::Timeout)) => {
            // The TTL path (not the cap path, which rejects immediately):
            // the read must have sat in the queue for roughly the TTL.
            assert!(
                elapsed >= Duration::from_secs(9),
                "read failed too early to be the TTL expiry: {elapsed:?}"
            );
        }
        Ok(other) => panic!("expected Err(Timeout) for the expired read, got {other:?}"),
        Err(_) => panic!("read still unresolved after 25s; pending-read expiry is broken"),
    }

    runtime.shutdown();
    Box::new(runtime).join().await.expect("join");
}

// ============================================================================
// Empty-ready persist skip keeps consensus healthy (item 5)
// ============================================================================

/// Single node: read-state-only readies carry nothing durable and are acked
/// inline. Proposals and repeated ReadIndex/read_barrier calls must keep
/// working, and every read must cover the proposed entries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_reads_and_writes_with_inline_persist_ack() {
    let machine = RecordingMachine::new();
    let recorder = machine.clone();

    let runtime = CatgaRaftRuntimeBuilder::from_cli(16100, 0, 1)
        .expect("builder")
        .start(machine)
        .await
        .expect("start single node");

    let leader_known = eventually(Duration::from_secs(5), || {
        ConsensusRuntime::coordinator(&runtime).is_leader()
    })
    .await;
    assert!(leader_known, "single node must become leader");

    const ENTRIES: u64 = 50;
    for i in 0..ENTRIES {
        runtime
            .propose(format!("entry-{i}").into_bytes())
            .await
            .expect("propose");
    }

    let applied = eventually(Duration::from_secs(10), || {
        recorder.applied_count() >= ENTRIES as usize
    })
    .await;
    assert!(applied, "all proposed entries must be applied");

    // Repeated reads resolve; each must cover the committed prefix.
    for _ in 0..10 {
        let idx = tokio::time::timeout(Duration::from_secs(5), runtime.read_index())
            .await
            .expect("read index timed out")
            .expect("read index failed");
        assert!(
            idx >= ENTRIES,
            "read index {idx} must cover {ENTRIES} entries"
        );
    }

    let barrier = runtime
        .read_barrier(Duration::from_secs(5))
        .await
        .expect("read barrier");
    assert!(
        barrier >= ENTRIES,
        "barrier index {barrier} below {ENTRIES}"
    );

    runtime.shutdown();
    Box::new(runtime).join().await.expect("join");
}

/// Three nodes: the leader interleaves real persists (entries, hard state)
/// with empty ones (heartbeats, read states); followers ack persisted
/// messages through the worker. The skip path must not break replication or
/// ReadIndex anywhere in that mix.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_node_replication_and_reads_with_empty_ready_skip() {
    // Nodes bind 16500/16600/16700; must not overlap the other tests' ports.
    const BASE_PORT: u16 = 16500;
    let machines: Vec<RecordingMachine> = (0..3).map(|_| RecordingMachine::new()).collect();
    let recorders: Vec<RecordingMachine> = machines.clone();

    let mut runtimes = Vec::new();
    for i in 0..3u64 {
        let runtime = CatgaRaftRuntimeBuilder::from_cli(BASE_PORT, i, 3)
            .expect("builder")
            .start(machines[i as usize].clone())
            .await
            .expect("start cluster node");
        runtimes.push(runtime);
    }

    let mut leader_idx = None;
    let elected = eventually(Duration::from_secs(15), || {
        for (i, rt) in runtimes.iter().enumerate() {
            if ConsensusRuntime::coordinator(rt).is_leader() {
                leader_idx = Some(i);
                return true;
            }
        }
        false
    })
    .await;
    assert!(elected, "cluster must elect a leader");
    let leader_idx = leader_idx.unwrap();

    const ENTRIES: u64 = 20;
    for i in 0..ENTRIES {
        runtimes[leader_idx]
            .propose(format!("w2-entry-{i}").into_bytes())
            .await
            .expect("leader propose");
    }

    for (i, recorder) in recorders.iter().enumerate() {
        let recorder = recorder.clone();
        let applied = eventually(Duration::from_secs(10), || {
            recorder
                .applied_payloads()
                .contains(&b"w2-entry-19".to_vec())
        })
        .await;
        assert!(applied, "node {i} must apply the replicated entries");
    }

    // ReadIndex resolves on the leader and on every follower.
    for (i, rt) in runtimes.iter().enumerate() {
        let idx = tokio::time::timeout(Duration::from_secs(5), rt.read_index())
            .await
            .unwrap_or_else(|_| panic!("node {i} read index timed out"))
            .unwrap_or_else(|e| panic!("node {i} read index failed: {e}"));
        assert!(
            idx >= ENTRIES,
            "node {i} read index {idx} must cover {ENTRIES} entries"
        );
    }

    for rt in runtimes {
        rt.shutdown();
        Box::new(rt).join().await.expect("join");
    }
}
