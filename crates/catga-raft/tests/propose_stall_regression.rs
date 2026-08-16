//! Regression for the production halt: after ~`max_inflight` (default 1024)
//! cumulative proposals, the pipeline wedged forever.
//!
//! Root cause: `PipelineManager::flush` incremented `inflight` per flushed
//! proposal, but the owner loop — the only consumer of batches — never called
//! `batch_completed`, so the counter only grew. Once it reached
//! `max_inflight`, every `propose` returned `Timeout` even though the node
//! was otherwise healthy. (Compounding: a full batch channel used to drop
//! whole batches *after* the increment, leaking even faster.)
//!
//! This is the end-to-end repro: a single node, 1500 sequential proposes
//! through `ConsensusRuntime::propose` — comfortably above the 1024 default
//! window — all of which must succeed and eventually be applied.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use catga_core::{CatgaResult, ConsensusRuntime, ConsensusStateMachine};
use catga_raft::CatgaRaftRuntimeBuilder;

/// Total proposals: comfortably above the default `max_inflight` of 1024.
const TOTAL: u64 = 1500;

/// State machine that only counts applied entries.
#[derive(Clone, Default)]
struct CountingMachine {
    applied: Arc<AtomicU64>,
}

impl ConsensusStateMachine for CountingMachine {
    fn apply(&mut self, _index: u64, _data: &[u8]) -> CatgaResult<()> {
        self.applied.fetch_add(1, Ordering::Relaxed);
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
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if condition() {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The production repro: 1500 sequential proposes through one node must all
/// succeed and every entry must be applied.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_1500_sequential_proposes_all_apply() {
    let machine = CountingMachine::default();
    let applied = Arc::clone(&machine.applied);

    let runtime = CatgaRaftRuntimeBuilder::from_cli(15400, 0, 1)
        .expect("builder")
        .start(machine)
        .await
        .expect("start single node");

    // A single voter wins its own election within a few ticks.
    let leader_known = eventually(Duration::from_secs(10), || {
        ConsensusRuntime::coordinator(&runtime).is_leader()
    })
    .await;
    assert!(leader_known, "single node must become leader");

    // Before the fix, proposes around #1024 started failing forever: the
    // in-flight counter only ever grew, so even single later proposes were
    // rejected while other nodes worked. With the fix, the owner loop
    // acknowledges every drained batch, so any *transient* backpressure from
    // a burst clears as soon as batches are consumed — which is exactly what
    // a retrying client observes here. Every entry must eventually be
    // accepted.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    for i in 0..TOTAL {
        let payload = format!("entry-{i}").into_bytes();
        loop {
            match runtime.propose(payload.clone()).await {
                Ok(()) => break,
                Err(e) if e.to_string().contains("backpressure") => {
                    // Transient overload: back off like a real client. Under
                    // the old bug this retried forever; now capacity returns.
                    assert!(
                        std::time::Instant::now() < deadline,
                        "propose #{i}/{TOTAL} still backpressured after 30s \
                         (in-flight leak regression); inflight={}, pending={}",
                        runtime.pipeline().inflight_count(),
                        runtime.pipeline().pending_count()
                    );
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err(e) => {
                    panic!("propose #{i}/{TOTAL} failed: {e} (in-flight leak regression)")
                }
            }
        }
    }

    // Fire-and-forget acceptance is not enough: everything must actually be
    // applied by the state machine.
    let all_applied = eventually(Duration::from_secs(30), || {
        applied.load(Ordering::Relaxed) >= TOTAL
    })
    .await;
    assert!(
        all_applied,
        "only {}/{} entries applied within 30s",
        applied.load(Ordering::Relaxed),
        TOTAL
    );

    let idx = runtime.applied_index().await.expect("applied_index");
    assert!(
        idx >= TOTAL,
        "applied_index {idx} must cover all {TOTAL} entries"
    );

    // The pipeline must be fully drained: nothing lost, nothing leaked.
    assert_eq!(runtime.pipeline().pending_count(), 0);
    assert_eq!(runtime.pipeline().inflight_count(), 0);

    runtime.shutdown();
    Box::new(runtime).join().await.expect("join");
}
