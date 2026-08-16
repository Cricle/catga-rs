//! End-to-end consensus tests for the wired raft runtime.
//!
//! Covers the full loop: pipeline propose -> raft log -> replication ->
//! apply thread -> state machine, over the real gRPC transport.

use std::sync::Arc;
use std::time::Duration;

use catga_core::{CatgaResult, ConsensusRuntime, ConsensusStateMachine};
use catga_raft::CatgaRaftRuntimeBuilder;
use parking_lot::Mutex;

type AppliedLog = Vec<(u64, Vec<u8>)>;

/// State machine that records every applied entry.
#[derive(Clone)]
struct RecordingMachine {
    applied: Arc<Mutex<AppliedLog>>,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_proposes_and_applies() {
    let machine = RecordingMachine::new();
    let recorder = machine.clone();

    let runtime = CatgaRaftRuntimeBuilder::from_cli(13100, 0, 1)
        .expect("builder")
        .start(machine)
        .await
        .expect("start single node");

    // A single voter wins its own election within a few ticks.
    let leader_known = eventually(Duration::from_secs(5), || {
        ConsensusRuntime::coordinator(&runtime).is_leader()
    })
    .await;
    assert!(leader_known, "single node must become leader");

    runtime
        .propose(b"hello-consensus".to_vec())
        .await
        .expect("single-node propose must be accepted");

    let applied = eventually(Duration::from_secs(5), || {
        recorder
            .applied_payloads()
            .contains(&b"hello-consensus".to_vec())
    })
    .await;
    assert!(applied, "proposed entry must reach the state machine");

    runtime.shutdown();
    Box::new(runtime).join().await.expect("join");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_node_cluster_elects_leader_and_replicates() {
    const BASE_PORT: u16 = 13200;
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

    // Wait until at least one node reports a known leader.
    let leader_idx = {
        let mut found = None;
        let ok = eventually(Duration::from_secs(15), || {
            for (i, rt) in runtimes.iter().enumerate() {
                if ConsensusRuntime::coordinator(rt).is_leader() {
                    found = Some(i);
                    return true;
                }
            }
            false
        })
        .await;
        assert!(ok, "cluster must elect a leader");
        found.unwrap()
    };

    let payload = b"replicated-write".to_vec();
    runtimes[leader_idx]
        .propose(payload.clone())
        .await
        .expect("leader propose must be accepted");

    for (i, recorder) in recorders.iter().enumerate() {
        let recorder = recorder.clone();
        let applied = eventually(Duration::from_secs(10), || {
            recorder.applied_payloads().contains(&payload)
        })
        .await;
        assert!(applied, "node {i} must apply the replicated entry");
    }

    for rt in runtimes {
        rt.shutdown();
        Box::new(rt).join().await.expect("join");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_read_index_resolves() {
    let machine = RecordingMachine::new();
    let runtime = CatgaRaftRuntimeBuilder::from_cli(13500, 0, 1)
        .expect("builder")
        .start(machine)
        .await
        .expect("start");

    let leader_known = eventually(Duration::from_secs(5), || {
        ConsensusRuntime::coordinator(&runtime).is_leader()
    })
    .await;
    assert!(leader_known, "single node must become leader");

    runtime
        .propose(b"read-index-entry".to_vec())
        .await
        .expect("propose");

    let idx = runtime.read_index().await.expect("read index");
    assert!(
        idx >= 1,
        "read index must cover the proposed entry, got {idx}"
    );

    runtime.shutdown();
    Box::new(runtime).join().await.expect("join");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_node_read_index_on_leader_and_follower() {
    const BASE_PORT: u16 = 13600;
    let machines: Vec<RecordingMachine> = (0..3).map(|_| RecordingMachine::new()).collect();

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
    let ok = eventually(Duration::from_secs(15), || {
        for (i, rt) in runtimes.iter().enumerate() {
            if ConsensusRuntime::coordinator(rt).is_leader() {
                leader_idx = Some(i);
                return true;
            }
        }
        false
    })
    .await;
    assert!(ok, "cluster must elect a leader");
    let leader_idx = leader_idx.unwrap();

    runtimes[leader_idx]
        .propose(b"before-read".to_vec())
        .await
        .expect("leader propose");

    // ReadIndex must resolve on the leader and on every follower.
    for (i, rt) in runtimes.iter().enumerate() {
        let idx = tokio::time::timeout(Duration::from_secs(5), rt.read_index())
            .await
            .unwrap_or_else(|_| panic!("node {i} read index timed out"))
            .unwrap_or_else(|e| panic!("node {i} read index failed: {e}"));
        assert!(
            idx >= 1,
            "node {i} read index must cover the committed entry"
        );
    }

    for rt in runtimes {
        rt.shutdown();
        Box::new(rt).join().await.expect("join");
    }
}
