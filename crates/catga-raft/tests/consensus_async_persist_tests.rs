//! End-to-end tests for the asynchronous raft persist path.
//!
//! The owner loop hands durable work (hard state, entries, snapshot) to a
//! dedicated persist worker and advances raft with `advance_append_async`;
//! these tests exercise that path for both storage variants:
//!
//! - single node on `with_data_dir`: propose -> apply -> clean shutdown ->
//!   reopen storage, everything durable;
//! - 3-node in-memory cluster: election, replication and ReadIndex;
//! - 3-node persistent cluster: 200 sequential proposes through the leader,
//!   all applied on every node, clean shutdown, nothing dropped.
//!
//! Ports: 14700+ (consensus_persist_tests uses 14100/14200, consensus_e2e
//! uses 13xxx).

use std::sync::Arc;
use std::time::Duration;

use catga_core::{CatgaResult, ConsensusRuntime, ConsensusStateMachine};
use catga_raft::CatgaRaftRuntimeBuilder;
use catga_raft::storage::EngineStorage;
use parking_lot::Mutex;
use raft::Storage as RaftStorage;
use raft::storage::GetEntriesContext;
use tempfile::tempdir;

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

    fn applied_entries(&self) -> Vec<(u64, Vec<u8>)> {
        self.applied.lock().clone()
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

fn read_log(storage: &EngineStorage) -> Vec<raft::prelude::Entry> {
    let last = RaftStorage::last_index(storage).expect("last_index");
    if last == 0 {
        return Vec::new();
    }
    RaftStorage::entries(storage, 1, last + 1, None, GetEntriesContext::empty(false))
        .expect("entries")
}

/// (a) Single node with a data dir: what was applied must be durable after a
/// clean shutdown, and a second run on the same directory must continue the
/// log instead of starting over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_async_persist_survives_restart() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("node1");
    let payload_1 = b"async-persist-run-1".to_vec();
    let payload_2 = b"async-persist-run-2".to_vec();

    // Run 1: propose, wait for apply, shut down cleanly.
    let last_index_run1 = {
        let machine = RecordingMachine::new();
        let recorder = machine.clone();
        let runtime = CatgaRaftRuntimeBuilder::from_cli(15000, 0, 1)
            .expect("builder")
            .with_data_dir(&data_dir)
            .start(machine)
            .await
            .expect("start run 1");

        let leader = eventually(Duration::from_secs(5), || {
            ConsensusRuntime::coordinator(&runtime).is_leader()
        })
        .await;
        assert!(leader, "single node must become leader");

        runtime.propose(payload_1.clone()).await.expect("propose");
        let applied = eventually(Duration::from_secs(5), || {
            recorder.applied_payloads().contains(&payload_1)
        })
        .await;
        assert!(applied, "proposed entry must reach the state machine");

        runtime.shutdown();
        Box::new(runtime).join().await.expect("join run 1");

        // Reopen the storage: the async persist worker must have made the
        // entry and the hard state durable before run() returned.
        let storage = EngineStorage::open(&data_dir, 1, None).expect("reopen storage");
        let state = RaftStorage::initial_state(&storage).expect("initial state");
        assert!(state.initialized(), "persisted state must be initialized");
        assert_eq!(state.conf_state.voters, vec![1]);
        let hard_state = storage.hard_state();
        assert!(hard_state.term >= 1, "election must have bumped the term");
        assert_eq!(hard_state.vote, 1, "node voted for itself");
        assert!(
            hard_state.commit >= 1,
            "applied entry must be committed in the persisted hard state"
        );
        let log = read_log(&storage);
        assert!(
            log.iter().any(|e| e.data == payload_1),
            "applied payload must be durable after clean shutdown"
        );
        RaftStorage::last_index(&storage).expect("last_index")
    };

    // Run 2: a fresh runtime on the same directory restores the persisted
    // state and continues the log.
    {
        let machine = RecordingMachine::new();
        let recorder = machine.clone();
        let runtime = CatgaRaftRuntimeBuilder::from_cli(15001, 0, 1)
            .expect("builder")
            .with_data_dir(&data_dir)
            .start(machine)
            .await
            .expect("start run 2 on the same data dir");

        let leader = eventually(Duration::from_secs(5), || {
            ConsensusRuntime::coordinator(&runtime).is_leader()
        })
        .await;
        assert!(leader, "restarted single node must become leader");

        runtime
            .propose(payload_2.clone())
            .await
            .expect("propose after restart");
        let applied = eventually(Duration::from_secs(5), || {
            recorder.applied_payloads().contains(&payload_2)
        })
        .await;
        assert!(applied, "entry proposed after restart must be applied");

        runtime.shutdown();
        Box::new(runtime).join().await.expect("join run 2");
    }

    let storage = EngineStorage::open(&data_dir, 1, None).expect("reopen storage after run 2");
    let last_index_run2 = RaftStorage::last_index(&storage).expect("last_index");
    assert!(
        last_index_run2 > last_index_run1,
        "log must continue after restart (run1 last={last_index_run1}, run2 last={last_index_run2})"
    );
    let log = read_log(&storage);
    assert!(
        log.iter().any(|e| e.data == payload_1),
        "pre-restart entry must survive the second run"
    );
    assert!(log.iter().any(|e| e.data == payload_2));
}

/// (b) 3-node in-memory cluster over the async persist path: elects a leader,
/// replicates a write to every node, and answers ReadIndex everywhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_node_memory_cluster_elects_replicates_and_reads() {
    const BASE_PORT: u16 = 14700;
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

    let payload = b"async-persist-replicated-write".to_vec();
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

/// (c) 3 nodes with data dirs: 200 sequential proposes through the leader are
/// all applied on every node, shutdown/join is clean, and nothing was dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_node_persistent_cluster_applies_200_sequential_proposes() {
    const BASE_PORT: u16 = 15100;
    const COUNT: usize = 200;

    let dir = tempdir().unwrap();
    let machines: Vec<RecordingMachine> = (0..3).map(|_| RecordingMachine::new()).collect();
    let recorders: Vec<RecordingMachine> = machines.clone();

    let mut runtimes = Vec::new();
    for i in 0..3u64 {
        let data_dir = dir.path().join(format!("node-{}", i + 1));
        let runtime = CatgaRaftRuntimeBuilder::from_cli(BASE_PORT, i, 3)
            .expect("builder")
            .with_data_dir(&data_dir)
            .start(machines[i as usize].clone())
            .await
            .expect("start cluster node");
        runtimes.push(runtime);
    }

    let mut leader_idx = {
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

    // Propose sequentially through the leader. propose() is fire-and-forget;
    // a transient loss of leadership re-discovers the leader and retries.
    for i in 0..COUNT {
        let payload = format!("seq-entry-{i:04}").into_bytes();
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            match runtimes[leader_idx].propose(payload.clone()).await {
                Ok(()) => break,
                Err(e) => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "propose {i} was never accepted: {e}"
                    );
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    for (j, rt) in runtimes.iter().enumerate() {
                        if ConsensusRuntime::coordinator(rt).is_leader() {
                            leader_idx = j;
                        }
                    }
                }
            }
        }
    }

    // Every node applies every entry exactly once: no drops, no duplicates.
    let expected: Vec<Vec<u8>> = (0..COUNT)
        .map(|i| format!("seq-entry-{i:04}").into_bytes())
        .collect();
    for (i, recorder) in recorders.iter().enumerate() {
        let recorder = recorder.clone();
        let all_applied = eventually(Duration::from_secs(60), || {
            let payloads = recorder.applied_payloads();
            expected.iter().all(|p| payloads.contains(p))
        })
        .await;
        let applied = recorder.applied_entries();
        assert!(
            all_applied,
            "node {i} must apply all {COUNT} entries; applied {} so far",
            applied.len()
        );
        assert_eq!(
            applied.len(),
            COUNT,
            "node {i} must apply exactly the {COUNT} proposed entries (no extras, no duplicates)"
        );
        let mut indices: Vec<u64> = applied.iter().map(|(idx, _)| *idx).collect();
        indices.sort_unstable();
        indices.dedup();
        assert_eq!(
            indices.len(),
            COUNT,
            "node {i} applied duplicate or clashing indexes"
        );
    }

    // Clean shutdown: join must complete (the owner drains its persist worker).
    for rt in runtimes {
        rt.shutdown();
        Box::new(rt).join().await.expect("join");
    }

    // Durability of the leader's log after the clean shutdown: reopen the
    // storage of the node we proposed through most and find every payload.
    let leader_dir = dir.path().join(format!("node-{}", leader_idx + 1));
    let storage = EngineStorage::open(&leader_dir, (leader_idx + 1) as u64, None)
        .expect("reopen leader storage");
    let log = read_log(&storage);
    for payload in &expected {
        assert!(
            log.iter().any(|e| &e.data == payload),
            "payload {:?} must be durable after clean shutdown",
            String::from_utf8_lossy(payload)
        );
    }
    let hard_state = storage.hard_state();
    assert!(
        hard_state.commit as usize >= COUNT,
        "persisted commit ({}) must cover all {} entries",
        hard_state.commit,
        COUNT
    );
}
