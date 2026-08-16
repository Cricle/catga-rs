//! Tests for attributed `propose_and_wait`: the caller's own entry is
//! resolved via its unique context, never another entry's.

use std::sync::Arc;
use std::time::Duration;

use catga_core::{CatgaResult, ConsensusRuntime, ConsensusStateMachine};
use catga_raft::CatgaRaftRuntimeBuilder;
use parking_lot::Mutex;

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
    fn payloads(&self) -> Vec<Vec<u8>> {
        self.applied.lock().iter().map(|(_, d)| d.clone()).collect()
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

async fn eventually<F: FnMut() -> bool>(timeout: Duration, mut f: F) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if f() {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn propose_and_wait_resolves_own_entry() {
    let machine = RecordingMachine::new();
    let recorder = machine.clone();
    let runtime = CatgaRaftRuntimeBuilder::from_cli(15700, 0, 1)
        .expect("builder")
        .start(machine)
        .await
        .unwrap_or_else(|e| panic!("start: {e}"));

    let leader_known = eventually(Duration::from_secs(5), || {
        ConsensusRuntime::coordinator(&runtime).is_leader()
    })
    .await;
    assert!(leader_known, "single node must become leader");

    let index = runtime
        .propose_and_wait(b"attributed-entry".to_vec(), Duration::from_secs(5))
        .await
        .expect("propose_and_wait");
    assert!(index >= 1);

    // Attribution resolves at COMMIT on the owner loop; the apply worker
    // applies the entry asynchronously right after, so poll briefly for it.
    let applied = eventually(Duration::from_secs(5), || {
        recorder.payloads().contains(&b"attributed-entry".to_vec())
    })
    .await;
    assert!(applied, "the committed entry must reach the state machine");

    runtime.shutdown();
    Box::new(runtime).join().await.expect("join");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_propose_and_wait_all_attributed() {
    let machine = RecordingMachine::new();
    let recorder = machine.clone();
    let runtime = Arc::new(
        CatgaRaftRuntimeBuilder::from_cli(15800, 0, 1)
            .expect("builder")
            .start(machine)
            .await
            .unwrap_or_else(|e| panic!("start: {e}")),
    );

    let leader_known = eventually(Duration::from_secs(5), || {
        ConsensusRuntime::coordinator(&*runtime).is_leader()
    })
    .await;
    assert!(leader_known, "single node must become leader");

    let mut handles = Vec::new();
    for i in 0..16u64 {
        let rt = Arc::clone(&runtime);
        handles.push(tokio::spawn(async move {
            let payload = format!("concurrent-{i}");
            let idx = rt
                .propose_and_wait(payload.clone().into_bytes(), Duration::from_secs(10))
                .await
                .expect("propose_and_wait");
            (idx, payload)
        }));
    }

    let mut results = Vec::new();
    for h in handles {
        results.push(h.await.expect("task"));
    }

    // Every caller got a distinct index.
    let mut indexes: Vec<u64> = results.iter().map(|(i, _)| *i).collect();
    indexes.sort_unstable();
    indexes.dedup();
    assert_eq!(
        indexes.len(),
        16,
        "each proposer must resolve its own entry"
    );

    // Every payload was applied (asynchronously after commit-time
    // resolution; poll until the apply worker has caught up).
    let all_applied = eventually(Duration::from_secs(10), || {
        let applied = recorder.payloads();
        results
            .iter()
            .all(|(_, payload)| applied.contains(&payload.as_bytes().to_vec()))
    })
    .await;
    assert!(
        all_applied,
        "every committed entry must reach the state machine"
    );

    let rt = Arc::try_unwrap(runtime).unwrap_or_else(|_| panic!("runtime still shared"));
    rt.shutdown();
    Box::new(rt).join().await.expect("join");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn propose_and_wait_on_three_node_cluster() {
    const BASE_PORT: u16 = 15900;
    let machines: Vec<RecordingMachine> = (0..3).map(|_| RecordingMachine::new()).collect();
    let recorders: Vec<RecordingMachine> = machines.clone();

    let mut runtimes = Vec::new();
    for i in 0..3u64 {
        let rt = CatgaRaftRuntimeBuilder::from_cli(BASE_PORT, i, 3)
            .expect("builder")
            .start(machines[i as usize].clone())
            .await
            .unwrap_or_else(|e| panic!("start: {e}"));
        runtimes.push(rt);
    }

    // Wait for the cluster to elect a leader before proposing. Generous
    // window: these suites run their clusters concurrently on CI boxes.
    let elected = eventually(Duration::from_secs(30), || {
        runtimes
            .iter()
            .any(|rt| ConsensusRuntime::coordinator(rt).is_leader())
    })
    .await;
    assert!(elected, "cluster must elect a leader");

    // Propose from any node: followers redirect proposals to the leader, and
    // the attribution resolves when this node applies the committed entry.
    let index = runtimes[0]
        .propose_and_wait(b"cluster-attributed".to_vec(), Duration::from_secs(10))
        .await
        .expect("propose_and_wait from any node");
    assert!(index >= 1);

    // All replicas eventually apply it.
    for (i, recorder) in recorders.iter().enumerate() {
        let recorder = recorder.clone();
        let ok = eventually(Duration::from_secs(30), || {
            recorder
                .payloads()
                .contains(&b"cluster-attributed".to_vec())
        })
        .await;
        assert!(ok, "node {i} must apply the attributed entry");
    }

    for rt in runtimes {
        rt.shutdown();
        Box::new(rt).join().await.expect("join");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn propose_serializable_round_trips_bincode() {
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
    struct Cmd {
        op: String,
        n: u64,
    }

    let machine = RecordingMachine::new();
    let recorder = machine.clone();
    let runtime = CatgaRaftRuntimeBuilder::from_cli(16200, 0, 1)
        .expect("builder")
        .start(machine)
        .await
        .unwrap_or_else(|e| panic!("start: {e}"));

    let leader_known = eventually(Duration::from_secs(5), || {
        ConsensusRuntime::coordinator(&runtime).is_leader()
    })
    .await;
    assert!(leader_known, "single node must become leader");

    let cmd = Cmd {
        op: "put".into(),
        n: 42,
    };
    let index = runtime
        .propose_and_wait_serializable(&cmd, Duration::from_secs(5))
        .await
        .expect("propose_and_wait_serializable");
    assert!(index >= 1);

    // Resolution is commit-time; the apply worker applies asynchronously.
    let applied = eventually(Duration::from_secs(5), || {
        recorder.payloads().iter().any(|bytes| {
            bincode::serde::decode_from_slice::<Cmd, _>(bytes, bincode::config::standard())
                .ok()
                .map(|(v, _)| v == cmd)
                .unwrap_or(false)
        })
    })
    .await;
    assert!(
        applied,
        "the committed entry must be applied and decode back to the original command"
    );

    // Fire-and-forget variant works too.
    runtime
        .propose_serializable(&Cmd {
            op: "del".into(),
            n: 7,
        })
        .expect("propose_serializable");

    runtime.shutdown();
    Box::new(runtime).join().await.expect("join");
}
