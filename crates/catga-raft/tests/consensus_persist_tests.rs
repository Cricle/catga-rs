//! Persistence e2e for the raft runtime: propose on a runtime with
//! `with_data_dir`, restart a new runtime on the same directory, and verify
//! the raft log and hard state survived.
//!
//! Ports: 14xxx range (unused by other test files).

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_log_and_hard_state_survive_restart() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("node1");
    let payload_1 = b"persist-run-1".to_vec();
    let payload_2 = b"persist-run-2".to_vec();

    // Run 1: propose and wait for apply.
    let last_index_run1 = {
        let machine = RecordingMachine::new();
        let recorder = machine.clone();
        let runtime = CatgaRaftRuntimeBuilder::from_cli(14100, 0, 1)
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

        // The persisted raft state must be readable between runs.
        let storage = EngineStorage::open(&data_dir, 1, None).expect("reopen storage");
        let state = RaftStorage::initial_state(&storage).expect("initial state");
        assert!(state.initialized(), "persisted state must be initialized");
        assert_eq!(state.conf_state.voters, vec![1]);
        let hard_state = storage.hard_state();
        assert!(hard_state.term >= 1, "election must have bumped the term");
        assert_eq!(hard_state.vote, 1, "node voted for itself");
        assert!(hard_state.commit >= 1, "proposed entry must be committed");
        let log = read_log(&storage);
        assert!(
            log.iter().any(|e| e.data == payload_1),
            "payload must persist"
        );
        RaftStorage::last_index(&storage).expect("last_index")
    };

    // Run 2: new runtime on the same directory. The restored hard state
    // (term/vote/commit) comes from disk; the log continues instead of
    // restarting from index 1.
    {
        let machine = RecordingMachine::new();
        let recorder = machine.clone();
        let runtime = CatgaRaftRuntimeBuilder::from_cli(14200, 0, 1)
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
    let hard_state = storage.hard_state();
    assert!(
        hard_state.commit >= last_index_run1,
        "commit must not regress after restart (commit={}, run1 last={})",
        hard_state.commit,
        last_index_run1
    );
}
