//! Safe raft log compaction tests.
//!
//! Compaction is conservative: entries are only discarded strictly below
//! `applied_index - margin` (margin defaults to 10k in production; these
//! tests lower it process-wide via `CatgaStorage::set_compaction_margin`,
//! which is safe because every test binary is its own process).
//!
//! Coverage:
//! - single node on `with_data_dir`: propose 150 entries, the owner loop's
//!   compaction cycle moves `first_index` to `applied - margin`, the log
//!   tail and hard/conf state stay intact, reads below `first_index` report
//!   `StorageError::Compacted`, and a reopen/restart stays consistent;
//! - 3-node in-memory cluster: replication keeps working while every node
//!   compacts; entries proposed after compaction still reach all followers
//!   (catch-up within the margin through normal replication);
//! - direct `CatgaStorage::maybe_compact` boundary checks for both variants.
//!
//! Ports: 16xxx range (unused by other test files).

use std::sync::Arc;
use std::time::Duration;

use catga_core::{CatgaResult, ConsensusRuntime, ConsensusStateMachine};
use catga_raft::storage::{CatgaStorage, EngineStorage};
use catga_raft::CatgaRaftRuntimeBuilder;
use parking_lot::Mutex;
use raft::prelude::Entry;
use raft::storage::GetEntriesContext;
use raft::{Error as RaftError, StorageError, Storage as RaftStorage};
use tempfile::tempdir;

/// Small margin so compaction becomes observable with ~150 entries. All
/// tests in this binary use the same value: they may run in parallel and
/// share the process-wide knob.
const MARGIN: u64 = 100;
/// Fast compaction cadence so the tests do not have to wait a minute.
const CHECK_INTERVAL: Duration = Duration::from_millis(100);

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
        self.applied.lock().iter().map(|(_, data)| data.clone()).collect()
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

fn configure_test_compaction() {
    CatgaStorage::set_compaction_margin(MARGIN);
    CatgaStorage::set_compaction_interval(CHECK_INTERVAL);
}

fn assert_is_compacted(err: &RaftError, what: &str) {
    assert!(
        matches!(err, RaftError::Store(StorageError::Compacted)),
        "{what} below the compaction boundary must report Compacted, got {err:?}"
    );
}

/// Proposes `count` entries (`{prefix}-{i:04}` payloads) through the current
/// leader, retrying with leader re-discovery on transient failures, then
/// waits until every node has applied at least `expected_total` entries.
/// Waiting per batch keeps every follower well inside the compaction margin,
/// which is exactly the catch-up window the margin guarantees.
async fn propose_batch(
    runtimes: &[catga_raft::CatgaRaftRuntime<RecordingMachine>],
    recorders: &[RecordingMachine],
    leader_idx: &mut usize,
    prefix: &str,
    count: u64,
    expected_total: u64,
) {
    for i in 0..count {
        let payload = format!("{prefix}-{i:04}").into_bytes();
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            match runtimes[*leader_idx].propose(payload.clone()).await {
                Ok(()) => break,
                Err(e) => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "propose {prefix}-{i} was never accepted: {e}"
                    );
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    for (j, rt) in runtimes.iter().enumerate() {
                        if ConsensusRuntime::coordinator(rt).is_leader() {
                            *leader_idx = j;
                        }
                    }
                }
            }
        }
    }
    let total = expected_total as usize;
    let all_applied = eventually(Duration::from_secs(30), || {
        recorders.iter().all(|r| r.applied_entries().len() >= total)
    })
    .await;
    assert!(
        all_applied,
        "all nodes must have applied at least the first {expected_total} entries"
    );
}

/// Single node on persistent storage: the owner loop compacts exactly up to
/// `applied - margin`, keeps the tail and hard/conf state intact, reports
/// `Compacted` for the discarded prefix, and survives both a storage reopen
/// and a full raft restart on the compacted directory.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_engine_compaction_moves_first_index_and_survives_restart() {
    configure_test_compaction();
    const COUNT: u64 = 150;
    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("node1");

    // Run 1: propose COUNT entries, wait for apply, give the low-frequency
    // compaction cycle time to run, then shut down cleanly.
    {
        let machine = RecordingMachine::new();
        let recorder = machine.clone();
        let runtime = CatgaRaftRuntimeBuilder::from_cli(16100, 0, 1)
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

        for i in 0..COUNT {
            let payload = format!("comp-entry-{i:04}").into_bytes();
            let deadline = std::time::Instant::now() + Duration::from_secs(15);
            loop {
                match runtime.propose(payload.clone()).await {
                    Ok(()) => break,
                    Err(e) => {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "propose {i} was never accepted: {e}"
                        );
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }
            }
        }

        let all_applied = eventually(Duration::from_secs(30), || {
            recorder.applied_entries().len() == COUNT as usize
        })
        .await;
        assert!(
            all_applied,
            "all {COUNT} entries must be applied; got {}",
            recorder.applied_entries().len()
        );
        let applied = ConsensusRuntime::applied_index(&runtime)
            .await
            .expect("applied index");
        // raft-rs `become_leader` appends one empty no-op entry (index 1), so
        // the COUNT payloads occupy indexes 2..=COUNT+1.
        assert_eq!(
            applied,
            COUNT + 1,
            "log must be the leader no-op plus the {COUNT} payloads"
        );

        // Let several compaction cycles fire (cadence is CHECK_INTERVAL).
        tokio::time::sleep(CHECK_INTERVAL * 10).await;

        runtime.shutdown();
        Box::new(runtime).join().await.expect("join run 1");
    }

    // Reopen the storage: compaction must be durable and exact. Payload index
    // mapping: entry i >= 2 carries payload i - 2 (index 1 is the no-op).
    let applied = COUNT + 1;
    let expected_first = applied - MARGIN;
    let payload_at = |index: u64| format!("comp-entry-{:04}", index - 2).into_bytes();
    {
        let storage = EngineStorage::open(&data_dir, 1, None).expect("reopen storage");
        let first = RaftStorage::first_index(&storage).expect("first_index");
        let last = RaftStorage::last_index(&storage).expect("last_index");
        assert_eq!(
            first, expected_first,
            "compaction boundary must be applied - margin"
        );
        assert_eq!(last, applied, "compaction must not touch the log tail");

        // The discarded prefix reports the raft-standard error.
        let err = RaftStorage::entries(&storage, 1, expected_first, None, GetEntriesContext::empty(false))
            .expect_err("entries below the boundary must fail");
        assert_is_compacted(&err, "entries");
        let err = RaftStorage::term(&storage, expected_first - 1).expect_err("term below the boundary must fail");
        assert_is_compacted(&err, "term");

        // The kept window is fully readable and intact.
        let kept = RaftStorage::entries(
            &storage,
            expected_first,
            applied + 1,
            None,
            GetEntriesContext::empty(false),
        )
        .expect("entries in the kept window");
        assert_eq!(kept.len(), MARGIN as usize + 1);
        assert_eq!(kept.first().unwrap().index, expected_first);
        assert_eq!(kept.first().unwrap().data, payload_at(expected_first).as_slice());
        assert_eq!(kept.last().unwrap().data, payload_at(applied).as_slice());

        // HardState and conf state are unaffected by compaction.
        let state = RaftStorage::initial_state(&storage).expect("initial state");
        assert!(state.initialized(), "persisted state must stay initialized");
        assert_eq!(state.conf_state.voters, vec![1]);
        let hard_state = storage.hard_state();
        assert!(hard_state.term >= 1, "term must survive compaction");
        assert_eq!(hard_state.vote, 1, "vote must survive compaction");
        assert!(
            hard_state.commit >= applied,
            "commit must survive compaction (commit={})",
            hard_state.commit
        );
    }

    // Run 2: a fresh raft on the compacted directory must come up, replay
    // its remaining window, and keep accepting writes.
    {
        let machine = RecordingMachine::new();
        let recorder = machine.clone();
        let runtime = CatgaRaftRuntimeBuilder::from_cli(16200, 0, 1)
            .expect("builder")
            .with_data_dir(&data_dir)
            .start(machine)
            .await
            .expect("start run 2 on the compacted data dir");

        let leader = eventually(Duration::from_secs(5), || {
            ConsensusRuntime::coordinator(&runtime).is_leader()
        })
        .await;
        assert!(leader, "restarted single node must become leader");

        runtime
            .propose(b"comp-post-restart".to_vec())
            .await
            .expect("propose after restart");
        let applied = eventually(Duration::from_secs(10), || {
            recorder.applied_payloads().contains(&b"comp-post-restart".to_vec())
        })
        .await;
        assert!(applied, "entry proposed after restart must be applied");

        runtime.shutdown();
        Box::new(runtime).join().await.expect("join run 2");
    }

    // Final reopen: the compaction boundary never regresses and the log
    // continues past it.
    {
        let storage = EngineStorage::open(&data_dir, 1, None).expect("reopen after run 2");
        let first = RaftStorage::first_index(&storage).expect("first_index");
        let last = RaftStorage::last_index(&storage).expect("last_index");
        assert!(
            first >= expected_first,
            "compaction boundary must not regress (first={first}, expected >= {expected_first})"
        );
        assert!(
            last >= applied + 1,
            "post-restart entries must be durable (last={last})"
        );
        assert!(first <= last, "first/last must stay ordered");
        let kept = RaftStorage::entries(&storage, first, last + 1, None, GetEntriesContext::empty(false))
            .expect("kept window readable after restart");
        assert_eq!(kept.len() as u64, last - first + 1);
        assert!(kept.iter().any(|e| e.data == b"comp-post-restart".to_vec()));
        if first > 1 {
            let err = RaftStorage::entries(&storage, 1, first, None, GetEntriesContext::empty(false))
                .expect_err("prefix below the boundary must stay compacted");
            assert_is_compacted(&err, "entries after restart");
        }
        let hard_state = storage.hard_state();
        assert!(
            hard_state.commit >= applied + 1,
            "commit must advance after run 2 (commit={})",
            hard_state.commit
        );
    }
}

/// 3-node in-memory cluster: every node compacts while the cluster keeps
/// replicating, and entries proposed after compaction still reach every
/// follower through normal replication (catch-up within the margin).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_node_memory_cluster_replicates_through_compaction() {
    configure_test_compaction();
    const BASE_PORT: u16 = 16300;
    const FIRST_BATCH: u64 = 200;
    const SECOND_BATCH: u64 = 50;

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

    // First batch in waves of 50 so no follower ever drifts near the margin
    // while compaction is active; each wave waits for full application.
    let mut proposed_total: u64 = 0;
    for wave in 0..(FIRST_BATCH / 50) {
        proposed_total += 50;
        propose_batch(
            &runtimes,
            &recorders,
            &mut leader_idx,
            &format!("pre-compact-w{wave}"),
            50,
            proposed_total,
        )
        .await;
    }

    // Give every node's compaction cycle time to run at the settled apply
    // frontier (applied = 200 > margin, so each node discards its prefix).
    tokio::time::sleep(CHECK_INTERVAL * 10).await;

    // Second batch after compaction: followers must still receive everything
    // via normal replication.
    proposed_total += SECOND_BATCH;
    propose_batch(
        &runtimes,
        &recorders,
        &mut leader_idx,
        "post-compact",
        SECOND_BATCH,
        proposed_total,
    )
    .await;

    let expected: Vec<Vec<u8>> = {
        let mut all = Vec::new();
        for wave in 0..(FIRST_BATCH / 50) {
            for i in 0..50u64 {
                all.push(format!("pre-compact-w{wave}-{i:04}").into_bytes());
            }
        }
        for i in 0..SECOND_BATCH {
            all.push(format!("post-compact-{i:04}").into_bytes());
        }
        all
    };
    for (i, recorder) in recorders.iter().enumerate() {
        let recorder = recorder.clone();
        let all_applied = eventually(Duration::from_secs(30), || {
            let payloads = recorder.applied_payloads();
            expected.iter().all(|p| payloads.contains(p))
        })
        .await;
        let applied = recorder.applied_entries();
        assert!(
            all_applied,
            "node {i} must apply every entry proposed after compaction; applied {} of {}",
            applied.len(),
            expected.len()
        );
        assert_eq!(
            applied.len(),
            proposed_total as usize,
            "node {i} must apply exactly the {proposed_total} proposed entries (no drops, no duplicates)"
        );
    }

    for rt in runtimes {
        rt.shutdown();
        Box::new(rt).join().await.expect("join");
    }
}

/// Direct boundary checks for `CatgaStorage::maybe_compact` on both
/// variants: never compact at/below the margin, never beyond applied, and
/// the discarded prefix reports `StorageError::Compacted`.
#[tokio::test(flavor = "current_thread")]
async fn maybe_compact_respects_margin_and_reports_compacted() {
    configure_test_compaction();

    // Memory variant.
    let memory = CatgaStorage::memory_with_conf_state((vec![1u64], vec![]));
    let entries: Vec<Entry> = (1..=150u64)
        .map(|i| {
            let mut e = Entry::default();
            e.index = i;
            e.term = 1;
            e.data = format!("mem-{i:04}").into_bytes().into();
            e
        })
        .collect();
    memory.append_entries(&entries).expect("append");
    assert_eq!(RaftStorage::first_index(&memory).unwrap(), 1);

    // applied at or below the margin: nothing may move.
    memory.maybe_compact(0).expect("compact at 0 is a no-op");
    memory.maybe_compact(MARGIN).expect("compact at the margin is a no-op");
    assert_eq!(RaftStorage::first_index(&memory).unwrap(), 1);
    assert_eq!(RaftStorage::last_index(&memory).unwrap(), 150);

    // applied = 150 -> boundary 50; the prefix reports Compacted.
    memory.maybe_compact(150).expect("compact");
    assert_eq!(RaftStorage::first_index(&memory).unwrap(), 150 - MARGIN);
    assert_eq!(RaftStorage::last_index(&memory).unwrap(), 150);
    let err = RaftStorage::entries(&memory, 1, 150 - MARGIN, None, GetEntriesContext::empty(false))
        .expect_err("entries below the boundary must fail");
    assert_is_compacted(&err, "memory entries");
    let err = RaftStorage::term(&memory, 150 - MARGIN - 1).expect_err("term below the boundary must fail");
    assert_is_compacted(&err, "memory term");
    assert_eq!(RaftStorage::term(&memory, 150 - MARGIN).unwrap(), 1);
    let kept = RaftStorage::entries(
        &memory,
        150 - MARGIN,
        151,
        None,
        GetEntriesContext::empty(false),
    )
    .unwrap();
    assert_eq!(kept.len(), MARGIN as usize + 1);

    // Repeated compaction at the same frontier is a no-op.
    memory.maybe_compact(150).expect("idempotent compact");
    assert_eq!(RaftStorage::first_index(&memory).unwrap(), 150 - MARGIN);

    // Engine variant: same policy, durable boundary.
    let dir = tempdir().unwrap();
    let engine_storage = CatgaStorage::engine(dir.path().join("unit"), 7, None).expect("engine open");
    engine_storage.append_entries(&entries).expect("append to engine");
    engine_storage.maybe_compact(MARGIN).expect("no-op at margin");
    assert_eq!(RaftStorage::first_index(&engine_storage).unwrap(), 1);
    engine_storage.maybe_compact(150).expect("engine compact");
    assert_eq!(RaftStorage::first_index(&engine_storage).unwrap(), 150 - MARGIN);
    assert_eq!(RaftStorage::last_index(&engine_storage).unwrap(), 150);
    let err = RaftStorage::entries(
        &engine_storage,
        1,
        150 - MARGIN,
        None,
        GetEntriesContext::empty(false),
    )
    .expect_err("engine entries below the boundary must fail");
    assert_is_compacted(&err, "engine entries");
    drop(engine_storage);

    // The boundary survives a reopen of the engine.
    let reopened = EngineStorage::open(dir.path().join("unit"), 7, None).expect("reopen engine");
    assert_eq!(RaftStorage::first_index(&reopened).unwrap(), 150 - MARGIN);
    assert_eq!(RaftStorage::last_index(&reopened).unwrap(), 150);
    let kept = RaftStorage::entries(
        &reopened,
        150 - MARGIN,
        151,
        None,
        GetEntriesContext::empty(false),
    )
    .unwrap();
    assert_eq!(kept.len(), MARGIN as usize + 1);
    assert_eq!(kept.first().unwrap().data, format!("mem-{:04}", 150 - MARGIN).into_bytes());
}
