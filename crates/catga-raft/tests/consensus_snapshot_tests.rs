//! Real raft snapshot tests (generation + install).
//!
//! Catga now ships actual state-machine snapshots inside ordinary raft
//! messages (`MsgSnap`), so a follower that falls further behind than the
//! compaction margin can still catch up instead of receiving the old empty
//! bootstrap snapshot.
//!
//! Coverage:
//! - (a) unit: an `EngineStorage` with a provider that returns fixed bytes at
//!   index N serves those bytes with correct metadata, reports `Compacted`
//!   below N afterwards, and stays consistent across a reopen;
//! - (b) e2e catch-up: a 3-node persistent cluster with a tiny compaction
//!   margin; node 3 is stopped while the pair keeps writing, compacting past
//!   node 3's match, then node 3 restarts on the same data dir and must reach
//!   the cluster's applied state (real snapshot install, or fast catch-up);
//!   plus a hand-built install-path unit test that drives the exact
//!   storage+restore sequence directly;
//! - (c) unit: a provider that errors surfaces
//!   `StorageError::SnapshotTemporarilyUnavailable`, which raft treats as
//!   "retry later" rather than a fatal store error.
//!
//! Ports: 18xxx range (unused by other test files).

use std::sync::Arc;
use std::time::Duration;

use catga_core::{CatgaError, CatgaResult, ConsensusRuntime, ConsensusStateMachine, ErrorCode};
use catga_raft::storage::{CatgaStorage, EngineStorage, SnapshotProvider};
use catga_raft::{ApplyThread, CatgaRaftError, CatgaRaftRuntimeBuilder};
use parking_lot::Mutex;
use raft::prelude::{ConfState, Entry, Snapshot};
use raft::storage::GetEntriesContext;
use raft::{Error as RaftError, StorageError, Storage as RaftStorage};
use tempfile::tempdir;

/// Tiny margin so compaction (and hence the snapshot path) becomes reachable
/// with a few dozen entries. All tests in this binary share the process-wide
/// knob; each test binary is its own process, so it never leaks. Kept very
/// small (2) so that, once the margin is applied after catch-up, the leader
/// compacts almost to the frontier and the stopped follower cannot reach the
/// tip by plain replication — it must install a real snapshot.
const MARGIN: u64 = 2;
/// Fast compaction cadence so the e2e test does not wait a minute.
const CHECK_INTERVAL: Duration = Duration::from_millis(100);

/// One applied log record, serialized into snapshots.
type EntryRec = (u64, Vec<u8>);

/// State machine that records every applied entry and can encode/decode its
/// full state, so `snapshot`/`restore` round-trip real data. `restores` counts
/// how many times a snapshot was installed, so tests can assert the snapshot
/// install path actually fired.
#[derive(Clone, Default)]
struct SnapMachine {
    applied: Arc<Mutex<Vec<EntryRec>>>,
    restores: Arc<std::sync::atomic::AtomicU64>,
}

impl SnapMachine {
    fn new() -> Self {
        Self {
            applied: Arc::new(Mutex::new(Vec::new())),
            restores: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    fn entries(&self) -> Vec<EntryRec> {
        self.applied.lock().clone()
    }

    fn payloads(&self) -> Vec<Vec<u8>> {
        self.applied.lock().iter().map(|(_, d)| d.clone()).collect()
    }

    fn restore_count(&self) -> u64 {
        self.restores.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn contains_all(&self, expected: &[Vec<u8>]) -> bool {
        let have = self.payloads();
        expected.iter().all(|p| have.contains(p))
    }
}

fn encode_entries(entries: &[EntryRec]) -> CatgaResult<Vec<u8>> {
    bincode::serde::encode_to_vec(entries, bincode::config::standard())
        .map_err(|e| CatgaError::new(ErrorCode::Internal, format!("encode snapshot: {e}")))
}

fn decode_entries(bytes: &[u8]) -> CatgaResult<Vec<EntryRec>> {
    let (entries, _): (Vec<EntryRec>, usize) =
        bincode::serde::decode_from_slice(bytes, bincode::config::standard())
            .map_err(|e| CatgaError::new(ErrorCode::Internal, format!("decode snapshot: {e}")))?;
    Ok(entries)
}

impl ConsensusStateMachine for SnapMachine {
    fn apply(&mut self, index: u64, data: &[u8]) -> CatgaResult<()> {
        self.applied.lock().push((index, data.to_vec()));
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        encode_entries(&self.entries())
    }

    fn restore(&mut self, data: &[u8]) -> CatgaResult<()> {
        let entries = decode_entries(data)?;
        *self.applied.lock() = entries;
        self.restores.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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

fn assert_is_compacted(err: &RaftError, what: &str) {
    assert!(
        matches!(err, RaftError::Store(StorageError::Compacted)),
        "{what} below the snapshot boundary must report Compacted, got {err:?}"
    );
}

fn new_entry(index: u64, term: u64, data: Vec<u8>) -> Entry {
    let mut e = Entry::default();
    e.index = index;
    e.term = term;
    e.data = data.into();
    e
}

fn bootstrap_conf_state() -> ConfState {
    ConfState::from((vec![1u64, 2, 3], Vec::<u64>::new()))
}

// ---------------------------------------------------------------------------
// (a) EngineStorage snapshot generation via a provider.
// ---------------------------------------------------------------------------

/// A provider returning fixed bytes at index N drives `EngineStorage::snapshot`
/// to produce a real, data-bearing snapshot; everything at or below N is then
/// compacted, reads below N report `Compacted`, and the marker survives reopen.
#[tokio::test(flavor = "current_thread")]
async fn engine_provider_snapshot_compacts_and_survives_reopen() {
    const N: u64 = 20;
    const TERM: u64 = 7;
    let payload: Vec<u8> = b"state-machine-snapshot-bytes".to_vec();

    let dir = tempdir().unwrap();
    let data_dir = dir.path().join("node");

    {
        let storage =
            EngineStorage::open(&data_dir, 1, Some(bootstrap_conf_state())).expect("open storage");

        // Log entries 1..=N+5 so the snapshot index N is covered by a real
        // entry (term lookup) and a live tail remains above it.
        let entries: Vec<Entry> = (1..=N + 5).map(|i| new_entry(i, TERM, format!("e-{i}").into_bytes())).collect();
        storage.append(&entries).expect("append entries");
        assert_eq!(RaftStorage::last_index(&storage).unwrap(), N + 5);
        assert_eq!(RaftStorage::first_index(&storage).unwrap(), 1);

        let bytes = payload.clone();
        let provider: SnapshotProvider = Arc::new(move || Ok((bytes.clone(), N)));
        storage.set_snapshot_provider(provider);

        // raft asks for a snapshot (request_index 0 = leader-initiated).
        let snap = RaftStorage::snapshot(&storage, 0, 0).expect("snapshot must succeed");
        assert_eq!(snap.get_data(), payload.as_slice(), "snapshot carries the provider bytes");
        assert_eq!(snap.get_metadata().index, N, "metadata index is the applied index");
        assert_eq!(snap.get_metadata().term, TERM, "metadata term is the entry term at N");
        assert_eq!(snap.get_metadata().get_conf_state().voters, vec![1, 2, 3]);

        // The snapshot marker becomes the new compaction boundary.
        assert_eq!(RaftStorage::first_index(&storage).unwrap(), N + 1);
        assert_eq!(RaftStorage::last_index(&storage).unwrap(), N + 5);
        assert_eq!(RaftStorage::term(&storage, N).unwrap(), TERM, "boundary term stays readable");

        let err = RaftStorage::entries(&storage, 1, N + 1, None, GetEntriesContext::empty(false))
            .expect_err("entries below the boundary must fail");
        assert_is_compacted(&err, "entries");
        let err = RaftStorage::term(&storage, N - 1).expect_err("term below the boundary must fail");
        assert_is_compacted(&err, "term");

        // The live tail above the boundary is intact.
        let tail = RaftStorage::entries(&storage, N + 1, N + 6, None, GetEntriesContext::empty(false))
            .expect("tail entries readable");
        assert_eq!(tail.len(), 5);
        assert_eq!(tail.first().unwrap().index, N + 1);
        assert_eq!(tail.last().unwrap().index, N + 5);
    }

    // Reopen: the marker and boundary are durable; the provider is a runtime
    // hook and is intentionally not persisted.
    let reopened = EngineStorage::open(&data_dir, 1, None).expect("reopen storage");
    assert_eq!(RaftStorage::first_index(&reopened).unwrap(), N + 1);
    assert_eq!(RaftStorage::last_index(&reopened).unwrap(), N + 5);
    assert_eq!(RaftStorage::term(&reopened, N).unwrap(), TERM);
    let tail = RaftStorage::entries(&reopened, N + 1, N + 6, None, GetEntriesContext::empty(false))
        .expect("tail readable after reopen");
    assert_eq!(tail.len(), 5);
    let err = RaftStorage::entries(&reopened, 1, N + 1, None, GetEntriesContext::empty(false))
        .expect_err("prefix stays compacted after reopen");
    assert_is_compacted(&err, "entries after reopen");
}

// ---------------------------------------------------------------------------
// (b) Install path, driven directly (hand-built snapshot).
// ---------------------------------------------------------------------------

/// A hand-built snapshot applied through the same storage + apply sequence the
/// owner uses installs into storage, restores the state machine, and lets
/// later entries apply on top with no gap.
#[tokio::test(flavor = "current_thread")]
async fn hand_built_snapshot_installs_and_replays_on_top() {
    const SNAP_INDEX: u64 = 100;
    const SNAP_TERM: u64 = 9;

    // The state the leader would have shipped: entries 1..=SNAP_INDEX.
    let base: Vec<EntryRec> = (1..=SNAP_INDEX).map(|i| (i, format!("v-{i}").into_bytes())).collect();
    let snap_bytes = encode_entries(&base).expect("encode base state");

    let dir = tempdir().unwrap();
    let storage =
        EngineStorage::open(dir.path().join("node"), 1, Some(bootstrap_conf_state())).expect("open");

    let mut snap = Snapshot::default();
    snap.mut_metadata().index = SNAP_INDEX;
    snap.mut_metadata().term = SNAP_TERM;
    snap.mut_metadata().set_conf_state(bootstrap_conf_state());
    snap.set_data(snap_bytes.clone().into());

    storage.apply_snapshot(&snap).expect("apply snapshot to storage");
    assert_eq!(RaftStorage::first_index(&storage).unwrap(), SNAP_INDEX + 1);
    assert_eq!(RaftStorage::term(&storage, SNAP_INDEX).unwrap(), SNAP_TERM);
    let err = RaftStorage::term(&storage, SNAP_INDEX - 1).expect_err("below boundary compacted");
    assert_is_compacted(&err, "term below snapshot");

    // Restore the machine and rebase the apply frontier at the snapshot index.
    let machine = SnapMachine::new();
    let recorder = machine.clone();
    let apply = ApplyThread::new(machine);
    apply.restore(&snap_bytes, SNAP_INDEX).expect("restore machine");
    assert_eq!(apply.applied_index(), SNAP_INDEX);
    assert_eq!(recorder.entries().len(), SNAP_INDEX as usize);

    // Entries after the snapshot apply on top without a gap.
    apply.apply_entry(SNAP_INDEX + 1, b"after-1").expect("apply next");
    apply.apply_entry(SNAP_INDEX + 2, b"after-2").expect("apply next");
    assert_eq!(apply.applied_index(), SNAP_INDEX + 2);

    let entries = recorder.entries();
    assert_eq!(entries.len(), SNAP_INDEX as usize + 2);
    assert_eq!(entries[SNAP_INDEX as usize], (SNAP_INDEX + 1, b"after-1".to_vec()));
    assert_eq!(entries[SNAP_INDEX as usize + 1], (SNAP_INDEX + 2, b"after-2".to_vec()));
    assert_eq!(entries.first().unwrap(), &(1, b"v-1".to_vec()));
}

// ---------------------------------------------------------------------------
// (c) Provider failure -> SnapshotTemporarilyUnavailable (raft retries later).
// ---------------------------------------------------------------------------

/// A failing provider must surface `SnapshotTemporarilyUnavailable` (which
/// raft-rs treats as "retry later"), never a fatal store error or a panic.
#[tokio::test(flavor = "current_thread")]
async fn provider_error_surfaces_as_snapshot_temporarily_unavailable() {
    let dir = tempdir().unwrap();
    let storage =
        EngineStorage::open(dir.path().join("node"), 1, Some(bootstrap_conf_state())).expect("open");
    let entries: Vec<Entry> = (1..=10).map(|i| new_entry(i, 1, vec![])).collect();
    storage.append(&entries).expect("append");

    let provider: SnapshotProvider = Arc::new(|| {
        Err(CatgaRaftError::Storage("machine snapshot failed".into()))
    });
    storage.set_snapshot_provider(provider);

    let err = RaftStorage::snapshot(&storage, 0, 0).expect_err("provider error must surface");
    assert!(
        matches!(err, RaftError::Store(StorageError::SnapshotTemporarilyUnavailable)),
        "expected SnapshotTemporarilyUnavailable, got {err:?}"
    );

    // A provider reporting nothing applied yet (index 0) also defers cleanly,
    // since raft-rs rejects an index-0 snapshot.
    let storage2 =
        EngineStorage::open(dir.path().join("node2"), 1, Some(bootstrap_conf_state())).expect("open");
    let entries2: Vec<Entry> = (1..=3).map(|i| new_entry(i, 1, vec![])).collect();
    storage2.append(&entries2).expect("append");
    let provider0: SnapshotProvider = Arc::new(|| Ok((vec![1, 2, 3], 0)));
    storage2.set_snapshot_provider(provider0);
    let err = RaftStorage::snapshot(&storage2, 0, 0).expect_err("applied=0 must defer");
    assert!(
        matches!(err, RaftError::Store(StorageError::SnapshotTemporarilyUnavailable)),
        "expected SnapshotTemporarilyUnavailable for applied=0, got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// (b) End-to-end catch-up across a real snapshot.
// ---------------------------------------------------------------------------

/// Maps the cluster's agreed-upon leader endpoint back to a runtime index.
///
/// Unlike `is_leader()` — which merely reports "a leader endpoint is known"
/// and is true on followers too — the leader endpoint identifies the single
/// real leader. Knowing the genuine leader lets the test stop a real follower
/// and propose to a real leader, closing a latent race where the old
/// `is_leader()` discovery could select a follower, stop the actual leader,
/// and then silently lose fire-and-forget catch-up proposals during the
/// leaderless window that follows.
fn discover_leader_index(
    runtimes: &[catga_raft::CatgaRaftRuntime<SnapMachine>],
    base_port: u16,
) -> Option<usize> {
    for rt in runtimes {
        let Some(endpoint) = ConsensusRuntime::coordinator(rt).leader_endpoint() else {
            continue;
        };
        let Some(port) = endpoint.rsplit(':').next().and_then(|p| p.parse::<u16>().ok()) else {
            continue;
        };
        if port >= base_port && (port - base_port) % 100 == 0 {
            let idx = ((port - base_port) / 100) as usize;
            if idx < runtimes.len() {
                return Some(idx);
            }
        }
    }
    None
}

/// Proposes `payloads` through `runtimes[leader_idx]`, re-discovering the real
/// leader on transient failures, then waits until every recorder in `wait_for`
/// has applied at least `expected_total` entries.
///
/// Each payload goes through the attributed `propose_and_wait`, which resolves
/// only once THIS entry is committed and applied. A proposal dropped by a
/// leadership change therefore surfaces as a timeout and is retried against a
/// freshly discovered leader, instead of being accepted fire-and-forget and
/// silently lost.
async fn propose_batch(
    runtimes: &[catga_raft::CatgaRaftRuntime<SnapMachine>],
    leader_idx: &mut usize,
    base_port: u16,
    payloads: &[Vec<u8>],
    wait_for: &[SnapMachine],
    expected_total: usize,
) {
    for payload in payloads {
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            match runtimes[*leader_idx]
                .propose_and_wait(payload.clone(), Duration::from_secs(10))
                .await
            {
                Ok(_) => break,
                Err(e) => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "propose never committed: {e}"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    if let Some(idx) = discover_leader_index(runtimes, base_port) {
                        *leader_idx = idx;
                    }
                }
            }
        }
    }
    let all_applied = eventually(Duration::from_secs(30), || {
        wait_for.iter().all(|r| r.entries().len() >= expected_total)
    })
    .await;
    assert!(
        all_applied,
        "survivors must apply all {expected_total} entries before proceeding"
    );
}

/// 3-node persistent cluster, tiny compaction margin. Node 3 stops while the
/// remaining pair keeps writing and compacts past node 3's match; node 3 then
/// restarts on the same data dir and must reach the cluster's full applied
/// state (real snapshot install, or fast catch-up within the margin).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn stopped_node_catches_up_after_compaction_passes_it() {
    // Fast compaction cadence, but a large margin during warm-up so node 3
    // keeps its whole warm-up prefix until it is stopped. The margin drops to
    // MARGIN only for the catch-up writes, so it is the surviving leader (not
    // node 3) that compacts past node 3's match.
    CatgaStorage::set_compaction_interval(CHECK_INTERVAL);
    CatgaStorage::set_compaction_margin(10_000);
    const BASE_PORT: u16 = 18100;
    const WARMUP: u64 = 5;
    // Need applied - MARGIN > (WARMUP + 1) so the leader's compacted
    // first_index passes the stopped follower's match; with WARMUP=5 and
    // MARGIN=2 any CATCHUP >= 3 works. 7 keeps the pair-commit burst small
    // and reliable while leaving a multi-entry gap only a snapshot can fill.
    const CATCHUP: u64 = 7;

    let dir = tempdir().unwrap();
    let data_dirs: Vec<_> = (0..3).map(|i| dir.path().join(format!("node{i}"))).collect();

    let machines: Vec<SnapMachine> = (0..3).map(|_| SnapMachine::new()).collect();
    let recorders: Vec<SnapMachine> = machines.clone();

    let mut runtimes = Vec::new();
    for i in 0..3u64 {
        let runtime = CatgaRaftRuntimeBuilder::from_cli(BASE_PORT, i, 3)
            .expect("builder")
            .with_data_dir(&data_dirs[i as usize])
            .start(machines[i as usize].clone())
            .await
            .expect("start cluster node");
        runtimes.push(runtime);
    }

    // Elect a leader and warm the cluster up so all three share a baseline.
    // Discover the genuine leader via the agreed leader endpoint rather than
    // `is_leader()` (which is also true on followers), so the node stopped
    // below is guaranteed to be a follower and the leader stays in place.
    let mut leader_idx = {
        let mut found = None;
        let ok = eventually(Duration::from_secs(15), || {
            if let Some(idx) = discover_leader_index(&runtimes, BASE_PORT) {
                found = Some(idx);
                true
            } else {
                false
            }
        })
        .await;
        assert!(ok, "cluster must elect a leader");
        found.unwrap()
    };

    let warmup_payloads: Vec<Vec<u8>> =
        (0..WARMUP).map(|i| format!("warm-{i:04}").into_bytes()).collect();
    let warmup_recorders = vec![recorders[0].clone(), recorders[1].clone(), recorders[2].clone()];
    propose_batch(
        &runtimes,
        &mut leader_idx,
        BASE_PORT,
        &warmup_payloads,
        &warmup_recorders,
        WARMUP as usize,
    )
    .await;

    // Stop a FOLLOWER (never the leader), so the leader stays in place and the
    // remaining pair keeps a quorum without a disruptive re-election. Shutdown
    // + join make its data dir durable.
    let stopped_idx = (leader_idx + 1) % 3;
    let survivors: Vec<usize> = (0..3).filter(|&i| i != stopped_idx).collect();
    runtimes[stopped_idx]
        .shutdown_and_join()
        .await
        .expect("join stopped follower");

    // The surviving pair keeps writing. The margin is still large here, so the
    // helper never needs a snapshot and the pair commits reliably.
    let catchup_payloads: Vec<Vec<u8>> =
        (0..CATCHUP).map(|i| format!("catch-{i:04}").into_bytes()).collect();
    let survivor_recorders: Vec<SnapMachine> = survivors.iter().map(|&i| recorders[i].clone()).collect();
    propose_batch(
        &runtimes,
        &mut leader_idx,
        BASE_PORT,
        &catchup_payloads,
        &survivor_recorders,
        (WARMUP + CATCHUP) as usize,
    )
    .await;

    // Now shrink the margin with no new writes in flight: the survivors sit at
    // the settled frontier, so their compaction cycles move first_index past the
    // stopped follower's match without any helper needing a snapshot itself.
    CatgaStorage::set_compaction_margin(MARGIN);

    // Give the survivors' compaction cycles time to run at the settled frontier,
    // moving first_index past the stopped follower's last index.
    tokio::time::sleep(CHECK_INTERVAL * 12).await;

    // Restart the stopped follower on the SAME data dir with a fresh state
    // machine. Its raft log keeps the warm-up prefix, so its `matched` stays
    // valid and the leader's heartbeats carry a commit it can honor. The
    // catch-up entries past the leader's compacted boundary, however, are only
    // available via a snapshot. (A fully fresh data dir would leave the
    // follower at last_index 0 while the leader's stale progress sends a higher
    // heartbeat commit, which raft-rs rejects outright; keeping the prefix
    // avoids that while still forcing the snapshot path for the compacted tail.)
    let machine_r = SnapMachine::new();
    let recorder_r = machine_r.clone();
    let runtime_r = CatgaRaftRuntimeBuilder::from_cli(BASE_PORT, stopped_idx as u64, 3)
        .expect("builder")
        .with_data_dir(&data_dirs[stopped_idx])
        .start(machine_r)
        .await
        .expect("restart the stopped follower on the same data dir");

    let expected: Vec<Vec<u8>> = warmup_payloads
        .iter()
        .chain(catchup_payloads.iter())
        .cloned()
        .collect();
    // The restarted follower must rejoin and reach the cluster's full applied
    // state. With the leader compacted past the follower's match, the lagging
    // prefix is only servable via a snapshot; the margin-window tail may arrive
    // by plain replication. Which split occurs is timing-dependent in a live
    // cluster, so we assert full, exact catch-up (the deterministic install-path
    // proof is `hand_built_snapshot_installs_and_replays_on_top`).
    let caught_up = eventually(Duration::from_secs(45), || recorder_r.contains_all(&expected)).await;
    assert!(
        caught_up,
        "restarted follower must catch up to the cluster's applied state; has {} of {} payloads, {} snapshot restore(s)",
        recorder_r.payloads().len(),
        expected.len(),
        recorder_r.restore_count()
    );
    // No duplicates / no drops: the follower ends with exactly the committed
    // entries, whether they arrived by snapshot, replication, or a mix.
    assert_eq!(
        recorder_r.entries().len(),
        expected.len(),
        "restarted follower must apply exactly the committed entries (no drops, no duplicates)"
    );

    runtime_r.shutdown_and_join().await.expect("join restarted follower");
    for i in survivors {
        runtimes[i].shutdown_and_join().await.expect("join survivor");
    }
}
