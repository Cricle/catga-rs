//! Persistence tests for `EngineStorage`, the raft-engine-backed
//! implementation of `raft::Storage`.
//!
//! Every test opens storage in a fresh tempdir, mutates it, drops it, and
//! reopens the same directory to prove state survives restarts.

use catga_raft::storage::EngineStorage;
use raft::prelude::{ConfState, Entry, HardState, Snapshot};
use raft::storage::GetEntriesContext;
use raft::{Error as RaftError, Storage as RaftStorage, StorageError};
use tempfile::tempdir;

const REGION: u64 = 1;

fn new_entry(index: u64, term: u64, data: &[u8]) -> Entry {
    let mut entry = Entry::default();
    entry.index = index;
    entry.term = term;
    entry.data = data.to_vec().into();
    entry
}

fn conf_state(voters: Vec<u64>) -> ConfState {
    ConfState::from((voters, Vec::<u64>::new()))
}

fn entries(
    storage: &EngineStorage,
    low: u64,
    high: u64,
    max_size: impl Into<Option<u64>>,
) -> raft::Result<Vec<Entry>> {
    RaftStorage::entries(storage, low, high, max_size, GetEntriesContext::empty(false))
}

fn hard_state(term: u64, vote: u64, commit: u64) -> HardState {
    let mut hs = HardState::default();
    hs.term = term;
    hs.vote = vote;
    hs.commit = commit;
    hs
}

/// Fresh open bootstraps the conf state; a reopen without bootstrap keeps
/// the persisted one.
#[test]
fn fresh_open_bootstraps_conf_state_and_reopen_keeps_it() {
    let dir = tempdir().unwrap();

    {
        let storage = EngineStorage::open(dir.path(), REGION, Some(conf_state(vec![1, 2])))
            .expect("open fresh storage");
        let state = RaftStorage::initial_state(&storage).expect("initial state");
        assert!(state.initialized(), "bootstrapped storage must be initialized");
        assert_eq!(state.conf_state.voters, vec![1, 2]);
        assert_eq!(state.hard_state, HardState::default());
        assert_eq!(RaftStorage::first_index(&storage).unwrap(), 1);
        assert_eq!(RaftStorage::last_index(&storage).unwrap(), 0);
    }

    // Reopen without bootstrap: persisted conf state wins.
    let storage = EngineStorage::open(dir.path(), REGION, None).expect("reopen");
    let state = RaftStorage::initial_state(&storage).expect("initial state");
    assert!(state.initialized());
    assert_eq!(state.conf_state.voters, vec![1, 2]);
}

/// Appended entries and the hard state survive drop + reopen.
#[test]
fn append_and_hard_state_survive_reopen() {
    let dir = tempdir().unwrap();

    {
        let storage = EngineStorage::open(dir.path(), REGION, Some(conf_state(vec![1])))
            .expect("open");
        storage
            .append(&[
                new_entry(1, 1, b"a"),
                new_entry(2, 1, b"b"),
                new_entry(3, 2, b"c"),
            ])
            .expect("append");
        storage.set_hard_state(hard_state(2, 1, 2)).expect("hard state");
    }

    let storage = EngineStorage::open(dir.path(), REGION, None).expect("reopen");

    let state = RaftStorage::initial_state(&storage).expect("initial state");
    assert_eq!(state.hard_state.term, 2);
    assert_eq!(state.hard_state.vote, 1);
    assert_eq!(state.hard_state.commit, 2);
    assert_eq!(state.conf_state.voters, vec![1]);

    assert_eq!(RaftStorage::first_index(&storage).unwrap(), 1);
    assert_eq!(RaftStorage::last_index(&storage).unwrap(), 3);

    let got = entries(&storage, 1, 4, None).expect("entries");
    assert_eq!(got.len(), 3);
    assert_eq!(
        got.iter()
            .map(|e| (e.index, e.term, e.data.to_vec()))
            .collect::<Vec<_>>(),
        vec![
            (1, 1, b"a".to_vec()),
            (2, 1, b"b".to_vec()),
            (3, 2, b"c".to_vec()),
        ]
    );

    assert_eq!(RaftStorage::term(&storage, 1).unwrap(), 1);
    assert_eq!(RaftStorage::term(&storage, 3).unwrap(), 2);
    // Snapshot index 0 acts as the retained "entry before first_index".
    assert_eq!(RaftStorage::term(&storage, 0).unwrap(), 0);
    assert_eq!(
        RaftStorage::term(&storage, 4).unwrap_err(),
        RaftError::Store(StorageError::Unavailable)
    );
}

/// `entries` honors raft's limit_size semantics: always at least one entry,
/// truncated once the accumulated size exceeds the cap.
#[test]
fn entries_respects_max_size_limit() {
    let dir = tempdir().unwrap();
    let storage = EngineStorage::open(dir.path(), REGION, Some(conf_state(vec![1])))
        .expect("open");

    let big = vec![0xAB; 1024];
    storage
        .append(&[
            new_entry(1, 1, &big),
            new_entry(2, 1, &big),
            new_entry(3, 1, &big),
        ])
        .expect("append");

    // Unlimited: all three.
    assert_eq!(entries(&storage, 1, 4, None).unwrap().len(), 3);
    assert_eq!(entries(&storage, 1, 4, raft::util::NO_LIMIT).unwrap().len(), 3);

    // Each entry encodes to ~1KB; a 1200-byte cap keeps only the first.
    let limited = entries(&storage, 1, 4, Some(1200u64)).unwrap();
    assert_eq!(limited.len(), 1, "limit_size must keep exactly one entry");
    assert_eq!(limited[0].index, 1);
}

/// Compact removes entries below the compact index and returns `Compacted`
/// for reads that touch the removed prefix.
#[test]
fn compact_makes_prefix_reads_fail() {
    let dir = tempdir().unwrap();
    let storage = EngineStorage::open(dir.path(), REGION, Some(conf_state(vec![1])))
        .expect("open");

    storage
        .append(&[new_entry(1, 1, b"a"), new_entry(2, 1, b"b"), new_entry(3, 2, b"c")])
        .expect("append");

    // No-op cases mirror MemStorage::compact.
    storage.compact(1).expect("compact at first index is a no-op");
    assert_eq!(
        storage.compact(9).unwrap_err().to_string(),
        "storage error: compact index 9 beyond last index 3"
    );

    storage.compact(2).expect("compact to 2");
    assert_eq!(RaftStorage::first_index(&storage).unwrap(), 2);
    assert_eq!(RaftStorage::last_index(&storage).unwrap(), 3);

    assert_eq!(
        entries(&storage, 1, 3, None).unwrap_err(),
        RaftError::Store(StorageError::Compacted)
    );
    assert_eq!(
        RaftStorage::term(&storage, 1).unwrap_err(),
        RaftError::Store(StorageError::Compacted)
    );
    assert_eq!(entries(&storage, 2, 4, None).unwrap().len(), 2);

    // Compaction survives reopen.
    drop(storage);
    let reopened = EngineStorage::open(dir.path(), REGION, None).expect("reopen");
    assert_eq!(RaftStorage::first_index(&reopened).unwrap(), 2);
    assert_eq!(entries(&reopened, 2, 4, None).unwrap().len(), 2);
}

/// Applying a snapshot truncates the log (including any tail past the
/// snapshot index), updates hard/conf state, and survives reopen.
#[test]
fn apply_snapshot_truncates_log_and_survives_reopen() {
    let dir = tempdir().unwrap();
    let storage = EngineStorage::open(dir.path(), REGION, Some(conf_state(vec![1])))
        .expect("open");

    storage
        .append(&[new_entry(1, 1, b"a"), new_entry(2, 1, b"b"), new_entry(3, 1, b"c")])
        .expect("append");

    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = 2;
    snapshot.mut_metadata().term = 1;
    snapshot.mut_metadata().mut_conf_state().voters = vec![1, 7];

    storage.apply_snapshot(&snapshot).expect("apply snapshot");

    // The whole log is discarded, MemStorage-style: entry 3 is gone too.
    assert_eq!(RaftStorage::first_index(&storage).unwrap(), 3);
    assert_eq!(RaftStorage::last_index(&storage).unwrap(), 2);
    assert_eq!(
        entries(&storage, 1, 3, None).unwrap_err(),
        RaftError::Store(StorageError::Compacted)
    );
    assert_eq!(
        RaftStorage::term(&storage, 3).unwrap_err(),
        RaftError::Store(StorageError::Unavailable)
    );

    let state = RaftStorage::initial_state(&storage).expect("initial state");
    assert_eq!(state.hard_state.commit, 2);
    assert_eq!(state.hard_state.term, 1);
    assert_eq!(state.conf_state.voters, vec![1, 7]);

    // An out-of-date snapshot is rejected.
    let mut old = Snapshot::default();
    old.mut_metadata().index = 1;
    assert!(storage.apply_snapshot(&old).is_err());

    drop(storage);
    let reopened = EngineStorage::open(dir.path(), REGION, None).expect("reopen");
    assert_eq!(RaftStorage::first_index(&reopened).unwrap(), 3);
    assert_eq!(RaftStorage::last_index(&reopened).unwrap(), 2);
    let state = RaftStorage::initial_state(&reopened).expect("initial state");
    assert_eq!(state.conf_state.voters, vec![1, 7]);
    assert_eq!(state.hard_state.commit, 2);

    // New appends continue right after the snapshot.
    reopened.append(&[new_entry(3, 2, b"d")]).expect("append after snapshot");
    assert_eq!(RaftStorage::last_index(&reopened).unwrap(), 3);
    assert_eq!(entries(&reopened, 3, 4, None).unwrap()[0].data.as_ref(), b"d");
}

/// A conflicting append (lower or equal index, different term) overwrites
/// the tail, as raft requires after a leader change.
#[test]
fn conflicting_append_overwrites_tail() {
    let dir = tempdir().unwrap();
    let storage = EngineStorage::open(dir.path(), REGION, Some(conf_state(vec![1])))
        .expect("open");

    storage
        .append(&[new_entry(1, 1, b"a"), new_entry(2, 1, b"b"), new_entry(3, 1, b"c")])
        .expect("append");
    storage.append(&[new_entry(2, 2, b"B")]).expect("conflicting append");

    assert_eq!(RaftStorage::last_index(&storage).unwrap(), 2);
    assert_eq!(RaftStorage::term(&storage, 2).unwrap(), 2);
    let got = entries(&storage, 1, 3, None).unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got[1].data.as_ref(), b"B");

    drop(storage);
    let reopened = EngineStorage::open(dir.path(), REGION, None).expect("reopen");
    assert_eq!(RaftStorage::last_index(&reopened).unwrap(), 2);
    assert_eq!(entries(&reopened, 2, 3, None).unwrap()[0].term, 2);
}

/// `snapshot` returns an empty snapshot anchored at the commit index with
/// the current conf state (never SnapshotTemporarilyUnavailable).
#[test]
fn snapshot_uses_commit_index_and_conf_state() {
    let dir = tempdir().unwrap();
    let storage = EngineStorage::open(dir.path(), REGION, Some(conf_state(vec![1, 2])))
        .expect("open");

    storage
        .append(&[new_entry(1, 1, b"a"), new_entry(2, 1, b"b"), new_entry(3, 2, b"c")])
        .expect("append");
    storage.set_hard_state(hard_state(2, 1, 2)).expect("hard state");

    let snap = RaftStorage::snapshot(&storage, 0, 0).expect("snapshot");
    assert_eq!(snap.get_metadata().index, 2);
    assert_eq!(snap.get_metadata().term, 1);
    assert_eq!(snap.get_metadata().get_conf_state().voters, vec![1, 2]);
    assert!(snap.data.is_empty(), "empty-snapshot bootstrap carries no data");

    // Requests ahead of the commit are bumped to the request index.
    let snap = RaftStorage::snapshot(&storage, 5, 0).expect("snapshot");
    assert_eq!(snap.get_metadata().index, 5);
}
