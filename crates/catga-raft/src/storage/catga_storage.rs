//! CatgaStorage - storage abstraction used by the builder and owner loop.
//!
//! `Memory` preserves the original in-memory behavior (log lost on restart);
//! `Engine` persists the raft log and hard state via raft-engine. Both
//! variants implement `raft::Storage` and expose the small persist API the
//! owner loop needs when processing `Ready`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use raft::prelude::{ConfState, Entry, HardState, RaftState, Snapshot};
use raft::storage::{GetEntriesContext, MemStorage, Storage};
use raft::{Error as RaftError, StorageError};

use super::engine::EngineStorage;
use crate::error::{CatgaRaftError, CatgaRaftResult};

/// Hook that produces a state-machine snapshot: the bytes returned by
/// `ConsensusStateMachine::snapshot()` plus the applied index that state
/// reflects.
///
/// Installed via [`CatgaStorage::set_snapshot_provider`] (the builder wires
/// it to the apply thread). `Storage::snapshot` calls it when raft needs a
/// snapshot for a far-behind follower; an `Err` result is surfaced to raft
/// as `StorageError::SnapshotTemporarilyUnavailable`, which makes raft retry
/// later instead of treating the storage as broken.
pub type SnapshotProvider = Arc<dyn Fn() -> CatgaRaftResult<(Vec<u8>, u64)> + Send + Sync>;

/// Shared, set-once slot for a [`SnapshotProvider`]. Arc-wrapped so every
/// `Clone` of a storage handle sees the same hook.
type SnapshotSlot = Arc<Mutex<Option<SnapshotProvider>>>;

fn new_snapshot_slot() -> SnapshotSlot {
    Arc::new(Mutex::new(None))
}

/// Number of applied log entries kept below the apply frontier as a safety
/// margin before compaction may delete them.
///
/// Compaction only ever discards entries strictly below
/// `applied_index - COMPACTION_SAFETY_MARGIN`. The margin protects two
/// consumers that still need readable entries below the apply frontier:
///
/// * temporarily lagging followers, which normally catch up through plain
///   `MsgAppend` replication. Once their `next_idx` falls below the
///   compaction boundary they are served a real state-machine snapshot
///   instead (see `set_snapshot_provider`), which always works but is far
///   more expensive than streaming the remaining entries;
/// * a restarting node, whose raft replay window starts at the compaction
///   boundary (`raft::RaftLog::new` initializes `applied` to
///   `first_index - 1`).
pub const COMPACTION_SAFETY_MARGIN: u64 = 10_000;

/// How often the owner loop checks whether the raft log can be compacted.
/// The check itself is cheap (two index comparisons); the cadence only
/// bounds how quickly freed prefix space is reclaimed.
pub const COMPACTION_CHECK_INTERVAL: Duration = Duration::from_secs(60);

static COMPACTION_MARGIN_OVERRIDE: AtomicU64 = AtomicU64::new(COMPACTION_SAFETY_MARGIN);
static COMPACTION_INTERVAL_MS_OVERRIDE: AtomicU64 =
    AtomicU64::new(COMPACTION_CHECK_INTERVAL.as_millis() as u64);

/// Storage variants for one raft group.
#[derive(Clone)]
pub enum CatgaStorage {
    /// In-memory storage; state is lost on restart. The slot carries the
    /// optional state-machine snapshot provider.
    Memory(MemStorage, SnapshotSlot),
    /// Persistent storage backed by raft-engine. The snapshot provider lives
    /// inside [`EngineStorage`] (`set_snapshot_provider`).
    Engine(Arc<EngineStorage>),
}

impl CatgaStorage {
    /// In-memory storage bootstrapped with the given conf state.
    pub fn memory_with_conf_state<T>(conf_state: T) -> Self
    where
        raft::prelude::ConfState: From<T>,
    {
        CatgaStorage::Memory(MemStorage::new_with_conf_state(conf_state), new_snapshot_slot())
    }

    /// Persistent storage opened (or created) under `dir`.
    pub fn engine(
        dir: impl AsRef<std::path::Path>,
        region_id: u64,
        bootstrap_conf_state: Option<raft::prelude::ConfState>,
    ) -> CatgaRaftResult<Self> {
        EngineStorage::open(dir, region_id, bootstrap_conf_state)
            .map(|storage| CatgaStorage::Engine(Arc::new(storage)))
    }

    /// Returns the engine storage if this is the persistent variant.
    pub fn engine_storage(&self) -> Option<&Arc<EngineStorage>> {
        match self {
            CatgaStorage::Engine(storage) => Some(storage),
            CatgaStorage::Memory(_, _) => None,
        }
    }

    /// Installs the state-machine snapshot provider used by
    /// `Storage::snapshot` to serve real snapshots to far-behind followers.
    ///
    /// The builder wires this from the apply thread
    /// (`machine.snapshot()` + `applied_index()`). Both variants honor it:
    /// the engine variant delegates to
    /// [`EngineStorage::set_snapshot_provider`], the memory variant keeps it
    /// in its shared slot. Without a provider both keep the historical empty
    /// bootstrap-style snapshot behavior.
    pub fn set_snapshot_provider(&self, provider: SnapshotProvider) {
        match self {
            CatgaStorage::Memory(_, slot) => *slot.lock() = Some(provider),
            CatgaStorage::Engine(storage) => storage.set_snapshot_provider(provider),
        }
    }

    /// Persist the hard state (term, vote, commit) from a raft `Ready`.
    pub fn persist_hard_state(&self, hard_state: HardState) -> CatgaRaftResult<()> {
        match self {
            CatgaStorage::Memory(storage, _) => {
                storage.wl().set_hardstate(hard_state);
                Ok(())
            }
            CatgaStorage::Engine(storage) => storage.set_hard_state(hard_state),
        }
    }

    /// Persist the conf state (membership) produced by applying a conf change.
    ///
    /// `RawNode::apply_conf_change` returns the resulting [`ConfState`]; it is
    /// the owner loop's job to make it durable so a restart reconstructs the
    /// same membership. The in-memory variant updates its core in place; the
    /// engine variant persists through its companion core file.
    pub fn set_conf_state(&self, conf_state: ConfState) -> CatgaRaftResult<()> {
        match self {
            CatgaStorage::Memory(storage, _) => {
                storage.wl().set_conf_state(conf_state);
                Ok(())
            }
            CatgaStorage::Engine(storage) => storage.set_conf_state(conf_state),
        }
    }

    /// Append `Ready` entries to stable storage.
    pub fn append_entries(&self, entries: &[Entry]) -> CatgaRaftResult<()> {
        match self {
            CatgaStorage::Memory(storage, _) => storage.wl().append(entries).map_err(storage_err),
            CatgaStorage::Engine(storage) => storage.append(entries),
        }
    }

    /// Make `Ready` entries readable without waiting for fsync (async
    /// persist, phase 1).
    ///
    /// raft-rs requires a `Ready`'s updates to be readable from `Storage`
    /// before `RawNode::advance_append_async`; durability follows later
    /// through [`Self::drain_visible`]. Memory storage is already durable,
    /// so this is a plain append there.
    pub fn append_visible(&self, entries: &[Entry]) -> CatgaRaftResult<()> {
        match self {
            CatgaStorage::Memory(storage, _) => storage.wl().append(entries).map_err(storage_err),
            CatgaStorage::Engine(storage) => storage.append_visible(entries),
        }
    }

    /// Make the visible log tail durable (async persist, phase 2).
    ///
    /// `up_to` is the highest entry index the caller needs durable; lower
    /// indexes are covered by the same fsync. Memory storage needs no work.
    pub fn drain_visible(&self, up_to: u64) -> CatgaRaftResult<()> {
        match self {
            CatgaStorage::Memory(_, _) => Ok(()),
            CatgaStorage::Engine(storage) => storage.drain_visible(up_to),
        }
    }

    /// Apply a snapshot received from the leader.
    pub fn apply_snapshot(&self, snapshot: Snapshot) -> CatgaRaftResult<()> {
        match self {
            CatgaStorage::Memory(storage, _) => {
                storage.wl().apply_snapshot(snapshot).map_err(storage_err)
            }
            CatgaStorage::Engine(storage) => storage.apply_snapshot(&snapshot),
        }
    }

    /// Make a received snapshot readable without waiting for fsync (async
    /// persist, phase 1); [`Self::persist_snapshot_state`] completes it.
    ///
    /// Re-applying the snapshot that is already installed at the same index
    /// is accepted as a no-op: the owner loop retries a whole `Ready` when
    /// the state-machine restore failed after the storage half succeeded,
    /// and the storage half must stay idempotent for that retry.
    pub fn apply_snapshot_visible(&self, snapshot: Snapshot) -> CatgaRaftResult<()> {
        match self {
            CatgaStorage::Memory(storage, _) => {
                let index = snapshot.get_metadata().index;
                match storage.wl().apply_snapshot(snapshot) {
                    Ok(()) => Ok(()),
                    Err(RaftError::Store(StorageError::SnapshotOutOfDate))
                        if raft::Storage::first_index(storage).map_or(false, |f| f > index) =>
                    {
                        // The boundary already sits past this snapshot.
                        Ok(())
                    }
                    Err(e) => Err(storage_err(e)),
                }
            }
            CatgaStorage::Engine(storage) => storage.apply_snapshot_visible(&snapshot),
        }
    }

    /// Make the metadata of the last applied snapshot durable (async
    /// persist, phase 2). Memory storage needs no work.
    pub fn persist_snapshot_state(&self, snapshot_index: u64) -> CatgaRaftResult<()> {
        match self {
            CatgaStorage::Memory(_, _) => Ok(()),
            CatgaStorage::Engine(storage) => storage.persist_snapshot_state(snapshot_index),
        }
    }

    /// Advance the persisted commit index.
    ///
    /// raft-rs 0.7 reports commit-only hard state updates through
    /// `LightReady::commit_index()` instead of `Ready::hs()`.
    pub fn persist_commit(&self, commit: u64) -> CatgaRaftResult<()> {
        match self {
            CatgaStorage::Memory(storage, _) => {
                let mut core = storage.wl();
                if commit > core.hard_state().commit {
                    core.mut_hard_state().commit = commit;
                }
                Ok(())
            }
            CatgaStorage::Engine(storage) => storage.set_commit(commit),
        }
    }

    /// The effective compaction safety margin (see
    /// [`COMPACTION_SAFETY_MARGIN`]).
    pub fn compaction_margin() -> u64 {
        COMPACTION_MARGIN_OVERRIDE.load(Ordering::Relaxed)
    }

    /// The effective compaction check cadence (see
    /// [`COMPACTION_CHECK_INTERVAL`]).
    pub fn compaction_interval() -> Duration {
        Duration::from_millis(COMPACTION_INTERVAL_MS_OVERRIDE.load(Ordering::Relaxed))
    }

    /// Override the process-wide compaction safety margin.
    ///
    /// Production keeps the [`COMPACTION_SAFETY_MARGIN`] default; this knob
    /// exists because the builder constructs storage internally, so tests
    /// cannot configure a per-instance margin. Each test binary runs in its
    /// own process, so the override never leaks between test suites.
    pub fn set_compaction_margin(margin: u64) {
        COMPACTION_MARGIN_OVERRIDE.store(margin, Ordering::Relaxed);
    }

    /// Override the process-wide compaction check cadence; same scope and
    /// rationale as [`Self::set_compaction_margin`].
    pub fn set_compaction_interval(interval: Duration) {
        COMPACTION_INTERVAL_MS_OVERRIDE.store(interval.as_millis() as u64, Ordering::Relaxed);
    }

    /// Conservatively compact the raft log based on the applied index.
    ///
    /// Discards entries strictly below `applied_index - margin` (margin from
    /// [`Self::compaction_margin`]) and nothing else:
    ///
    /// * never compacts when `applied_index <= margin` (the target would
    ///   underflow or reach index 0);
    /// * never compacts at or beyond the applied index, so raft-rs never
    ///   sees a `first_index` ahead of what the state machine has applied
    ///   (on restart `raft::RaftLog::new` bases its replay at
    ///   `first_index - 1`);
    /// * the margin keeps every entry a temporarily lagging follower may
    ///   still fetch via normal replication; a follower that falls further
    ///   behind than the boundary is served a real state-machine snapshot
    ///   instead (see [`Self::set_snapshot_provider`]).
    ///
    /// Safe to call repeatedly: a target at or below the current compaction
    /// boundary is a no-op. HardState and ConfState are untouched; only log
    /// entries are affected.
    pub fn maybe_compact(&self, applied_index: u64) -> CatgaRaftResult<()> {
        let margin = Self::compaction_margin();
        if applied_index <= margin {
            return Ok(());
        }
        let target = applied_index - margin;
        match self {
            CatgaStorage::Memory(storage, _) => {
                // `MemStorageCore::compact` panics when the target passes
                // `last_index + 1`; applied never runs past the log tail so
                // the clamp is belt-and-braces only.
                let last = storage.last_index().map_err(storage_err)?;
                storage.wl().compact(target.min(last)).map_err(storage_err)
            }
            CatgaStorage::Engine(storage) => storage.compact(target),
        }
    }
}

fn storage_err(e: raft::Error) -> CatgaRaftError {
    CatgaRaftError::Storage(e.to_string())
}

impl Storage for CatgaStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        match self {
            CatgaStorage::Memory(storage, _) => storage.initial_state(),
            CatgaStorage::Engine(storage) => storage.initial_state(),
        }
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        match self {
            CatgaStorage::Memory(storage, _) => storage.entries(low, high, max_size, context),
            CatgaStorage::Engine(storage) => storage.entries(low, high, max_size, context),
        }
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        match self {
            CatgaStorage::Memory(storage, _) => storage.term(idx),
            CatgaStorage::Engine(storage) => storage.term(idx),
        }
    }

    fn first_index(&self) -> raft::Result<u64> {
        match self {
            CatgaStorage::Memory(storage, _) => storage.first_index(),
            CatgaStorage::Engine(storage) => storage.first_index(),
        }
    }

    fn last_index(&self) -> raft::Result<u64> {
        match self {
            CatgaStorage::Memory(storage, _) => storage.last_index(),
            CatgaStorage::Engine(storage) => storage.last_index(),
        }
    }

    fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<Snapshot> {
        match self {
            CatgaStorage::Memory(storage, slot) => match slot.lock().clone() {
                Some(provider) => memory_provider_snapshot(storage, request_index, &provider),
                None => storage.snapshot(request_index, to),
            },
            CatgaStorage::Engine(storage) => storage.snapshot(request_index, to),
        }
    }
}

/// Provider-driven snapshot generation for the memory variant, mirroring
/// [`EngineStorage`](super::engine::EngineStorage)'s:
///
/// * `metadata.index` is the provider's applied index (raised to
///   `request_index` if higher, raft-rs `MemStorage` style);
/// * `metadata.term` is the true term of the entry at that index — after a
///   follower installs the snapshot, the leader's next append anchors at
///   (`index`, `term`) and a fabricated term would wedge the receiver;
/// * the snapshot is recorded as the storage's new compaction boundary via
///   `MemStorageCore::apply_snapshot` (which sets the snapshot metadata and
///   discards the log), and the tail beyond the boundary is re-appended so
///   current followers keep receiving entries. Commit/term are clamped so
///   the hard state never regresses.
///
/// Any failure surfaces as `SnapshotTemporarilyUnavailable` so raft retries
/// later; every other storage error would be fatal to the raft node.
fn memory_provider_snapshot(
    storage: &MemStorage,
    request_index: u64,
    provider: &SnapshotProvider,
) -> raft::Result<Snapshot> {
    let unavailable = || RaftError::Store(StorageError::SnapshotTemporarilyUnavailable);

    let (data, applied) = match provider() {
        Ok(result) => result,
        Err(e) => {
            tracing::warn!(
                target: "catga_raft::storage",
                error = %e,
                "snapshot provider failed; snapshot temporarily unavailable"
            );
            return Err(unavailable());
        }
    };
    if applied == 0 {
        // raft-rs rejects an index-0 snapshot ("need non-empty snapshot");
        // defer until the apply frontier moves.
        return Err(unavailable());
    }
    let index = applied.max(request_index);
    let last = storage.last_index().map_err(|_| unavailable())?;
    if index > last {
        return Err(unavailable());
    }
    let term = storage.term(index).map_err(|_| unavailable())?;
    let conf_state = storage.initial_state()?.conf_state;

    // The tail beyond the snapshot boundary survives the truncation.
    let tail = if index < last {
        storage.entries(index + 1, last + 1, None, GetEntriesContext::empty(false))?
    } else {
        Vec::new()
    };

    let mut snapshot = Snapshot::default();
    snapshot.set_data(data.into());
    snapshot.mut_metadata().index = index;
    snapshot.mut_metadata().term = term;
    snapshot.mut_metadata().set_conf_state(conf_state);

    {
        let mut core = storage.wl();
        let old_commit = core.hard_state().commit;
        let old_term = core.hard_state().term;
        match core.apply_snapshot(snapshot.clone()) {
            Ok(()) => {
                if !tail.is_empty() {
                    core.append(&tail)?;
                }
                // apply_snapshot rewrites commit/term to the snapshot's; do
                // not let either regress below what was already persisted.
                if old_commit > core.hard_state().commit {
                    core.mut_hard_state().commit = old_commit;
                }
                if old_term > core.hard_state().term {
                    core.mut_hard_state().term = old_term;
                }
            }
            // Boundary already at or past this snapshot (retry): the marker
            // is recorded; still serve the snapshot.
            Err(RaftError::Store(StorageError::SnapshotOutOfDate)) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(snapshot)
}
