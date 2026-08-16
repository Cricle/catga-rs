//! EngineStorage - persistent `raft::Storage` backed by raft-engine.
//!
//! raft-engine 0.4 only persists log entries (as protobuf messages, via the
//! `MessageExt` adapter), so the remaining pieces of raft state - HardState,
//! ConfState and the last applied snapshot metadata - live in a small
//! companion file (`hard_state.bin`) next to the engine directory. The file
//! is rewritten atomically (temp file + rename) on every state change.
//!
//! Layout under the data directory:
//!
//! ```text
//! <data_dir>/
//!   engine/          raft-engine log files
//!   hard_state.bin   HardState + ConfState + snapshot metadata
//! ```

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use protobuf::Message as ProtobufMessage;
use raft::prelude::{ConfState, Entry, HardState, RaftState, Snapshot};
use raft::storage::GetEntriesContext;
use raft::{Error as RaftError, Storage, StorageError};
use raft_engine::{Config as RaftEngineConfig, Engine, LogBatch, MessageExt};

use crate::error::{CatgaRaftError, CatgaRaftResult};

use super::catga_storage::SnapshotProvider;

/// Name of the companion file holding non-entry raft state.
const STATE_FILE_NAME: &str = "hard_state.bin";
/// Magic prefix of the companion file ("catga raft hard state").
const STATE_MAGIC: &[u8; 4] = b"CRHS";
/// Companion file format version.
const STATE_VERSION: u8 = 1;

/// Teaches raft-engine how to index raft-rs entries. Both crates speak
/// protobuf 2, so `Entry` is stored on disk verbatim without conversion.
struct EntryExt;

impl MessageExt for EntryExt {
    type Entry = Entry;

    fn index(e: &Entry) -> u64 {
        e.index
    }
}

/// The slice of raft state raft-engine does not persist.
#[derive(Default, Clone)]
struct PersistedCore {
    hard_state: HardState,
    conf_state: ConfState,
    /// Metadata index of the last applied snapshot (log truncated below it).
    snapshot_index: u64,
    /// Metadata term of the last applied snapshot.
    snapshot_term: u64,
    /// Highest valid log index. raft-engine cannot truncate the log tail, so
    /// after `apply_snapshot` (which discards every entry, MemStorage-style,
    /// including any past the snapshot index) stale tail entries are hidden
    /// by clamping reads to this value; they are physically overwritten by
    /// the next append. 0 means no entry was ever appended.
    ///
    /// With async persistence this may run ahead of raft-engine: entries
    /// appended through `append_visible` live in the pending overlay until
    /// the persist worker drains them, so this value is in-memory state.
    last_valid_index: u64,
    /// Highest log index that is actually durable: written to raft-engine
    /// with `sync = true` (or covered by a persisted snapshot). The
    /// companion file always encodes this value (clamped to
    /// `last_valid_index`), never the in-memory tail, so a restart never
    /// sees a log tail that raft-engine cannot serve.
    durable_last_index: u64,
}

/// Companion file layout:
/// `magic(4) | version(1) | hs_len(u32 LE) | hs | cs_len(u32 LE) | cs |
///  snapshot_index(u64 LE) | snapshot_term(u64 LE) | durable_last_index(u64 LE)`
///
/// The trailing field is the *durable* log tail (clamped to the visible
/// tail), so a restart never references entries raft-engine cannot serve.
fn encode_core(core: &PersistedCore) -> CatgaRaftResult<Vec<u8>> {
    let hs = core
        .hard_state
        .write_to_bytes()
        .map_err(|e| CatgaRaftError::Storage(format!("encode hard state: {e}")))?;
    let cs = core
        .conf_state
        .write_to_bytes()
        .map_err(|e| CatgaRaftError::Storage(format!("encode conf state: {e}")))?;
    let mut buf = Vec::with_capacity(4 + 1 + 8 + hs.len() + cs.len());
    buf.extend_from_slice(STATE_MAGIC);
    buf.push(STATE_VERSION);
    buf.extend_from_slice(&(hs.len() as u32).to_le_bytes());
    buf.extend_from_slice(&hs);
    buf.extend_from_slice(&(cs.len() as u32).to_le_bytes());
    buf.extend_from_slice(&cs);
    buf.extend_from_slice(&core.snapshot_index.to_le_bytes());
    buf.extend_from_slice(&core.snapshot_term.to_le_bytes());
    let durable = core.durable_last_index.min(core.last_valid_index);
    buf.extend_from_slice(&durable.to_le_bytes());
    Ok(buf)
}

fn decode_core(bytes: &[u8]) -> CatgaRaftResult<PersistedCore> {
    let err = |what: &str| CatgaRaftError::Storage(format!("corrupt {STATE_FILE_NAME}: {what}"));
    let read_u32 = |pos: usize| -> CatgaRaftResult<usize> {
        let raw: [u8; 4] = bytes
            .get(pos..pos + 4)
            .ok_or_else(|| err("truncated length"))?
            .try_into()
            .map_err(|_| err("truncated length"))?;
        Ok(u32::from_le_bytes(raw) as usize)
    };
    let read_u64 = |pos: usize, what: &str| -> CatgaRaftResult<u64> {
        let raw: [u8; 8] = bytes
            .get(pos..pos + 8)
            .ok_or_else(|| err(what))?
            .try_into()
            .map_err(|_| err(what))?;
        Ok(u64::from_le_bytes(raw))
    };

    if bytes.len() < 5 || &bytes[0..4] != STATE_MAGIC {
        return Err(err("bad magic"));
    }
    if bytes[4] != STATE_VERSION {
        return Err(err("unsupported version"));
    }

    let mut pos = 5;
    let hs_len = read_u32(pos)?;
    pos += 4;
    let hs_bytes = bytes
        .get(pos..pos + hs_len)
        .ok_or_else(|| err("truncated hard state"))?;
    pos += hs_len;
    let cs_len = read_u32(pos)?;
    pos += 4;
    let cs_bytes = bytes
        .get(pos..pos + cs_len)
        .ok_or_else(|| err("truncated conf state"))?;
    pos += cs_len;
    let snapshot_index = read_u64(pos, "truncated snapshot index")?;
    pos += 8;
    let snapshot_term = read_u64(pos, "truncated snapshot term")?;
    pos += 8;
    let durable_last_index = read_u64(pos, "truncated durable last index")?;

    let mut hard_state = HardState::default();
    hard_state
        .merge_from_bytes(hs_bytes)
        .map_err(|e| CatgaRaftError::Storage(format!("decode hard state: {e}")))?;
    let mut conf_state = ConfState::default();
    conf_state
        .merge_from_bytes(cs_bytes)
        .map_err(|e| CatgaRaftError::Storage(format!("decode conf state: {e}")))?;

    Ok(PersistedCore {
        hard_state,
        conf_state,
        snapshot_index,
        snapshot_term,
        // On disk there is one tail and it is durable; the in-memory visible
        // tail starts out equal to it and may run ahead during async persist.
        last_valid_index: durable_last_index,
        durable_last_index,
    })
}

/// Write atomically so a crash mid-write never leaves a torn state file.
fn write_atomic(path: &Path, data: &[u8]) -> CatgaRaftResult<()> {
    let tmp = path.with_extension("tmp");
    let mut file = std::fs::File::create(&tmp)
        .map_err(|e| CatgaRaftError::Storage(format!("create {}: {e}", tmp.display())))?;
    file.write_all(data)
        .map_err(|e| CatgaRaftError::Storage(format!("write {}: {e}", tmp.display())))?;
    file.sync_all()
        .map_err(|e| CatgaRaftError::Storage(format!("sync {}: {e}", tmp.display())))?;
    std::fs::rename(&tmp, path)
        .map_err(|e| CatgaRaftError::Storage(format!("rename {}: {e}", path.display())))?;
    Ok(())
}

fn engine_err(e: raft_engine::Error) -> RaftError {
    RaftError::Store(StorageError::Other(Box::new(e)))
}

/// Persistent raft storage for a single raft group, backed by raft-engine.
///
/// Log entries live in raft-engine's append-only log files; HardState,
/// ConfState and snapshot metadata are persisted in a companion file.
#[derive(Clone)]
pub struct EngineStorage {
    engine: Arc<Engine>,
    /// raft-engine raft group id; the builder uses the node id.
    region_id: u64,
    core: Arc<Mutex<PersistedCore>>,
    state_path: PathBuf,
    /// Entries appended by the raft owner but not yet fsynced into
    /// raft-engine. Async persistence (`advance_append_async`) requires the
    /// entries to be readable from `Storage` before raft is advanced, so they
    /// land here first; the persist worker later drains them into raft-engine
    /// with `sync = true`. Reads consult the overlay for the tail it covers.
    overlay: Arc<Mutex<OverlayLog>>,
    /// Optional hook producing real state-machine snapshot bytes plus the
    /// applied index they reflect. When set, `Storage::snapshot` ships that
    /// data to far-behind followers instead of an empty bootstrap snapshot;
    /// when absent the historical empty-snapshot behavior is kept.
    /// Arc-wrapped so `Clone`s of this handle share the same hook.
    snapshot_provider: Arc<Mutex<Option<SnapshotProvider>>>,
}

/// A contiguous run of pending (visible but not yet durable) log entries.
#[derive(Default)]
struct OverlayLog {
    /// Index of `entries[0]`; meaningless while `entries` is empty.
    offset: u64,
    entries: Vec<Entry>,
}

impl OverlayLog {
    fn last_index(&self) -> Option<u64> {
        if self.entries.is_empty() {
            None
        } else {
            Some(self.offset + self.entries.len() as u64 - 1)
        }
    }

    fn term(&self, idx: u64) -> Option<u64> {
        if self.entries.is_empty() || idx < self.offset {
            return None;
        }
        let pos = (idx - self.offset) as usize;
        self.entries.get(pos).map(|e| e.term)
    }
}

impl EngineStorage {
    /// Open (or create) persistent storage under `dir`.
    ///
    /// `bootstrap_conf_state` is only applied on first use; on restart the
    /// persisted conf state wins.
    pub fn open(
        dir: impl AsRef<Path>,
        region_id: u64,
        bootstrap_conf_state: Option<ConfState>,
    ) -> CatgaRaftResult<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir).map_err(|e| {
            CatgaRaftError::Storage(format!("create data dir {}: {e}", dir.display()))
        })?;

        let config = RaftEngineConfig {
            dir: dir.join("engine").to_string_lossy().into_owned(),
            ..Default::default()
        };
        let engine = Engine::open(config).map_err(|e| {
            CatgaRaftError::Storage(format!("open raft-engine at {}: {e}", dir.display()))
        })?;

        let state_path = dir.join(STATE_FILE_NAME);
        let mut core = match std::fs::read(&state_path) {
            Ok(bytes) => decode_core(&bytes)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => PersistedCore::default(),
            Err(e) => {
                return Err(CatgaRaftError::Storage(format!(
                    "read {}: {e}",
                    state_path.display()
                )));
            }
        };

        let fresh = core.conf_state == ConfState::default();
        if fresh
            && let Some(conf_state) = bootstrap_conf_state {
                core.conf_state = conf_state;
            }

        let storage = Self {
            engine: Arc::new(engine),
            region_id,
            core: Arc::new(Mutex::new(core)),
            state_path,
            overlay: Arc::new(Mutex::new(OverlayLog::default())),
            snapshot_provider: Arc::new(Mutex::new(None)),
        };
        if fresh {
            // Persist immediately so a restart before the first ready still
            // finds a valid state file.
            storage.persist_core()?;
        }
        Ok(storage)
    }

    /// The raft group id this storage serves.
    pub fn region_id(&self) -> u64 {
        self.region_id
    }

    /// The currently persisted hard state.
    pub fn hard_state(&self) -> HardState {
        self.core.lock().hard_state.clone()
    }

    /// The currently persisted conf state.
    pub fn conf_state(&self) -> ConfState {
        self.core.lock().conf_state.clone()
    }

    /// Installs the hook that produces real state-machine snapshot bytes.
    ///
    /// Once set, [`Storage::snapshot`](Storage::snapshot) calls it to build a
    /// data-bearing snapshot for a far-behind follower and records the
    /// resulting snapshot marker (compacting the log prefix below the
    /// snapshot index). Before it is set, `snapshot` keeps the historical
    /// empty bootstrap behavior.
    pub fn set_snapshot_provider(&self, provider: SnapshotProvider) {
        *self.snapshot_provider.lock() = Some(provider);
    }

    /// Append entries to the stable log. Conflicting tail entries (same or
    /// lower index) are overwritten by raft-engine automatically.
    pub fn append(&self, entries: &[Entry]) -> CatgaRaftResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut batch = LogBatch::default();
        batch
            .add_entries::<EntryExt>(self.region_id, entries)
            .map_err(|e| CatgaRaftError::Storage(format!("batch entries: {e}")))?;
        // sync=true fsyncs the write: raft requires entries durable before
        // responding to the leader.
        self.engine
            .write(&mut batch, true)
            .map_err(|e| CatgaRaftError::Storage(format!("engine write: {e}")))?;
        // Raft appends are contiguous (extend or overwrite the tail), so the
        // last entry is the new end of the valid log even when this append
        // replaced a conflicting tail. A sync write is durable by definition.
        let last = entries.last().map(|e| e.index).unwrap_or(0);
        let mut core = self.core.lock();
        core.last_valid_index = last;
        core.durable_last_index = core.durable_last_index.max(last);
        drop(core);
        self.persist_core()
    }

    /// Make `Ready` entries readable without fsyncing (async persist phase 1).
    ///
    /// raft-rs requires the updates of a `Ready` to be readable from
    /// `Storage` before `RawNode::advance_append_async`, but the expensive
    /// fsync may happen later: entries land in the pending overlay and the
    /// persist worker drains them into raft-engine via [`Self::drain_visible`].
    /// This mirrors `MemStorage::append` conflict semantics: a conflicting
    /// suffix (same or lower index) is replaced.
    pub fn append_visible(&self, entries: &[Entry]) -> CatgaRaftResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let first = entries[0].index;
        let last = entries.last().map(|e| e.index).unwrap_or(first);
        {
            let core = self.core.lock();
            if first <= core.snapshot_index {
                return Err(CatgaRaftError::Storage(format!(
                    "append at {first} overlaps snapshot at {}",
                    core.snapshot_index
                )));
            }
        }
        {
            let mut overlay = self.overlay.lock();
            if overlay.entries.is_empty() {
                overlay.offset = first;
                overlay.entries = entries.to_vec();
            } else {
                let overlay_end = overlay
                    .last_index()
                    .expect("non-empty overlay has a last index");
                if first > overlay_end + 1 {
                    return Err(CatgaRaftError::Storage(format!(
                        "append gap: first {first} after overlay end {overlay_end}"
                    )));
                }
                if first <= overlay.offset {
                    // Truncation reaches into (or past) the overlay root:
                    // replace it wholesale; any engine entries at these
                    // indexes are rewritten when the overlay drains.
                    overlay.offset = first;
                    overlay.entries = entries.to_vec();
                } else {
                    let keep = (first - overlay.offset) as usize;
                    overlay.entries.truncate(keep);
                    overlay.entries.extend_from_slice(entries);
                }
            }
        }
        self.core.lock().last_valid_index = last;
        Ok(())
    }

    /// Drain the pending overlay up to `up_to` into raft-engine with
    /// `sync = true` (async persist phase 2: durability).
    ///
    /// One call fsyncs the whole append queue, so a batch of readies becomes
    /// durable behind a single raft-engine write. Entries superseded by a
    /// conflicting append may already be gone from the overlay; they need no
    /// durability and the (lower) durable tail stays correct.
    pub fn drain_visible(&self, up_to: u64) -> CatgaRaftResult<()> {
        // Snapshot the prefix WITHOUT removing it: raft-engine applies a
        // batch to its memtable only after the fsync inside `write`, so the
        // entries must stay readable from the overlay for the whole write.
        let drained: Vec<Entry> = {
            let overlay = self.overlay.lock();
            if overlay.entries.is_empty() {
                return Ok(());
            }
            let n = overlay
                .entries
                .iter()
                .take_while(|e| e.index <= up_to)
                .count();
            if n == 0 {
                return Ok(());
            }
            overlay.entries[..n].to_vec()
        };
        let mut batch = LogBatch::default();
        batch
            .add_entries::<EntryExt>(self.region_id, &drained)
            .map_err(|e| CatgaRaftError::Storage(format!("batch entries: {e}")))?;
        self.engine
            .write(&mut batch, true)
            .map_err(|e| CatgaRaftError::Storage(format!("engine write: {e}")))?;
        // The engine now serves the drained entries; drop them from the
        // overlay unless a conflicting append replaced them meanwhile (in
        // which case the overlay shadows the stale engine records until the
        // successor entries drain).
        {
            let mut overlay = self.overlay.lock();
            let n = drained.len();
            let unchanged = overlay.entries.len() >= n
                && overlay.offset == drained[0].index
                && overlay.entries[..n] == drained[..];
            if unchanged {
                overlay.entries.drain(..n);
                overlay.offset = overlay.entries.first().map(|e| e.index).unwrap_or(0);
            }
        }
        let last = drained.last().map(|e| e.index).unwrap_or(0);
        let mut core = self.core.lock();
        core.durable_last_index = core.durable_last_index.max(last);
        Ok(())
    }

    /// Persist the raft hard state (term, vote, commit).
    pub fn set_hard_state(&self, hard_state: HardState) -> CatgaRaftResult<()> {
        self.core.lock().hard_state = hard_state;
        self.persist_core()
    }

    /// Persist a new conf state (membership change).
    pub fn set_conf_state(&self, conf_state: ConfState) -> CatgaRaftResult<()> {
        self.core.lock().conf_state = conf_state;
        self.persist_core()
    }

    /// Advance the persisted commit index.
    ///
    /// raft-rs 0.7 reports commit-only hard state updates through
    /// `LightReady::commit_index()` instead of `Ready::hs()`, so they need a
    /// dedicated persist path.
    pub fn set_commit(&self, commit: u64) -> CatgaRaftResult<()> {
        {
            let mut core = self.core.lock();
            if commit <= core.hard_state.commit {
                return Ok(());
            }
            core.hard_state.commit = commit;
        }
        self.persist_core()
    }

    /// Apply a snapshot, mirroring `MemStorageCore::apply_snapshot`: the log
    /// is truncated, hard/conf state are updated, and everything survives
    /// restarts.
    pub fn apply_snapshot(&self, snapshot: &Snapshot) -> CatgaRaftResult<()> {
        let index = self.apply_snapshot_core(snapshot)?;
        let mut core = self.core.lock();
        core.durable_last_index = core.durable_last_index.max(index);
        drop(core);
        self.persist_core()
    }

    /// Make a received snapshot visible without fsyncing (async persist
    /// phase 1): the in-memory core and log truncation happen now, the
    /// companion file is rewritten later by the persist worker through
    /// [`Self::persist_snapshot_state`].
    pub fn apply_snapshot_visible(&self, snapshot: &Snapshot) -> CatgaRaftResult<()> {
        self.apply_snapshot_core(snapshot).map(|_| ())
    }

    /// Make the metadata of the last applied snapshot durable (async persist
    /// phase 2). Safe to call when no snapshot is pending: it just rewrites
    /// the companion file.
    pub fn persist_snapshot_state(&self, snapshot_index: u64) -> CatgaRaftResult<()> {
        let mut core = self.core.lock();
        core.durable_last_index = core.durable_last_index.max(snapshot_index);
        drop(core);
        self.persist_core()
    }

    /// Shared in-memory half of snapshot application.
    fn apply_snapshot_core(&self, snapshot: &Snapshot) -> CatgaRaftResult<u64> {
        let meta = snapshot.get_metadata();
        let index = meta.index;
        {
            let mut core = self.core.lock();
            if index == core.snapshot_index {
                // Already installed at this index (e.g. the owner retrying a
                // ready whose state-machine restore failed): the log prefix
                // is truncated and the marker recorded, so there is nothing
                // left to do.
                return Ok(index);
            }
            let first = self
                .engine
                .first_index(self.region_id)
                .unwrap_or(core.snapshot_index + 1);
            if first > index {
                return Err(CatgaRaftError::Storage(format!(
                    "snapshot out of date: index {index} < first index {first}"
                )));
            }
            core.snapshot_index = index;
            core.snapshot_term = meta.term;
            core.hard_state.term = core.hard_state.term.max(meta.term);
            core.hard_state.commit = index;
            core.conf_state = meta.get_conf_state().clone();
            // MemStorage discards the whole log on snapshot apply, including
            // entries past the snapshot index; hide them the same way.
            core.last_valid_index = index;
        }
        // A snapshot discards the whole log, pending overlay included.
        let mut overlay = self.overlay.lock();
        overlay.offset = 0;
        overlay.entries.clear();
        drop(overlay);
        // Entries up to `index` are covered by the snapshot;
        // compact_to(n) discards entries with index < n.
        self.engine.compact_to(self.region_id, index + 1);
        Ok(index)
    }

    /// Discard log entries with index strictly below `compact_index`,
    /// mirroring `MemStorageCore::compact` (the entry at `compact_index`
    /// itself is kept).
    ///
    /// Raft-rs 0.7 tolerates storage-side compaction without notification:
    /// `RaftLog` reads everything through the `Storage` trait, committed
    /// entries handed to the state machine are clamped to
    /// `max(applied + 1, first_index)`, and replication to a follower whose
    /// `next_idx` falls below `first_index` falls back to the snapshot path.
    /// The owner loop therefore only ever compacts up to
    /// `applied - COMPACTION_SAFETY_MARGIN`, which keeps every entry a
    /// temporarily lagging follower or a restart replay could still need
    /// readable (see `CatgaStorage::maybe_compact`).
    ///
    /// The new boundary is persisted by raft-engine as a `Compact` command,
    /// so `first_index` survives a restart. HardState and ConfState live in
    /// the companion file and are untouched.
    pub fn compact(&self, compact_index: u64) -> CatgaRaftResult<()> {
        let first = self.first_index_internal();
        if compact_index <= first {
            return Ok(());
        }
        let last = self.last_index_internal();
        if compact_index > last + 1 {
            return Err(CatgaRaftError::Storage(format!(
                "compact index {compact_index} beyond last index {last}"
            )));
        }
        self.engine.compact_to(self.region_id, compact_index);
        Ok(())
    }

    /// The current compaction boundary: every entry with index below this
    /// value has been discarded. Equals `Storage::first_index` as seen by
    /// raft-rs.
    pub fn compaction_progress(&self) -> u64 {
        self.first_index_internal()
    }

    /// Builds a real, data-bearing snapshot from the installed provider and
    /// records it as the log's new compaction boundary.
    ///
    /// Metadata decisions:
    ///
    /// * `index` — the provider's applied index (the state the bytes
    ///   reflect), raised to `request_index` if that is higher. The raise
    ///   only matters for explicit follower `request_snapshot` calls (catga
    ///   never issues them; the normal far-behind path passes
    ///   `request_index = 0`); it mirrors raft-rs `MemStorage`, whose
    ///   contract requires `index >= request_index`.
    /// * `term` — the term of the entry at `index`, resolved from the log.
    ///   raft-rs requires the term of the last entry the snapshot covers:
    ///   after a follower installs the snapshot, the leader's next
    ///   `MsgAppend` anchors at (`index`, `term`) and the follower checks it
    ///   against `Storage::term(index)` — a fabricated term would make the
    ///   receiver reject every subsequent append forever. If the term cannot
    ///   be resolved (cannot happen in practice: `index` is an applied index
    ///   and margin compaction never passes `applied - margin`), the
    ///   snapshot is deferred instead of guessing.
    ///
    /// Every failure returns `SnapshotTemporarilyUnavailable`, which raft-rs
    /// handles by leaving the peer in snapshot state and retrying later;
    /// any other storage error would be fatal to the raft node.
    fn provider_snapshot(
        &self,
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
            // Nothing applied yet: raft-rs rejects an index-0 snapshot
            // ("need non-empty snapshot"). Defer until the apply frontier
            // moves.
            return Err(unavailable());
        }
        let index = applied.max(request_index);
        if index > self.last_index_internal() {
            // The requested boundary lies beyond the log tail; defer until
            // entries (and the apply frontier) catch up.
            return Err(unavailable());
        }
        let term = self.term_internal(index).map_err(|_| unavailable())?;
        let conf_state = self.core.lock().conf_state.clone();

        // Record the snapshot marker, then truncate everything it covers:
        // raft-engine keeps entries with index >= index + 1, and any pending
        // overlay prefix at or below `index` is dropped so the persist worker
        // never re-drains it back into the engine. Entries above `index`
        // (the live log tail) are untouched.
        {
            let mut core = self.core.lock();
            if index > core.snapshot_index {
                core.snapshot_index = index;
                core.snapshot_term = term;
            }
        }
        {
            let mut overlay = self.overlay.lock();
            if !overlay.entries.is_empty() && overlay.offset <= index {
                let drop = overlay
                    .entries
                    .iter()
                    .take_while(|e| e.index <= index)
                    .count();
                overlay.entries.drain(..drop);
                overlay.offset = overlay.entries.first().map(|e| e.index).unwrap_or(0);
            }
        }
        self.engine.compact_to(self.region_id, index + 1);
        if let Err(e) = self.persist_core() {
            tracing::warn!(
                target: "catga_raft::storage",
                error = %e,
                "failed to persist snapshot marker; snapshot temporarily unavailable"
            );
            return Err(unavailable());
        }

        let mut snapshot = Snapshot::default();
        snapshot.set_data(data.into());
        snapshot.mut_metadata().index = index;
        snapshot.mut_metadata().term = term;
        snapshot.mut_metadata().set_conf_state(conf_state);
        Ok(snapshot)
    }

    fn persist_core(&self) -> CatgaRaftResult<()> {
        let core = self.core.lock().clone();
        let bytes = encode_core(&core)?;
        write_atomic(&self.state_path, &bytes)
    }

    fn first_index_internal(&self) -> u64 {
        let snapshot_index = self.core.lock().snapshot_index;
        self.engine
            .first_index(self.region_id)
            .unwrap_or(snapshot_index + 1)
    }

    fn last_index_internal(&self) -> u64 {
        let core = self.core.lock();
        if core.last_valid_index > 0 {
            return core.last_valid_index;
        }
        core.snapshot_index
    }

    /// Shared implementation of [`Storage::term`]: the snapshot boundary
    /// term, then the pending overlay, then raft-engine.
    fn term_internal(&self, idx: u64) -> raft::Result<u64> {
        let (snapshot_index, snapshot_term) = {
            let core = self.core.lock();
            (core.snapshot_index, core.snapshot_term)
        };
        if idx == snapshot_index {
            return Ok(snapshot_term);
        }
        let first = self.first_index_internal();
        if idx < first {
            return Err(RaftError::Store(StorageError::Compacted));
        }
        let last = self.last_index_internal();
        if idx > last {
            return Err(RaftError::Store(StorageError::Unavailable));
        }
        if let Some(term) = self.overlay.lock().term(idx) {
            return Ok(term);
        }
        match self.engine.get_entry::<EntryExt>(self.region_id, idx) {
            Ok(Some(entry)) => Ok(entry.term),
            Ok(None) => Err(RaftError::Store(StorageError::Unavailable)),
            Err(e) => Err(engine_err(e)),
        }
    }
}

impl Storage for EngineStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        let core = self.core.lock();
        Ok(RaftState::new(
            core.hard_state.clone(),
            core.conf_state.clone(),
        ))
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        if low < self.first_index_internal() {
            return Err(RaftError::Store(StorageError::Compacted));
        }
        let last = self.last_index_internal();
        if high > last + 1 {
            panic!("entries high {high} out of bound (last: {last})");
        }
        // raft-engine's size limit counts encoded entry bytes and always
        // returns at least one entry - the same semantics as raft's
        // `limit_size`.
        let max_size = max_size.into();
        let limit = match max_size {
            None | Some(raft::util::NO_LIMIT) => None,
            Some(max) => Some(max as usize),
        };
        let mut entries = Vec::new();
        // Hold the overlay lock for the whole read. The persist worker moves
        // entries from the overlay into raft-engine in two steps (engine
        // write, then overlay removal); holding the lock across both the
        // engine fetch and the overlay slice means we always observe a
        // consistent split - either an entry is still in the overlay or it
        // is already in raft-engine, never neither. The engine fetch is an
        // in-memory read and the worker's slow `engine.write` runs without
        // this lock, so this cannot deadlock.
        let overlay = self.overlay.lock();
        let overlay_offset = if overlay.entries.is_empty() {
            None
        } else {
            Some(overlay.offset)
        };
        let engine_high = match overlay_offset {
            Some(offset) if low < offset => std::cmp::min(high, offset),
            Some(_) => low,
            None => high,
        };
        if low < engine_high {
            self.engine
                .fetch_entries_to::<EntryExt>(self.region_id, low, engine_high, limit, &mut entries)
                .map_err(engine_err)?;
            if (entries.len() as u64) < engine_high - low {
                return Ok(entries);
            }
        }
        if let Some(offset) = overlay_offset
            && high > offset {
                let start = std::cmp::max(low, offset);
                let lo = (start - offset) as usize;
                let hi = (high - offset) as usize;
                if let Some(slice) = overlay.entries.get(lo..hi) {
                    entries.extend_from_slice(slice);
                }
            }
        drop(overlay);
        raft::util::limit_size(&mut entries, max_size);
        Ok(entries)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        self.term_internal(idx)
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.first_index_internal())
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.last_index_internal())
    }

    fn snapshot(&self, request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        // With a provider installed this produces a real state-machine
        // snapshot (data + metadata) for a far-behind follower; without one
        // it falls back to the empty bootstrap-style snapshot below.
        if let Some(provider) = self.snapshot_provider.lock().clone() {
            return self.provider_snapshot(request_index, &provider);
        }

        let (commit, conf_state, snapshot_index, snapshot_term) = {
            let core = self.core.lock();
            (
                core.hard_state.commit,
                core.conf_state.clone(),
                core.snapshot_index,
                core.snapshot_term,
            )
        };

        // Same construction as MemStorageCore::snapshot: the snapshot covers
        // everything up to the commit index.
        let index = commit.max(snapshot_index);
        let term = if index == snapshot_index {
            snapshot_term
        } else {
            self.engine
                .get_entry::<EntryExt>(self.region_id, index)
                .map_err(engine_err)?
                .map(|e| e.term)
                .ok_or(RaftError::Store(StorageError::Unavailable))?
        };

        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().index = index.max(request_index);
        snapshot.mut_metadata().term = term;
        snapshot.mut_metadata().set_conf_state(conf_state);
        Ok(snapshot)
    }
}
