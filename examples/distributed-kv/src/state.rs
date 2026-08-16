//! Shared replicated state and the consensus-agnostic state machine for the
//! KV store.
//!
//! Values persist in a redb database (copy-on-write B-trees: the file is
//! consistent at every instant, which makes file-copy snapshots safe). The
//! applied index persists in the same database so a restarted node resumes
//! from where it stopped; the dedup window of recent op ids stays in memory.
//!
//! Writes use group commit: applied entries are buffered in memory and
//! committed in one redb write transaction (one fsync). The background
//! flusher is event-driven: `apply` wakes it whenever it buffers an entry,
//! and it drains back-to-back batches, so under load the commit time itself
//! becomes the accumulation window (one fsync amortized over many entries).
//! When the buffer reaches [`GROUP_COMMIT_BATCH`] entries the apply path
//! commits synchronously to keep memory bounded; the flusher also wakes at
//! least every [`GROUP_COMMIT_INTERVAL`] as a safety net for tail entries.

use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use catga_core::{CatgaError, CatgaResult, ConsensusStateMachine, ErrorCode};
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

const APPLIED_OP_WINDOW: usize = 4096;

/// Group commit: maximum number of applied entries buffered before the
/// apply path forces a synchronous commit.
const GROUP_COMMIT_BATCH: usize = 32;
/// Group commit: safety-net wake cadence for the flusher when no new
/// entries arrive (normal wake-ups are event-driven via a Notify).
const GROUP_COMMIT_INTERVAL: Duration = Duration::from_millis(2);

const VALUES: TableDefinition<&str, &str> = TableDefinition::new("values");
const META: TableDefinition<&str, u64> = TableDefinition::new("meta");
const META_APPLIED_INDEX: &str = "applied_index";

/// One replicated write command carried by a committed Raft entry.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum KvCommand {
    Put {
        op_id: u64,
        key: String,
        value: String,
    },
}

impl KvCommand {
    /// Encodes the command into the replicated-entry payload.
    ///
    /// Bincode 2 instead of JSON: the payload is machine-to-machine only,
    /// and the binary encoding is both smaller and faster to (de)serialize
    /// on the apply hot path.
    pub(crate) fn encode(&self) -> CatgaResult<Vec<u8>> {
        bincode::serde::encode_to_vec(self, bincode::config::standard()).map_err(|error| {
            CatgaError::new(
                ErrorCode::SerializationFailed,
                format!("kv command encode failed: {error}"),
            )
        })
    }

    /// Decodes one replicated-entry payload back into a command.
    pub(crate) fn decode(data: &[u8]) -> CatgaResult<Self> {
        bincode::serde::decode_from_slice::<Self, _>(data, bincode::config::standard())
            .map(|(command, _)| command)
            .map_err(|error| {
                CatgaError::new(
                    ErrorCode::SerializationFailed,
                    format!("invalid kv command: {error}"),
                )
            })
    }
}

/// One applied-but-not-yet-committed entry in the group-commit buffer.
struct PendingPut {
    index: u64,
    op_id: u64,
    key: String,
    value: String,
}

/// Inner state shared between [`SharedState`], the state machine, and the
/// background flusher task.
struct Inner {
    db: Database,
    applied_index: AtomicU64,
    /// Recent committed op ids (dedup window), updated after each batch commit.
    applied_ops: Mutex<VecDeque<u64>>,
    /// Fired after each batch commit so `wait_applied` waiters wake up.
    applied_notify: tokio::sync::Notify,
    /// Applied entries waiting to be committed, in raft index order.
    pending: Mutex<Vec<PendingPut>>,
    /// Serializes flushes so batches commit in the order they are taken
    /// (redb's `begin_write` blocks on a concurrent write transaction but
    /// does not order waiters).
    flush_guard: Mutex<()>,
    /// Wakes the flusher whenever `apply` buffers an entry.
    flush_notify: tokio::sync::Notify,
    /// Set when `SharedState` is dropped so the flusher task stops.
    shutdown: AtomicBool,
}

impl Inner {
    /// Commits every buffered entry in one redb write transaction.
    ///
    /// `applied_index`, the op-id window, and the apply notification are
    /// only published after the commit is durable, so readers never observe
    /// uncommitted work. Returns `true` if a non-empty batch was committed.
    fn flush_batch(&self) -> CatgaResult<bool> {
        let _guard = self
            .flush_guard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entries = {
            let mut pending = self
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut *pending)
        };
        if entries.is_empty() {
            return Ok(false);
        }
        // Entries are buffered in raft index order, so the last one carries
        // the highest index of the batch.
        let last_index = entries[entries.len() - 1].index;

        let result = (|| -> CatgaResult<()> {
            let txn = self.db.begin_write().map_err(storage_error)?;
            {
                let mut values = txn.open_table(VALUES).map_err(storage_error)?;
                for entry in &entries {
                    values
                        .insert(entry.key.as_str(), entry.value.as_str())
                        .map_err(storage_error)?;
                }
                let mut meta = txn.open_table(META).map_err(storage_error)?;
                meta.insert(META_APPLIED_INDEX, last_index)
                    .map_err(storage_error)?;
            }
            txn.commit().map_err(storage_error)
        })();
        if let Err(error) = result {
            // Transaction failed: put the entries back at the front of the
            // buffer (they carry the lowest indexes) so a retry does not
            // lose them.
            let mut pending = self
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            pending.splice(0..0, entries);
            return Err(error);
        }

        self.applied_index.store(last_index, Ordering::Release);
        if let Ok(mut ops) = self.applied_ops.lock() {
            for entry in &entries {
                if ops.len() >= APPLIED_OP_WINDOW {
                    ops.pop_front();
                }
                ops.push_back(entry.op_id);
            }
        }
        self.applied_notify.notify_waiters();
        Ok(true)
    }
}

/// Applied read model shared between the Raft state machine and HTTP handlers.
pub(crate) struct SharedState {
    inner: Arc<Inner>,
    path: PathBuf,
}

impl SharedState {
    /// Opens (or creates) the redb database at `path` and starts the
    /// background group-commit flusher (must be called inside a tokio runtime).
    pub(crate) fn new(path: impl AsRef<Path>) -> CatgaResult<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(io_error)?;
        }
        let db = Database::create(&path).map_err(storage_error)?;
        // Make sure both tables exist before the first read.
        let txn = db.begin_write().map_err(storage_error)?;
        {
            txn.open_table(VALUES).map_err(storage_error)?;
            txn.open_table(META).map_err(storage_error)?;
        }
        txn.commit().map_err(storage_error)?;

        let applied_index = read_applied_index(&db)?.unwrap_or(0);

        let inner = Arc::new(Inner {
            db,
            applied_index: AtomicU64::new(applied_index),
            applied_ops: Mutex::new(VecDeque::new()),
            applied_notify: tokio::sync::Notify::new(),
            pending: Mutex::new(Vec::with_capacity(GROUP_COMMIT_BATCH)),
            flush_guard: Mutex::new(()),
            flush_notify: tokio::sync::Notify::new(),
            shutdown: AtomicBool::new(false),
        });

        // Group-commit flusher: woken by `apply` whenever an entry is
        // buffered, it drains back-to-back batches so that under load each
        // fsync-commit window accumulates many entries into one transaction.
        // The interval timeout is only a safety net for missed wake-ups.
        let flusher = Arc::clone(&inner);
        tokio::spawn(async move {
            loop {
                // Register before draining so a concurrent buffer push
                // cannot lose its notification.
                let notified = flusher.flush_notify.notified();
                loop {
                    match flusher.flush_batch() {
                        Ok(true) => {} // Committed a batch; drain what piled up meanwhile.
                        Ok(false) => break,
                        Err(error) => {
                            tracing::warn!("kv group-commit flush failed: {error}");
                            break;
                        }
                    }
                }
                if flusher.shutdown.load(Ordering::Acquire) {
                    break;
                }
                let _ = tokio::time::timeout(GROUP_COMMIT_INTERVAL, notified).await;
            }
        });

        Ok(Self { inner, path })
    }

    pub(crate) fn get(&self, key: &str) -> Option<String> {
        let txn = self.inner.db.begin_read().ok()?;
        let table = txn.open_table(VALUES).ok()?;
        table
            .get(key)
            .ok()
            .flatten()
            .map(|guard| guard.value().to_string())
    }

    pub(crate) fn applied_index(&self) -> u64 {
        self.inner.applied_index.load(Ordering::Acquire)
    }

    pub(crate) fn is_applied(&self, op_id: u64) -> bool {
        self.inner
            .applied_ops
            .lock()
            .map(|ops| ops.contains(&op_id))
            .unwrap_or(false)
    }

    pub(crate) async fn wait_applied(&self, op_id: u64, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.is_applied(op_id) {
                return true;
            }
            let notified = self.inner.applied_notify.notified();
            // Re-check after registering to avoid missing a notification.
            if self.is_applied(op_id) {
                return true;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return self.is_applied(op_id);
            }
            let _ = tokio::time::timeout(remaining, notified).await;
        }
    }

    /// Buffers an applied entry for group commit and reports whether the
    /// batch threshold has been reached (caller then flushes synchronously).
    fn buffer_put(&self, index: u64, op_id: u64, key: String, value: String) -> bool {
        let threshold = {
            let mut pending = self
                .inner
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            pending.push(PendingPut {
                index,
                op_id,
                key,
                value,
            });
            pending.len() >= GROUP_COMMIT_BATCH
        };
        // Wake the flusher so trailing entries do not wait for a full batch.
        self.inner.flush_notify.notify_one();
        threshold
    }

    /// Commits all buffered entries; a no-op when the buffer is empty.
    pub(crate) fn flush(&self) -> CatgaResult<()> {
        self.inner.flush_batch().map(|_| ())
    }
}

impl Drop for SharedState {
    fn drop(&mut self) {
        // Ask the flusher task to stop and wake it so it exits promptly.
        self.inner.shutdown.store(true, Ordering::Release);
        self.inner.flush_notify.notify_one();
    }
}

fn read_applied_index(db: &Database) -> CatgaResult<Option<u64>> {
    let txn = db.begin_read().map_err(storage_error)?;
    let table = txn.open_table(META).map_err(storage_error)?;
    Ok(table
        .get(META_APPLIED_INDEX)
        .map_err(storage_error)?
        .map(|guard| guard.value()))
}

fn storage_error(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Internal, format!("kv storage: {error}"))
}

fn io_error(error: std::io::Error) -> CatgaError {
    CatgaError::new(ErrorCode::Internal, format!("kv storage io: {error}"))
}

/// The deterministic state machine driven by the raft runtime.
///
/// It implements only the consensus-agnostic [`ConsensusStateMachine`]
/// contract; `node/raft_backend.rs` hands it to the raft runtime.
pub(crate) struct KvMachine {
    state: Arc<SharedState>,
}

impl KvMachine {
    pub(crate) fn new(state: Arc<SharedState>) -> Self {
        Self { state }
    }
}

impl Clone for KvMachine {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl ConsensusStateMachine for KvMachine {
    /// Buffers the committed entry for group commit; when the buffer reaches
    /// the batch threshold it commits synchronously so memory stays bounded.
    /// The background flusher commits smaller trailing batches.
    fn apply(&mut self, index: u64, data: &[u8]) -> CatgaResult<()> {
        let command = KvCommand::decode(data)?;
        match command {
            KvCommand::Put { op_id, key, value } => {
                if self.state.buffer_put(index, op_id, key, value) {
                    self.state.flush()?;
                }
            }
        }
        Ok(())
    }

    /// Snapshot = a byte copy of the redb file. The copy-on-write layout
    /// keeps the file consistent at every instant, so a plain file copy taken
    /// while the database is open is a valid snapshot (TiKV-style: snapshot
    /// the engine files, not a re-serialization).
    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        // Commit everything applied so far so the file copy is complete.
        self.state.flush()?;
        std::fs::read(&self.state.path).map_err(io_error)
    }

    /// Restore = replace local contents with the snapshot's contents. The
    /// snapshot file is opened read-only and every key is copied into the
    /// live database in one transaction, so the handle never needs reopening.
    fn restore(&mut self, bytes: &[u8]) -> CatgaResult<()> {
        // Defensive: commit anything still buffered before replacing state.
        self.state.flush()?;
        let tmp = self.state.path.with_extension("redb.snap");
        std::fs::write(&tmp, bytes).map_err(io_error)?;
        let result = (|| -> CatgaResult<()> {
            let snap = Database::open(&tmp).map_err(storage_error)?;
            let read_txn = snap.begin_read().map_err(storage_error)?;
            let snap_values = read_txn.open_table(VALUES).map_err(storage_error)?;
            let snap_applied = read_txn
                .open_table(META)
                .ok()
                .and_then(|meta| meta.get(META_APPLIED_INDEX).ok().flatten())
                .map(|guard| guard.value());

            let write_txn = self.state.inner.db.begin_write().map_err(storage_error)?;
            {
                let mut values = write_txn.open_table(VALUES).map_err(storage_error)?;
                // Clear local keys, then copy the snapshot contents.
                let keys: Vec<String> = {
                    let iter = values.iter().map_err(storage_error)?;
                    let mut keys = Vec::new();
                    for entry in iter {
                        let (key, _) = entry.map_err(storage_error)?;
                        keys.push(key.value().to_string());
                    }
                    keys
                };
                for key in keys {
                    values.remove(key.as_str()).map_err(storage_error)?;
                }
                let iter = snap_values.iter().map_err(storage_error)?;
                for entry in iter {
                    let (key, value) = entry.map_err(storage_error)?;
                    values
                        .insert(key.value(), value.value())
                        .map_err(storage_error)?;
                }
                let mut meta = write_txn.open_table(META).map_err(storage_error)?;
                if let Some(index) = snap_applied {
                    meta.insert(META_APPLIED_INDEX, index)
                        .map_err(storage_error)?;
                }
            }
            write_txn.commit().map_err(storage_error)?;
            if let Some(index) = snap_applied {
                self.state
                    .inner
                    .applied_index
                    .store(index, Ordering::Release);
            }
            Ok(())
        })();
        let _ = std::fs::remove_file(&tmp);
        result
    }
}
