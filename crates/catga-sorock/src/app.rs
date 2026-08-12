//! Adapts a [`ConsensusStateMachine`] to sorock's `RaftApp` trait.

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::anyhow;
use bytes::Bytes;
use catga_core::ConsensusStateMachine;
use futures::StreamExt;
use sorock::process::{LogIndex, RaftApp, SnapshotStream};

/// Index of the implicit genesis snapshot sorock seeds every fresh log with.
/// Snapshot index 1 never carries application bytes.
const GENESIS_SNAPSHOT_INDEX: LogIndex = 1;

/// Maximum size of one chunk yielded when a snapshot is streamed to a follower.
const SNAPSHOT_CHUNK_SIZE: usize = 256 * 1024;

/// sorock `RaftApp` implementation backed by a [`ConsensusStateMachine`].
///
/// Write entries are forwarded to [`ConsensusStateMachine::apply`] with their
/// log index; the response payload is always empty because the
/// `catga-core` consensus contract is fire-and-forget and carries no
/// application response.
///
/// # Reads
///
/// `process_read` is **not supported**: [`ConsensusStateMachine`] has no read
/// path, so reads return an empty payload without touching the machine. Issue
/// linearizable reads through your own application layer if you need them.
///
/// # Snapshots
///
/// sorock 0.12 never asks the application to *take* a snapshot; instead the
/// application advertises one through `get_latest_snapshot` and sorock folds
/// it into the log. [`SorockApp`] therefore snapshots the machine every
/// `snapshot_interval` applied entries (disabled when `0`) into an in-memory
/// store, serves the bytes to followers as a chunk stream, and restores the
/// machine on `install_snapshot`. The store is volatile: see the crate-level
/// docs before enabling snapshots on file-backed nodes.
pub struct SorockApp<M: ConsensusStateMachine> {
    machine: tokio::sync::Mutex<M>,
    snapshots: Mutex<BTreeMap<LogIndex, Bytes>>,
    last_applied: Arc<AtomicU64>,
    snapshot_interval: u64,
}

impl<M: ConsensusStateMachine> SorockApp<M> {
    /// Creates an app wrapper around `machine`.
    ///
    /// `snapshot_interval` is the number of applied entries between
    /// application-driven snapshots; `0` disables snapshotting.
    pub fn new(machine: M, snapshot_interval: u64) -> Self {
        Self {
            machine: tokio::sync::Mutex::new(machine),
            snapshots: Mutex::new(BTreeMap::new()),
            last_applied: Arc::new(AtomicU64::new(0)),
            snapshot_interval,
        }
    }

    /// Returns a shared counter tracking the greatest log index applied to the
    /// state machine (including installed snapshots). Used by
    /// [`crate::SorockRuntime`] to implement `applied_index`.
    pub fn applied_index_handle(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.last_applied)
    }

    fn lock_snapshots(&self) -> MutexGuard<'_, BTreeMap<LogIndex, Bytes>> {
        self.snapshots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn snapshot_bytes(&self, snapshot_index: LogIndex) -> anyhow::Result<Bytes> {
        self.lock_snapshots()
            .get(&snapshot_index)
            .cloned()
            .ok_or_else(|| {
                anyhow!("snapshot at index {snapshot_index} is not in the snapshot store")
            })
    }
}

#[async_trait::async_trait]
impl<M> RaftApp for SorockApp<M>
where
    M: ConsensusStateMachine + 'static,
{
    async fn process_read(&self, _request: &[u8]) -> anyhow::Result<Bytes> {
        // `ConsensusStateMachine` has no read path; reads are acknowledged
        // with an empty payload and never touch the machine.
        Ok(Bytes::new())
    }

    async fn process_write(&self, request: &[u8], entry_index: LogIndex) -> anyhow::Result<Bytes> {
        let mut machine = self.machine.lock().await;
        machine
            .apply(entry_index, request)
            .map_err(anyhow::Error::new)?;
        self.last_applied.fetch_max(entry_index, Ordering::AcqRel);
        if self.snapshot_interval > 0 && entry_index.is_multiple_of(self.snapshot_interval) {
            let snapshot = machine.snapshot().map_err(anyhow::Error::new)?;
            self.lock_snapshots()
                .insert(entry_index, Bytes::from(snapshot));
        }
        Ok(Bytes::new())
    }

    async fn install_snapshot(&self, snapshot_index: LogIndex) -> anyhow::Result<()> {
        if snapshot_index == GENESIS_SNAPSHOT_INDEX {
            // The genesis snapshot is implicit; the machine starts empty.
            return Ok(());
        }
        let bytes = self.snapshot_bytes(snapshot_index)?;
        let mut machine = self.machine.lock().await;
        machine.restore(&bytes).map_err(anyhow::Error::new)?;
        self.last_applied
            .fetch_max(snapshot_index, Ordering::AcqRel);
        Ok(())
    }

    async fn save_snapshot(
        &self,
        st: SnapshotStream,
        snapshot_index: LogIndex,
    ) -> anyhow::Result<()> {
        let mut st = st;
        let mut buf = Vec::new();
        while let Some(chunk) = st.next().await {
            buf.extend_from_slice(&chunk?);
        }
        self.lock_snapshots()
            .insert(snapshot_index, Bytes::from(buf));
        Ok(())
    }

    async fn open_snapshot(&self, snapshot_index: LogIndex) -> anyhow::Result<SnapshotStream> {
        let bytes = self.snapshot_bytes(snapshot_index)?;
        let chunks: Vec<anyhow::Result<Bytes>> = bytes
            .chunks(SNAPSHOT_CHUNK_SIZE)
            .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
            .collect();
        Ok(Box::pin(futures::stream::iter(chunks)))
    }

    async fn delete_snapshots_before(&self, i: LogIndex) -> anyhow::Result<()> {
        let mut snapshots = self.lock_snapshots();
        let kept = snapshots.split_off(&i);
        *snapshots = kept;
        Ok(())
    }

    async fn get_latest_snapshot(&self) -> anyhow::Result<LogIndex> {
        let latest = self
            .lock_snapshots()
            .keys()
            .next_back()
            .copied()
            .unwrap_or(GENESIS_SNAPSHOT_INDEX);
        Ok(latest)
    }
}
