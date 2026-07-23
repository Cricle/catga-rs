//! Durable and in-memory persistence backends for the Raft driver.

use std::{cmp, path::Path, sync::Arc};

use arc_swap::ArcSwapOption;
use raft::{
    Error, Result as RaftResult, StorageError,
    eraftpb::{ConfState, Entry, HardState, Snapshot},
    storage::{GetEntriesContext, MemStorage, RaftState, Storage},
};
use raft_engine::{Command, Config, Engine, LogBatch, MessageExt};

use crate::RaftNodeError;

const RAFT_GROUP_ID: u64 = 1;
const HARD_STATE_KEY: &[u8] = b"catga/hard-state";
const CONF_STATE_KEY: &[u8] = b"catga/conf-state";
const SNAPSHOT_KEY: &[u8] = b"catga/snapshot";

/// The entry adapter required by `raft-engine`'s protobuf log API.
struct RaftEntry;

impl MessageExt for RaftEntry {
    type Entry = Entry;

    fn index(entry: &Self::Entry) -> u64 {
        entry.index
    }
}

/// Storage selected by the public Raft node constructors.
#[derive(Clone)]
pub(crate) enum RaftStorage {
    InMemory(InMemoryRaftStorage),
    Persistent(PersistentRaftStorage),
}

impl RaftStorage {
    pub(crate) fn in_memory(conf_state: ConfState) -> Self {
        Self::InMemory(InMemoryRaftStorage::new(conf_state))
    }

    pub(crate) fn open_persistent(
        directory: &Path,
        conf_state: ConfState,
    ) -> Result<Self, RaftNodeError> {
        PersistentRaftStorage::open(directory, conf_state).map(Self::Persistent)
    }

    pub(crate) fn persist(
        &self,
        snapshot: Option<&Snapshot>,
        entries: &[Entry],
        hard_state: Option<&HardState>,
    ) -> RaftResult<()> {
        match self {
            Self::InMemory(storage) => {
                let snapshot_to_store = snapshot.cloned();
                let mut storage_core = storage.storage.wl();
                if let Some(snapshot) = snapshot {
                    storage_core.apply_snapshot(snapshot.clone())?;
                }
                if !entries.is_empty() {
                    storage_core.append(entries)?;
                }
                if let Some(hard_state) = hard_state {
                    storage_core.set_hardstate(hard_state.clone());
                }
                drop(storage_core);
                if let Some(snapshot) = snapshot_to_store {
                    storage.store_snapshot(snapshot);
                }
                Ok(())
            }
            Self::Persistent(storage) => storage.persist(snapshot, entries, hard_state),
        }
    }

    pub(crate) fn persist_commit(&self, commit: u64) -> RaftResult<()> {
        match self {
            Self::InMemory(storage) => {
                storage.storage.wl().mut_hard_state().set_commit(commit);
                Ok(())
            }
            Self::Persistent(storage) => storage.persist_commit(commit),
        }
    }

    pub(crate) fn committed_entries(&self) -> RaftResult<Vec<Entry>> {
        let state = self.initial_state()?;
        let commit = state.hard_state.commit;
        let first = self.first_index()?;
        if commit < first {
            return Ok(Vec::new());
        }
        self.entries(first, commit + 1, None, GetEntriesContext::empty(false))
    }

    pub(crate) fn application_snapshot(&self) -> RaftResult<Option<Snapshot>> {
        match self {
            Self::InMemory(storage) => Ok(storage
                .snapshot
                .load_full()
                .map(|snapshot| (*snapshot).clone())),
            Self::Persistent(storage) => storage.stored_snapshot(),
        }
    }

    pub(crate) fn create_snapshot(&self, index: u64, data: Vec<u8>) -> RaftResult<()> {
        let state = self.initial_state()?;
        if index == 0 || index > state.hard_state.commit {
            return Err(Error::Store(StorageError::Unavailable));
        }
        let mut snapshot = Snapshot::default();
        let metadata = snapshot.mut_metadata();
        metadata.index = index;
        metadata.term = self.term(index)?;
        metadata.set_conf_state(state.conf_state);
        snapshot.set_data(data.into());

        match self {
            Self::InMemory(storage) => storage.create_snapshot(snapshot),
            Self::Persistent(storage) => storage.persist_checkpoint(&snapshot),
        }
    }
}

impl Storage for RaftStorage {
    fn initial_state(&self) -> RaftResult<RaftState> {
        match self {
            Self::InMemory(storage) => storage.initial_state(),
            Self::Persistent(storage) => storage.initial_state(),
        }
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: GetEntriesContext,
    ) -> RaftResult<Vec<Entry>> {
        match self {
            Self::InMemory(storage) => storage.entries(low, high, max_size, context),
            Self::Persistent(storage) => storage.entries(low, high, max_size, context),
        }
    }

    fn term(&self, index: u64) -> RaftResult<u64> {
        match self {
            Self::InMemory(storage) => storage.term(index),
            Self::Persistent(storage) => storage.term(index),
        }
    }

    fn first_index(&self) -> RaftResult<u64> {
        match self {
            Self::InMemory(storage) => storage.first_index(),
            Self::Persistent(storage) => storage.first_index(),
        }
    }

    fn last_index(&self) -> RaftResult<u64> {
        match self {
            Self::InMemory(storage) => storage.last_index(),
            Self::Persistent(storage) => storage.last_index(),
        }
    }

    fn snapshot(&self, request_index: u64, to: u64) -> RaftResult<Snapshot> {
        match self {
            Self::InMemory(storage) => storage.snapshot(request_index, to),
            Self::Persistent(storage) => storage.snapshot(request_index, to),
        }
    }
}

#[derive(Clone)]
pub(crate) struct InMemoryRaftStorage {
    storage: MemStorage,
    snapshot: Arc<ArcSwapOption<Snapshot>>,
}

impl InMemoryRaftStorage {
    fn new(conf_state: ConfState) -> Self {
        Self {
            storage: MemStorage::new_with_conf_state(conf_state),
            snapshot: Arc::new(ArcSwapOption::empty()),
        }
    }

    fn create_snapshot(&self, snapshot: Snapshot) -> RaftResult<()> {
        let index = snapshot.get_metadata().index;
        self.storage.wl().compact(index.saturating_add(1))?;
        self.store_snapshot(snapshot);
        Ok(())
    }

    fn store_snapshot(&self, snapshot: Snapshot) {
        self.snapshot.store(Some(Arc::new(snapshot)));
    }
}

impl Storage for InMemoryRaftStorage {
    fn initial_state(&self) -> RaftResult<RaftState> {
        self.storage.initial_state()
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: GetEntriesContext,
    ) -> RaftResult<Vec<Entry>> {
        self.storage.entries(low, high, max_size, context)
    }

    fn term(&self, index: u64) -> RaftResult<u64> {
        if let Some(snapshot) = self.snapshot.load_full()
            && snapshot.get_metadata().index == index
        {
            return Ok(snapshot.get_metadata().term);
        }
        self.storage.term(index)
    }

    fn first_index(&self) -> RaftResult<u64> {
        self.storage.first_index()
    }

    fn last_index(&self) -> RaftResult<u64> {
        self.storage.last_index()
    }

    fn snapshot(&self, request_index: u64, to: u64) -> RaftResult<Snapshot> {
        match self.snapshot.load_full() {
            Some(snapshot) if snapshot.get_metadata().index >= request_index => {
                Ok((*snapshot).clone())
            }
            Some(_) => Err(Error::Store(StorageError::SnapshotTemporarilyUnavailable)),
            None => self.storage.snapshot(request_index, to),
        }
    }
}

/// A lock-free-at-the-driver-boundary `raft-engine` storage adapter.
///
/// `raft-engine` owns its internal concurrent indexes and write coordination;
/// this adapter adds no process-local locks around Raft reads or writes.
#[derive(Clone)]
pub(crate) struct PersistentRaftStorage {
    engine: std::sync::Arc<Engine>,
}

impl PersistentRaftStorage {
    fn open(directory: &Path, conf_state: ConfState) -> Result<Self, RaftNodeError> {
        let engine = Engine::open(Config {
            dir: directory.to_string_lossy().into_owned(),
            ..Config::default()
        })
        .map_err(RaftNodeError::RaftEngine)?;
        let storage = Self {
            engine: std::sync::Arc::new(engine),
        };

        match storage.conf_state().map_err(RaftNodeError::Raft)? {
            Some(existing) if existing != conf_state => {
                Err(RaftNodeError::PersistedConfStateMismatch)
            }
            Some(_) => Ok(storage),
            None => {
                let mut batch = LogBatch::default();
                batch
                    .put_message(RAFT_GROUP_ID, CONF_STATE_KEY.to_vec(), &conf_state)
                    .map_err(RaftNodeError::RaftEngine)?;
                storage
                    .engine
                    .write(&mut batch, true)
                    .map_err(RaftNodeError::RaftEngine)?;
                Ok(storage)
            }
        }
    }

    fn hard_state(&self) -> RaftResult<HardState> {
        self.engine
            .get_message(RAFT_GROUP_ID, HARD_STATE_KEY)
            .map(|hard_state| hard_state.unwrap_or_default())
            .map_err(engine_error)
    }

    fn conf_state(&self) -> RaftResult<Option<ConfState>> {
        self.engine
            .get_message(RAFT_GROUP_ID, CONF_STATE_KEY)
            .map_err(engine_error)
    }

    fn stored_snapshot(&self) -> RaftResult<Option<Snapshot>> {
        self.engine
            .get_message(RAFT_GROUP_ID, SNAPSHOT_KEY)
            .map_err(engine_error)
    }

    fn persist(
        &self,
        snapshot: Option<&Snapshot>,
        entries: &[Entry],
        hard_state: Option<&HardState>,
    ) -> RaftResult<()> {
        let mut batch = LogBatch::default();
        let mut hard_state = hard_state.cloned();

        if let Some(snapshot) = snapshot {
            let metadata = snapshot.get_metadata();
            batch
                .put_message(RAFT_GROUP_ID, SNAPSHOT_KEY.to_vec(), snapshot)
                .map_err(engine_error)?;
            batch
                .put_message(
                    RAFT_GROUP_ID,
                    CONF_STATE_KEY.to_vec(),
                    metadata.get_conf_state(),
                )
                .map_err(engine_error)?;
            if metadata.index > 0 {
                batch.add_command(
                    RAFT_GROUP_ID,
                    Command::Compact {
                        index: metadata.index + 1,
                    },
                );
            }

            let mut persisted = hard_state.take().unwrap_or(self.hard_state()?);
            persisted.term = cmp::max(persisted.term, metadata.term);
            persisted.commit = metadata.index;
            hard_state = Some(persisted);
        }

        if !entries.is_empty() {
            batch
                .add_entries::<RaftEntry>(RAFT_GROUP_ID, entries)
                .map_err(engine_error)?;
        }
        if let Some(hard_state) = hard_state {
            batch
                .put_message(RAFT_GROUP_ID, HARD_STATE_KEY.to_vec(), &hard_state)
                .map_err(engine_error)?;
        }
        self.engine.write(&mut batch, true).map_err(engine_error)?;
        Ok(())
    }

    fn persist_checkpoint(&self, snapshot: &Snapshot) -> RaftResult<()> {
        let metadata = snapshot.get_metadata();
        let mut batch = LogBatch::default();
        batch
            .put_message(RAFT_GROUP_ID, SNAPSHOT_KEY.to_vec(), snapshot)
            .map_err(engine_error)?;
        batch
            .put_message(
                RAFT_GROUP_ID,
                CONF_STATE_KEY.to_vec(),
                metadata.get_conf_state(),
            )
            .map_err(engine_error)?;
        batch.add_command(
            RAFT_GROUP_ID,
            Command::Compact {
                index: metadata.index + 1,
            },
        );
        self.engine
            .write(&mut batch, true)
            .map(|_| ())
            .map_err(engine_error)
    }

    fn persist_commit(&self, commit: u64) -> RaftResult<()> {
        let mut hard_state = self.hard_state()?;
        if hard_state.commit >= commit {
            return Ok(());
        }
        hard_state.commit = commit;
        let mut batch = LogBatch::default();
        batch
            .put_message(RAFT_GROUP_ID, HARD_STATE_KEY.to_vec(), &hard_state)
            .map_err(engine_error)?;
        self.engine.write(&mut batch, true).map_err(engine_error)?;
        Ok(())
    }
}

impl Storage for PersistentRaftStorage {
    fn initial_state(&self) -> RaftResult<RaftState> {
        Ok(RaftState::new(
            self.hard_state()?,
            self.conf_state()?.unwrap_or_default(),
        ))
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> RaftResult<Vec<Entry>> {
        let first = self.first_index()?;
        if low < first {
            return Err(Error::Store(StorageError::Compacted));
        }
        let last = self.last_index()?;
        if high > last + 1 {
            panic!("index out of bound (last: {}, high: {high})", last + 1);
        }
        if low == high {
            return Ok(Vec::new());
        }

        let max_size = max_size
            .into()
            .map(|size| usize::try_from(size).unwrap_or(usize::MAX));
        let mut entries = Vec::new();
        self.engine
            .fetch_entries_to::<RaftEntry>(RAFT_GROUP_ID, low, high, max_size, &mut entries)
            .map_err(engine_error)?;
        if entries.is_empty() {
            return Err(Error::Store(StorageError::Unavailable));
        }
        Ok(entries)
    }

    fn term(&self, index: u64) -> RaftResult<u64> {
        let snapshot = self.stored_snapshot()?.unwrap_or_default();
        let metadata = snapshot.get_metadata();
        if index == metadata.index {
            return Ok(metadata.term);
        }
        if index < self.first_index()? {
            return Err(Error::Store(StorageError::Compacted));
        }
        if index > self.last_index()? {
            return Err(Error::Store(StorageError::Unavailable));
        }
        self.engine
            .get_entry::<RaftEntry>(RAFT_GROUP_ID, index)
            .map_err(engine_error)?
            .map(|entry| entry.term)
            .ok_or(Error::Store(StorageError::Unavailable))
    }

    fn first_index(&self) -> RaftResult<u64> {
        let snapshot_index = self
            .stored_snapshot()?
            .map_or(0, |snapshot| snapshot.get_metadata().index);
        Ok(snapshot_index + 1)
    }

    fn last_index(&self) -> RaftResult<u64> {
        let snapshot_index = self
            .stored_snapshot()?
            .map_or(0, |snapshot| snapshot.get_metadata().index);
        Ok(self
            .engine
            .last_index(RAFT_GROUP_ID)
            .unwrap_or(snapshot_index)
            .max(snapshot_index))
    }

    fn snapshot(&self, request_index: u64, _to: u64) -> RaftResult<Snapshot> {
        let snapshot = self.stored_snapshot()?.unwrap_or_default();
        (snapshot.get_metadata().index >= request_index)
            .then_some(snapshot)
            .ok_or(Error::Store(StorageError::SnapshotTemporarilyUnavailable))
    }
}

fn engine_error(error: raft_engine::Error) -> Error {
    Error::Store(StorageError::Other(Box::new(error)))
}
