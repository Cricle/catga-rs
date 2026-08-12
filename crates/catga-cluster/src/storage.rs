//! Durable and in-memory persistence backends for the Raft driver.

use std::{cmp, path::Path, sync::Arc};

use arc_swap::ArcSwapOption;
use raft::{
    Error, Result as RaftResult, StorageError,
    eraftpb::{ConfState, Entry, HardState, Snapshot},
    storage::{GetEntriesContext, MemStorage, RaftState, Storage},
};
use raft_engine::{Command, Config, Engine, LogBatch, MessageExt};

use crate::{RaftMember, RaftNodeError};

const RAFT_GROUP_ID: u64 = 1;
const HARD_STATE_KEY: &[u8] = b"catga/hard-state";
const CONF_STATE_KEY: &[u8] = b"catga/conf-state";
const SNAPSHOT_KEY: &[u8] = b"catga/snapshot";
/// Raw-byte record of the member endpoints bound to the persisted conf state.
///
/// The native Raft [`ConfState`] carries member identifiers only, so the
/// endpoint map is stored next to it to restore the coordinator view (and any
/// transport member map) after a restart.
const MEMBERS_KEY: &[u8] = b"catga/members";

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
        members: &[RaftMember],
    ) -> Result<Self, RaftNodeError> {
        PersistentRaftStorage::open(directory, conf_state, members).map(Self::Persistent)
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

    /// Durably records the latest applied voter configuration and its member
    /// endpoints.
    ///
    /// For the persistent backend the conf state and the endpoint map land in
    /// one synced `LogBatch`, matching the atomicity discipline of the other
    /// protocol-state writes. The in-memory backend only needs the conf state
    /// override so a later checkpoint snapshots the current membership.
    pub(crate) fn persist_membership(
        &self,
        conf_state: &ConfState,
        members: &[RaftMember],
    ) -> RaftResult<()> {
        match self {
            Self::InMemory(storage) => {
                storage.storage.wl().set_conf_state(conf_state.clone());
                Ok(())
            }
            Self::Persistent(storage) => storage.persist_membership(conf_state, members),
        }
    }

    /// Returns the member endpoint map persisted alongside the conf state, if
    /// this backend stores one.
    pub(crate) fn persisted_members(&self) -> RaftResult<Option<Vec<RaftMember>>> {
        match self {
            Self::InMemory(_) => Ok(None),
            Self::Persistent(storage) => storage.persisted_members(),
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

    /// Reads one bounded recovery page ending at the current durable commit.
    ///
    /// The continuation index advances over all Raft entries, including empty
    /// protocol entries, so callers cannot stop early after an empty page.
    pub(crate) fn committed_entries_page(
        &self,
        start_index: u64,
        max_entries: usize,
    ) -> RaftResult<(Vec<Entry>, Option<u64>)> {
        if max_entries == 0 {
            return Err(Error::Store(StorageError::Unavailable));
        }
        let state = self.initial_state()?;
        let first = self.first_index()?;
        let start = start_index.max(first);
        let commit = state.hard_state.commit;
        if start > commit {
            return Ok((Vec::new(), None));
        }
        let limit =
            u64::try_from(max_entries).map_err(|_| Error::Store(StorageError::Unavailable))?;
        let commit_exclusive = commit
            .checked_add(1)
            .ok_or(Error::Store(StorageError::Unavailable))?;
        let exclusive_end = start
            .checked_add(limit)
            .map(|end| end.min(commit_exclusive))
            .ok_or(Error::Store(StorageError::Unavailable))?;
        let entries = self.entries(start, exclusive_end, None, GetEntriesContext::empty(false))?;
        let next_index = (exclusive_end <= commit).then_some(exclusive_end);
        Ok((entries, next_index))
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
        if index != self.storage.last_index()? {
            return Err(Error::Store(StorageError::Unavailable));
        }
        self.storage.wl().apply_snapshot(snapshot.clone())?;
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
    /// Opens the durable log, bootstrapping membership only on a fresh directory.
    ///
    /// A directory that already holds a conf state always wins over the
    /// supplied configuration: cluster membership changes are applied through
    /// committed Raft conf-change entries, so a stale static configuration must
    /// never roll a restarted node back to an obsolete voter set.
    fn open(
        directory: &Path,
        conf_state: ConfState,
        members: &[RaftMember],
    ) -> Result<Self, RaftNodeError> {
        let engine = Engine::open(Config {
            dir: directory.to_string_lossy().into_owned(),
            ..Config::default()
        })
        .map_err(RaftNodeError::RaftEngine)?;
        let storage = Self {
            engine: std::sync::Arc::new(engine),
        };

        if storage.conf_state().map_err(RaftNodeError::Raft)?.is_some() {
            return Ok(storage);
        }
        let mut batch = LogBatch::default();
        batch
            .put_message(RAFT_GROUP_ID, CONF_STATE_KEY.to_vec(), &conf_state)
            .map_err(RaftNodeError::RaftEngine)?;
        batch
            .put(RAFT_GROUP_ID, MEMBERS_KEY.to_vec(), encode_members(members))
            .map_err(RaftNodeError::RaftEngine)?;
        storage
            .engine
            .write(&mut batch, true)
            .map_err(RaftNodeError::RaftEngine)?;
        Ok(storage)
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

    fn persist_membership(&self, conf_state: &ConfState, members: &[RaftMember]) -> RaftResult<()> {
        let mut batch = LogBatch::default();
        batch
            .put_message(RAFT_GROUP_ID, CONF_STATE_KEY.to_vec(), conf_state)
            .map_err(engine_error)?;
        batch
            .put(RAFT_GROUP_ID, MEMBERS_KEY.to_vec(), encode_members(members))
            .map_err(engine_error)?;
        self.engine.write(&mut batch, true).map_err(engine_error)?;
        Ok(())
    }

    fn persisted_members(&self) -> RaftResult<Option<Vec<RaftMember>>> {
        self.engine
            .get(RAFT_GROUP_ID, MEMBERS_KEY)
            .map(|bytes| decode_members(&bytes))
            .transpose()
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
            return Err(Error::Store(StorageError::Unavailable));
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

/// Encodes the member endpoint map as `u32 count` followed by `(u64 id,
/// u32 length, UTF-8 endpoint)` records, all little-endian.
fn encode_members(members: &[RaftMember]) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&(members.len() as u32).to_le_bytes());
    for member in members {
        encoded.extend_from_slice(&member.id().to_le_bytes());
        encoded.extend_from_slice(&(member.endpoint().len() as u32).to_le_bytes());
        encoded.extend_from_slice(member.endpoint().as_bytes());
    }
    encoded
}

fn decode_members(bytes: &[u8]) -> RaftResult<Vec<RaftMember>> {
    fn malformed() -> Error {
        Error::Store(StorageError::Other(
            "malformed persisted Raft member map".into(),
        ))
    }

    fn take<'a>(bytes: &mut &'a [u8], len: usize) -> RaftResult<&'a [u8]> {
        if bytes.len() < len {
            return Err(malformed());
        }
        let (head, tail) = bytes.split_at(len);
        *bytes = tail;
        Ok(head)
    }

    let mut rest = bytes;
    let count = u32::from_le_bytes(take(&mut rest, 4)?.try_into().map_err(|_| malformed())?);
    let mut members = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let id = u64::from_le_bytes(take(&mut rest, 8)?.try_into().map_err(|_| malformed())?);
        let len = u32::from_le_bytes(take(&mut rest, 4)?.try_into().map_err(|_| malformed())?);
        let endpoint =
            std::str::from_utf8(take(&mut rest, len as usize)?).map_err(|_| malformed())?;
        members.push(RaftMember::new(id, endpoint));
    }
    if !rest.is_empty() {
        return Err(malformed());
    }
    Ok(members)
}
