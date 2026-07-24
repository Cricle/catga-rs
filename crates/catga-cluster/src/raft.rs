//! A single-owner `raft-rs` driver with a lock-free coordinator view.

use std::{
    collections::HashSet,
    error::Error,
    fmt, mem,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use raft::{
    Config, RawNode, Storage,
    eraftpb::{ConfState, Entry, EntryType, Message},
};
use slog::Logger;
use tokio::sync::Notify;

use crate::{ClusterCoordinator, RaftTiming, storage::RaftStorage};

/// A wire-level Raft protocol message for a caller-provided transport.
pub type RaftMessage = Message;

/// A stable Raft member identifier and its externally reachable endpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftMember {
    id: u64,
    endpoint: Arc<str>,
}

impl RaftMember {
    /// Creates one member definition.
    pub fn new(id: u64, endpoint: impl Into<Arc<str>>) -> Self {
        Self {
            id,
            endpoint: endpoint.into(),
        }
    }

    /// Returns the numeric Raft node identifier.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Returns the endpoint used to route traffic to this member.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

/// An application entry committed by Raft.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftCommittedEntry {
    /// The monotonic Raft log index assigned to this entry.
    pub index: u64,
    /// The application-owned command bytes.
    pub data: Vec<u8>,
}

/// A durable application state snapshot embedded in a native Raft snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftApplicationSnapshot {
    /// The last application command included in the snapshot.
    pub index: u64,
    /// Application-owned, opaque snapshot bytes.
    pub data: Vec<u8>,
}

/// Errors raised while building a Raft node.
#[derive(Debug)]
pub enum RaftNodeError {
    /// The supplied member list was empty.
    EmptyMembers,
    /// A member used the reserved zero Raft identifier.
    ZeroMemberId,
    /// Two supplied members used the same identifier.
    DuplicateMemberId(u64),
    /// The local node was absent from the supplied member list.
    LocalMemberMissing(u64),
    /// The endpoint for the local member did not match the local endpoint.
    LocalEndpointMismatch {
        /// The endpoint declared for the local member in cluster configuration.
        member: Arc<str>,
        /// The endpoint supplied to this node constructor.
        local: Arc<str>,
    },
    /// `raft-rs` rejected the supplied Raft configuration.
    Raft(raft::Error),
    /// `raft-engine` could not open or durably write the Raft log.
    RaftEngine(raft_engine::Error),
    /// The persisted voter configuration differs from the supplied members.
    PersistedConfStateMismatch,
}

impl fmt::Display for RaftNodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyMembers => formatter.write_str("a Raft cluster needs at least one member"),
            Self::ZeroMemberId => formatter.write_str("Raft member id zero is reserved"),
            Self::DuplicateMemberId(id) => write!(formatter, "duplicate Raft member id {id}"),
            Self::LocalMemberMissing(id) => write!(formatter, "local Raft member {id} is missing"),
            Self::LocalEndpointMismatch { member, local } => write!(
                formatter,
                "local endpoint {local} does not match configured member endpoint {member}"
            ),
            Self::Raft(error) => error.fmt(formatter),
            Self::RaftEngine(error) => error.fmt(formatter),
            Self::PersistedConfStateMismatch => {
                formatter.write_str("persisted Raft voters differ from the configured members")
            }
        }
    }
}

impl Error for RaftNodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Raft(error) => Some(error),
            Self::RaftEngine(error) => Some(error),
            _ => None,
        }
    }
}

impl From<raft::Error> for RaftNodeError {
    fn from(error: raft::Error) -> Self {
        Self::Raft(error)
    }
}

impl From<raft_engine::Error> for RaftNodeError {
    fn from(error: raft_engine::Error) -> Self {
        Self::RaftEngine(error)
    }
}

struct RaftCoordinatorState {
    leader_id: Option<u64>,
}

struct RaftCoordinatorInner {
    state: ArcSwap<RaftCoordinatorState>,
    changed: Notify,
}

/// Lock-free read model of a [`RaftNode`] for the existing cluster APIs.
pub struct RaftClusterNode {
    inner: Arc<RaftCoordinatorInner>,
    member_id: u64,
    node_id: Box<str>,
    members: Arc<[RaftMember]>,
    endpoints: Arc<[Arc<str>]>,
}

impl ClusterCoordinator for RaftClusterNode {
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn is_leader(&self) -> bool {
        self.inner.state.load().leader_id == Some(self.member_id)
    }

    fn leader_endpoint(&self) -> Option<Arc<str>> {
        let leader_id = self.inner.state.load().leader_id?;
        self.members
            .iter()
            .find(|member| member.id == leader_id)
            .map(|member| Arc::clone(&member.endpoint))
    }

    fn member_endpoints(&self) -> Arc<[Arc<str>]> {
        Arc::clone(&self.endpoints)
    }

    async fn wait_for_leadership(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            let notified = self.inner.changed.notified();
            if self.is_leader() {
                return true;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() || tokio::time::timeout(remaining, notified).await.is_err() {
                return self.is_leader();
            }
        }
    }

    async fn wait_for_leadership_change(&self, was_leader: bool) -> bool {
        loop {
            let notified = self.inner.changed.notified();
            let is_leader = self.is_leader();
            if is_leader != was_leader {
                return is_leader;
            }
            notified.await;
        }
    }
}

/// Owns one `raft-rs` [`RawNode`] and must be driven by exactly one task or thread.
///
/// This explicit ownership is the serialization boundary for Raft mutations. The
/// [`RaftClusterNode`] returned by [`Self::coordinator`] is independently
/// shareable and reads its leadership state without taking a mutex.
pub struct RaftNode {
    raw: RawNode<RaftStorage>,
    storage: RaftStorage,
    coordinator: Arc<RaftClusterNode>,
    outbox: Vec<RaftMessage>,
    committed: Vec<RaftCommittedEntry>,
    installed_snapshots: Vec<RaftApplicationSnapshot>,
    auto_acknowledge_apply: bool,
    last_acknowledged_index: u64,
    last_committed_index: u64,
}

impl RaftNode {
    /// Builds an in-memory Raft node with a fixed initial voter set.
    ///
    /// The `MemStorage` backend makes this constructor appropriate for tests
    /// and ephemeral deployments. A durable storage backend must persist the
    /// Raft log, hard state, snapshots, and applied application state together.
    pub fn new(
        id: u64,
        endpoint: impl Into<Arc<str>>,
        members: Vec<RaftMember>,
    ) -> Result<Self, RaftNodeError> {
        Self::new_with_timing(id, endpoint, members, RaftTiming::default_node())
    }

    /// Builds an in-memory Raft node using validated logical timing.
    pub fn new_with_timing(
        id: u64,
        endpoint: impl Into<Arc<str>>,
        members: Vec<RaftMember>,
        timing: RaftTiming,
    ) -> Result<Self, RaftNodeError> {
        let endpoint = endpoint.into();
        validate_members(id, &endpoint, &members)?;
        let storage = RaftStorage::in_memory(conf_state(&members));
        Self::from_storage(id, endpoint, members, storage, timing)
    }

    /// Opens a Raft node whose protocol state survives process restarts.
    ///
    /// The directory is exclusively owned by this node. It stores the Raft
    /// log, hard state, membership state, and received snapshots through
    /// `raft-engine`; application state must be snapshotted atomically by the
    /// caller before the corresponding Raft log is compacted.
    pub fn open_persistent(
        id: u64,
        endpoint: impl Into<Arc<str>>,
        members: Vec<RaftMember>,
        directory: impl AsRef<Path>,
    ) -> Result<Self, RaftNodeError> {
        Self::open_persistent_with_timing(
            id,
            endpoint,
            members,
            directory,
            RaftTiming::default_node(),
        )
    }

    /// Opens a persistent Raft node using validated logical timing.
    pub fn open_persistent_with_timing(
        id: u64,
        endpoint: impl Into<Arc<str>>,
        members: Vec<RaftMember>,
        directory: impl AsRef<Path>,
        timing: RaftTiming,
    ) -> Result<Self, RaftNodeError> {
        let endpoint = endpoint.into();
        validate_members(id, &endpoint, &members)?;
        let storage = RaftStorage::open_persistent(directory.as_ref(), conf_state(&members))?;
        Self::from_storage(id, endpoint, members, storage, timing)
    }

    fn from_storage(
        id: u64,
        _endpoint: Arc<str>,
        members: Vec<RaftMember>,
        storage: RaftStorage,
        timing: RaftTiming,
    ) -> Result<Self, RaftNodeError> {
        let members: Arc<[RaftMember]> = members.into();
        let config = Config {
            id,
            election_tick: timing.election_ticks(),
            heartbeat_tick: timing.heartbeat_ticks(),
            check_quorum: true,
            pre_vote: true,
            max_size_per_msg: 1024 * 1024,
            max_inflight_msgs: 256,
            ..Config::default()
        };
        let logger = Logger::root(slog::Discard, slog::o!());
        let raw = RawNode::new(&config, storage.clone(), &logger)?;
        let coordinator = Arc::new(RaftClusterNode {
            inner: Arc::new(RaftCoordinatorInner {
                state: ArcSwap::from_pointee(RaftCoordinatorState { leader_id: None }),
                changed: Notify::new(),
            }),
            member_id: id,
            node_id: id.to_string().into_boxed_str(),
            endpoints: members
                .iter()
                .map(|member| Arc::clone(&member.endpoint))
                .collect(),
            members,
        });
        Ok(Self {
            raw,
            storage,
            coordinator,
            outbox: Vec::new(),
            committed: Vec::new(),
            installed_snapshots: Vec::new(),
            auto_acknowledge_apply: true,
            last_acknowledged_index: 0,
            last_committed_index: 0,
        })
    }

    /// Returns the local numeric Raft node identifier.
    pub fn id(&self) -> u64 {
        self.raw.raft.id
    }

    /// Returns the shareable, lock-free coordinator view for this node.
    pub fn coordinator(&self) -> Arc<RaftClusterNode> {
        Arc::clone(&self.coordinator)
    }

    /// Starts an election immediately, which is useful for deterministic tests.
    pub fn campaign(&mut self) -> raft::Result<()> {
        self.raw.campaign()?;
        self.drive_ready()
    }

    /// Advances Raft's logical clock by one tick and processes all resulting work.
    pub fn tick(&mut self) -> raft::Result<()> {
        self.raw.tick();
        self.drive_ready()
    }

    /// Accepts one message received from the configured Raft transport.
    pub fn step(&mut self, message: RaftMessage) -> raft::Result<()> {
        self.raw.step(message)?;
        self.drive_ready()
    }

    /// Proposes an application command on the current leader.
    pub fn propose(&mut self, data: impl Into<Vec<u8>>) -> raft::Result<()> {
        self.raw.propose(Vec::new(), data.into())?;
        self.drive_ready()
    }

    /// Takes outbound Raft protocol messages for delivery by the caller's transport.
    pub fn drain_messages(&mut self) -> Vec<RaftMessage> {
        mem::take(&mut self.outbox)
    }

    /// Takes committed non-empty normal entries for application to business state.
    pub fn drain_committed(&mut self) -> Vec<RaftCommittedEntry> {
        mem::take(&mut self.committed)
    }

    /// Takes application snapshots installed from incoming Raft messages.
    pub fn drain_installed_snapshots(&mut self) -> Vec<RaftApplicationSnapshot> {
        mem::take(&mut self.installed_snapshots)
    }

    /// Returns the durable application snapshot, if this node has one.
    pub fn application_snapshot(&self) -> raft::Result<Option<RaftApplicationSnapshot>> {
        Ok(self
            .storage
            .application_snapshot()?
            .map(application_snapshot))
    }

    /// Persists application snapshot bytes and compacts entries through `index`.
    ///
    /// The caller must only use an index whose command has been successfully
    /// applied to the application state machine. The in-memory backend only
    /// accepts a checkpoint at its durable log tip; persistent nodes support
    /// compaction while retaining a later Raft log suffix.
    pub fn checkpoint(&mut self, index: u64, data: Vec<u8>) -> raft::Result<()> {
        self.storage.create_snapshot(index, data)
    }

    pub(crate) fn defer_application_acknowledgement(&mut self) {
        self.auto_acknowledge_apply = false;
    }

    pub(crate) fn acknowledge_all_committed(&mut self) -> raft::Result<()> {
        self.acknowledge_committed();
        self.drive_ready()
    }

    pub(crate) fn acknowledge_recovered(&mut self, index: u64) -> raft::Result<()> {
        if index == 0 {
            return Ok(());
        }
        if index > self.storage.initial_state()?.hard_state.commit {
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }
        self.raw.advance_apply_to(index);
        self.last_acknowledged_index = self.last_acknowledged_index.max(index);
        self.last_committed_index = self.last_committed_index.max(index);
        self.drive_ready()
    }

    /// Returns the durable, uncompacted normal entries at or below the Raft
    /// commit index without marking them as newly applied.
    ///
    /// This is intended for application recovery. Callers must keep their own
    /// applied-index checkpoint to avoid applying an already materialized
    /// business command twice.
    pub fn persisted_committed_entries(&self) -> raft::Result<Vec<RaftCommittedEntry>> {
        Ok(committed_entries(self.storage.committed_entries()?))
    }

    fn drive_ready(&mut self) -> raft::Result<()> {
        while self.raw.has_ready() {
            let mut ready = self.raw.ready();
            self.outbox.extend(ready.take_messages());
            let snapshot = (!ready.snapshot().is_empty()).then(|| ready.snapshot().clone());

            self.storage
                .persist(snapshot.as_ref(), ready.entries(), ready.hs())?;
            if let Some(snapshot) = snapshot {
                self.last_committed_index =
                    self.last_committed_index.max(snapshot.get_metadata().index);
                self.installed_snapshots
                    .push(application_snapshot(snapshot));
            }
            self.outbox.extend(ready.take_persisted_messages());
            self.record_committed(ready.take_committed_entries());

            let mut light_ready = self.raw.advance_append(ready);
            if let Some(commit) = light_ready.commit_index() {
                self.storage.persist_commit(commit)?;
            }
            self.outbox.extend(light_ready.take_messages());
            self.record_committed(light_ready.take_committed_entries());
            if self.auto_acknowledge_apply {
                self.acknowledge_committed();
            }
        }
        self.publish_coordinator_state();
        Ok(())
    }

    fn record_committed(&mut self, entries: Vec<Entry>) {
        if let Some(entry) = entries.last() {
            self.last_committed_index = self.last_committed_index.max(entry.index);
        }
        self.committed.extend(committed_entries(entries));
    }

    fn acknowledge_committed(&mut self) {
        if self.last_acknowledged_index < self.last_committed_index {
            self.raw.advance_apply_to(self.last_committed_index);
            self.last_acknowledged_index = self.last_committed_index;
        }
    }

    fn publish_coordinator_state(&self) {
        let leader_id = (self.raw.raft.leader_id != 0).then_some(self.raw.raft.leader_id);
        if self.coordinator.inner.state.load().leader_id != leader_id {
            self.coordinator
                .inner
                .state
                .store(Arc::new(RaftCoordinatorState { leader_id }));
            self.coordinator.inner.changed.notify_waiters();
        }
    }
}

fn application_snapshot(snapshot: raft::eraftpb::Snapshot) -> RaftApplicationSnapshot {
    RaftApplicationSnapshot {
        index: snapshot.get_metadata().index,
        data: snapshot.data.to_vec(),
    }
}

fn committed_entries(entries: impl IntoIterator<Item = Entry>) -> Vec<RaftCommittedEntry> {
    entries
        .into_iter()
        .filter_map(|entry| {
            (entry.get_entry_type() == EntryType::EntryNormal && !entry.data.is_empty()).then(
                || RaftCommittedEntry {
                    index: entry.index,
                    data: entry.data.to_vec(),
                },
            )
        })
        .collect()
}

fn conf_state(members: &[RaftMember]) -> ConfState {
    ConfState::from((
        members.iter().map(RaftMember::id).collect::<Vec<_>>(),
        Vec::new(),
    ))
}

fn validate_members(
    id: u64,
    endpoint: &Arc<str>,
    members: &[RaftMember],
) -> Result<(), RaftNodeError> {
    if members.is_empty() {
        return Err(RaftNodeError::EmptyMembers);
    }
    let mut ids = HashSet::with_capacity(members.len());
    for member in members {
        if member.id == 0 {
            return Err(RaftNodeError::ZeroMemberId);
        }
        if !ids.insert(member.id) {
            return Err(RaftNodeError::DuplicateMemberId(member.id));
        }
    }
    let Some(local) = members.iter().find(|member| member.id == id) else {
        return Err(RaftNodeError::LocalMemberMissing(id));
    };
    if local.endpoint.as_ref() != endpoint.as_ref() {
        return Err(RaftNodeError::LocalEndpointMismatch {
            member: Arc::clone(&local.endpoint),
            local: Arc::clone(endpoint),
        });
    }
    Ok(())
}
