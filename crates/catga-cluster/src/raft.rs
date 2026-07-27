//! A single-owner `raft-rs` driver with a lock-free coordinator view.

use std::{
    collections::{HashSet, VecDeque},
    error::Error,
    fmt, mem,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use raft::{
    Config, RawNode, Storage,
    eraftpb::{ConfState, Entry, EntryType, Message},
};
use slog::Logger;
use tokio::sync::Notify;

use crate::{
    ClusterCoordinator, CoordinatorStateStore, LeadershipPublication, LeadershipSnapshot,
    LeadershipSnapshotSource, LeadershipSnapshotState, RaftTiming, metrics::RaftMetrics,
    storage::RaftStorage,
};

const DEFAULT_PENDING_COMMIT_CAPACITY: usize = 1_024;

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

/// One bounded page of persisted committed application commands.
///
/// `next_index` advances over every Raft log entry in the page, including
/// protocol entries that do not contain application data. A caller must keep
/// reading while it is `Some` even when `entries` is empty.
pub(crate) struct PersistedCommittedPage {
    pub(crate) entries: Vec<RaftCommittedEntry>,
    pub(crate) next_index: Option<u64>,
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
    /// The configured number of retained, unapplied application commands was zero.
    ZeroPendingCommitCapacity,
    /// Accepting another application proposal would exceed the bounded pending-commit queue.
    PendingCommitCapacity {
        /// Maximum number of committed application commands retained for the caller.
        capacity: usize,
    },
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
            Self::ZeroPendingCommitCapacity => {
                formatter.write_str("Raft pending application commit capacity must be non-zero")
            }
            Self::PendingCommitCapacity { capacity } => write!(
                formatter,
                "Raft pending application commit capacity of {capacity} has been reached"
            ),
        }
    }
}

impl Error for RaftNodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Raft(error) => Some(error),
            Self::RaftEngine(error) => Some(error),
            Self::EmptyMembers
            | Self::ZeroMemberId
            | Self::DuplicateMemberId(_)
            | Self::LocalMemberMissing(_)
            | Self::LocalEndpointMismatch { .. }
            | Self::PersistedConfStateMismatch
            | Self::ZeroPendingCommitCapacity
            | Self::PendingCommitCapacity { .. } => None,
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
    snapshot: Arc<LeadershipSnapshot>,
}

impl LeadershipSnapshotState for RaftCoordinatorState {
    fn leadership_snapshot(&self) -> Arc<LeadershipSnapshot> {
        Arc::clone(&self.snapshot)
    }
}

struct RaftCoordinatorInner {
    state: Arc<CoordinatorStateStore<RaftCoordinatorState>>,
    publication: Arc<LeadershipPublication>,
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

    fn leadership_snapshot(&self) -> Arc<LeadershipSnapshot> {
        self.inner.state.load().leadership_snapshot()
    }

    fn subscribe_leadership(&self) -> crate::LeadershipSubscription {
        self.inner.publication.subscribe()
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
        let notified = self.inner.changed.notified();
        if self.is_leader() != was_leader {
            return self.is_leader();
        }
        notified.await;
        self.is_leader()
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
    committed: VecDeque<RaftCommittedEntry>,
    pending_commit_capacity: usize,
    next_unqueued_commit_index: u64,
    installed_snapshots: Vec<RaftApplicationSnapshot>,
    metrics: RaftMetrics,
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
        Self::new_with_timing_and_pending_commit_capacity(
            id,
            endpoint,
            members,
            RaftTiming::default_node(),
            DEFAULT_PENDING_COMMIT_CAPACITY,
        )
    }

    /// Builds an in-memory node with a bounded queue for committed application commands.
    pub fn new_with_pending_commit_capacity(
        id: u64,
        endpoint: impl Into<Arc<str>>,
        members: Vec<RaftMember>,
        pending_commit_capacity: usize,
    ) -> Result<Self, RaftNodeError> {
        Self::new_with_timing_and_pending_commit_capacity(
            id,
            endpoint,
            members,
            RaftTiming::default_node(),
            pending_commit_capacity,
        )
    }

    /// Builds an in-memory Raft node using validated logical timing.
    pub fn new_with_timing(
        id: u64,
        endpoint: impl Into<Arc<str>>,
        members: Vec<RaftMember>,
        timing: RaftTiming,
    ) -> Result<Self, RaftNodeError> {
        Self::new_with_timing_and_pending_commit_capacity(
            id,
            endpoint,
            members,
            timing,
            DEFAULT_PENDING_COMMIT_CAPACITY,
        )
    }

    /// Builds an in-memory node using validated timing and a bounded commit queue.
    pub fn new_with_timing_and_pending_commit_capacity(
        id: u64,
        endpoint: impl Into<Arc<str>>,
        members: Vec<RaftMember>,
        timing: RaftTiming,
        pending_commit_capacity: usize,
    ) -> Result<Self, RaftNodeError> {
        let endpoint = endpoint.into();
        validate_members(id, &endpoint, &members)?;
        let storage = RaftStorage::in_memory(conf_state(&members));
        Self::from_storage(
            id,
            endpoint,
            members,
            storage,
            timing,
            pending_commit_capacity,
        )
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
        Self::from_storage(
            id,
            endpoint,
            members,
            storage,
            timing,
            DEFAULT_PENDING_COMMIT_CAPACITY,
        )
    }

    fn from_storage(
        id: u64,
        _endpoint: Arc<str>,
        members: Vec<RaftMember>,
        storage: RaftStorage,
        timing: RaftTiming,
        pending_commit_capacity: usize,
    ) -> Result<Self, RaftNodeError> {
        if pending_commit_capacity == 0 {
            return Err(RaftNodeError::ZeroPendingCommitCapacity);
        }
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
        let snapshot = Arc::new(LeadershipSnapshot {
            epoch: 0,
            leader_node_id: None,
            leader_endpoint: None,
        });
        let state = Arc::new(CoordinatorStateStore::new(RaftCoordinatorState {
            leader_id: None,
            snapshot,
        }));
        let source: Arc<dyn LeadershipSnapshotSource> = state.clone();
        let publication = LeadershipPublication::new(source);
        let coordinator = Arc::new(RaftClusterNode {
            inner: Arc::new(RaftCoordinatorInner {
                state,
                publication,
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
        let mut node = Self {
            raw,
            storage,
            coordinator,
            outbox: Vec::new(),
            committed: VecDeque::new(),
            pending_commit_capacity,
            next_unqueued_commit_index: 1,
            installed_snapshots: Vec::new(),
            metrics: RaftMetrics::default(),
            auto_acknowledge_apply: true,
            last_acknowledged_index: 0,
            last_committed_index: 0,
        };
        node.publish_metrics();
        Ok(node)
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
        self.try_propose(data)
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))
    }

    /// Proposes one application command unless the bounded pending-commit queue is full.
    ///
    /// Callers that need to distinguish Raft protocol failures from application backpressure
    /// should use this method instead of [`Self::propose`].
    pub fn try_propose(&mut self, data: impl Into<Vec<u8>>) -> Result<(), RaftNodeError> {
        if let Err(error) = self.refill_committed() {
            self.metrics.record_failure("proposal");
            return Err(error);
        }
        if self.committed.len() >= self.pending_commit_capacity {
            self.metrics.record_failure("proposal");
            return Err(RaftNodeError::PendingCommitCapacity {
                capacity: self.pending_commit_capacity,
            });
        }
        if let Err(error) = self.raw.propose(Vec::new(), data.into()) {
            self.metrics.record_failure("proposal");
            return Err(error.into());
        }
        if let Err(error) = self.drive_ready() {
            self.metrics.record_failure("proposal");
            return Err(error.into());
        }
        Ok(())
    }

    /// Takes outbound Raft protocol messages for delivery by the caller's transport.
    pub fn drain_messages(&mut self) -> Vec<RaftMessage> {
        mem::take(&mut self.outbox)
    }

    /// Takes committed non-empty normal entries for application to business state.
    pub fn drain_committed(&mut self) -> Vec<RaftCommittedEntry> {
        let entries = self.committed.drain(..).collect();
        self.publish_metrics();
        entries
    }

    /// Takes at most the configured pending-commit capacity of application commands.
    ///
    /// Commands beyond the in-memory page remain in the durable Raft log until a
    /// later call. This makes repeated calls suitable for consumers that require
    /// a fixed memory bound even when a peer commits a large batch at once.
    pub fn try_drain_committed(&mut self) -> Result<Vec<RaftCommittedEntry>, RaftNodeError> {
        self.refill_committed()?;
        Ok(self.drain_committed())
    }

    /// Returns and removes one committed application command, if one is pending.
    pub fn next_committed(&mut self) -> Option<RaftCommittedEntry> {
        let entry = self.committed.pop_front();
        self.publish_metrics();
        entry
    }

    /// Returns and removes one committed application command after refilling the
    /// bounded in-memory page from durable Raft storage when needed.
    pub fn try_next_committed(&mut self) -> Result<Option<RaftCommittedEntry>, RaftNodeError> {
        self.refill_committed()?;
        Ok(self.next_committed())
    }

    /// Returns the number of bounded, unapplied application commands retained locally.
    pub fn pending_commit_count(&self) -> usize {
        self.committed.len()
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

    pub(crate) fn acknowledge_applied_through(&mut self, index: u64) -> raft::Result<()> {
        if self.last_acknowledged_index < index {
            self.raw.advance_apply_to(index);
            self.last_acknowledged_index = index;
        }
        self.drive_ready()?;
        self.publish_metrics();
        Ok(())
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

    /// Reads at most `max_entries` durable log entries for recovery.
    ///
    /// The returned application commands exclude empty Raft protocol entries.
    /// `next_index` nevertheless advances past those entries, allowing a
    /// recovery loop to retain a fixed memory bound even across configuration
    /// changes or no-op entries.
    pub(crate) fn persisted_committed_page(
        &self,
        start_index: u64,
        max_entries: usize,
    ) -> raft::Result<PersistedCommittedPage> {
        let (entries, next_index) = self
            .storage
            .committed_entries_page(start_index, max_entries)?;
        Ok(PersistedCommittedPage {
            entries: committed_entries(entries),
            next_index,
        })
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
            self.refill_committed()
                .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?;
            if self.auto_acknowledge_apply {
                self.acknowledge_committed();
            }
        }
        self.publish_coordinator_state();
        self.publish_metrics();
        Ok(())
    }

    fn record_committed(&mut self, entries: Vec<Entry>) {
        if let Some(entry) = entries.last() {
            self.last_committed_index = self.last_committed_index.max(entry.index);
        }
        for entry in entries {
            if entry.index < self.next_unqueued_commit_index {
                continue;
            }
            if entry.get_entry_type() == EntryType::EntryNormal
                && !entry.data.is_empty()
                && self.committed.len() == self.pending_commit_capacity
            {
                return;
            }
            self.next_unqueued_commit_index = entry.index.saturating_add(1);
            if entry.get_entry_type() == EntryType::EntryNormal && !entry.data.is_empty() {
                self.committed.push_back(RaftCommittedEntry {
                    index: entry.index,
                    data: entry.data.to_vec(),
                });
            }
        }
    }

    fn refill_committed(&mut self) -> Result<(), RaftNodeError> {
        while self.committed.len() < self.pending_commit_capacity
            && self.next_unqueued_commit_index <= self.last_committed_index
        {
            let available = self.pending_commit_capacity - self.committed.len();
            let (entries, next_index) = self
                .storage
                .committed_entries_page(self.next_unqueued_commit_index, available)?;
            if entries.is_empty() {
                self.next_unqueued_commit_index = self.last_committed_index.saturating_add(1);
                break;
            }
            self.record_committed(entries);
            if next_index.is_none()
                && self.next_unqueued_commit_index <= self.last_committed_index
                && self.committed.len() < self.pending_commit_capacity
            {
                self.next_unqueued_commit_index = self.last_committed_index.saturating_add(1);
            }
        }
        Ok(())
    }

    fn acknowledge_committed(&mut self) {
        if self.last_acknowledged_index < self.last_committed_index {
            self.raw.advance_apply_to(self.last_committed_index);
            self.last_acknowledged_index = self.last_committed_index;
        }
    }

    fn publish_coordinator_state(&self) {
        let _publication = self.coordinator.inner.publication.publication.lock();
        let leader_id = (self.raw.raft.leader_id != 0).then_some(self.raw.raft.leader_id);
        let current = self.coordinator.inner.state.load_full();
        if current.leader_id != leader_id {
            let leader_node_id = leader_id.map(|id| Arc::<str>::from(id.to_string()));
            let leader_endpoint = leader_id.and_then(|id| {
                self.coordinator
                    .members
                    .iter()
                    .find(|member| member.id == id)
                    .map(|member| Arc::clone(&member.endpoint))
            });
            let snapshot = Arc::new(LeadershipSnapshot {
                epoch: current.snapshot.epoch.saturating_add(1),
                leader_node_id,
                leader_endpoint,
            });
            self.coordinator
                .inner
                .state
                .store(Arc::new(RaftCoordinatorState {
                    leader_id,
                    snapshot: Arc::clone(&snapshot),
                }));
            self.coordinator.inner.publication.publish_locked(snapshot);
            self.coordinator.inner.changed.notify_waiters();
        }
    }

    fn publish_metrics(&mut self) {
        let leader_id = (self.raw.raft.leader_id != 0).then_some(self.raw.raft.leader_id);
        self.metrics.record_state(
            self.raw.raft.state,
            leader_id,
            self.raw.raft.term,
            self.raw.raft.raft_log.committed,
            self.raw.raft.raft_log.applied,
            self.committed.len(),
        );
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
