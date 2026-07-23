//! A single-owner `raft-rs` driver with a lock-free coordinator view.

use std::{
    collections::HashSet,
    error::Error,
    fmt, mem,
    sync::Arc,
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use raft::{
    Config, RawNode,
    eraftpb::{ConfState, Entry, EntryType, Message},
    storage::MemStorage,
};
use slog::Logger;
use tokio::sync::Notify;

use crate::ClusterCoordinator;

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
        }
    }
}

impl Error for RaftNodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Raft(error) => Some(error),
            _ => None,
        }
    }
}

impl From<raft::Error> for RaftNodeError {
    fn from(error: raft::Error) -> Self {
        Self::Raft(error)
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
}

/// Owns one `raft-rs` [`RawNode`] and must be driven by exactly one task or thread.
///
/// This explicit ownership is the serialization boundary for Raft mutations. The
/// [`RaftClusterNode`] returned by [`Self::coordinator`] is independently
/// shareable and reads its leadership state without taking a mutex.
pub struct RaftNode {
    raw: RawNode<MemStorage>,
    storage: MemStorage,
    coordinator: Arc<RaftClusterNode>,
    outbox: Vec<RaftMessage>,
    committed: Vec<RaftCommittedEntry>,
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
        let endpoint = endpoint.into();
        validate_members(id, &endpoint, &members)?;
        let members: Arc<[RaftMember]> = members.into();
        let voters = members.iter().map(RaftMember::id).collect::<Vec<_>>();
        let storage = MemStorage::new_with_conf_state(ConfState::from((voters, Vec::new())));
        let config = Config {
            id,
            election_tick: 10,
            heartbeat_tick: 1,
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

    fn drive_ready(&mut self) -> raft::Result<()> {
        while self.raw.has_ready() {
            let mut ready = self.raw.ready();
            self.outbox.extend(ready.take_messages());

            if !ready.snapshot().is_empty() {
                self.storage.wl().apply_snapshot(ready.snapshot().clone())?;
            }
            if !ready.entries().is_empty() {
                self.storage.wl().append(ready.entries())?;
            }
            if let Some(hard_state) = ready.hs() {
                self.storage.wl().set_hardstate(hard_state.clone());
            }
            self.outbox.extend(ready.take_persisted_messages());
            self.record_committed(ready.take_committed_entries());

            let mut light_ready = self.raw.advance(ready);
            if let Some(commit) = light_ready.commit_index() {
                self.storage.wl().mut_hard_state().set_commit(commit);
            }
            self.outbox.extend(light_ready.take_messages());
            self.record_committed(light_ready.take_committed_entries());
            self.raw.advance_apply();
        }
        self.publish_coordinator_state();
        Ok(())
    }

    fn record_committed(&mut self, entries: Vec<Entry>) {
        self.committed
            .extend(entries.into_iter().filter_map(|entry| {
                (entry.get_entry_type() == EntryType::EntryNormal && !entry.data.is_empty()).then(
                    || RaftCommittedEntry {
                        index: entry.index,
                        data: entry.data.to_vec(),
                    },
                )
            }));
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
