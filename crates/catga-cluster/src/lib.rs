#![forbid(unsafe_code)]
//! Lock-free cluster coordination contracts and deterministic in-memory implementations.
//!
//! The public contracts separate application decisions from Raft transport, persistence, and task
//! ownership. An application drives a [`RaftNode`] or [`RaftRuntime`], supplies a
//! [`RaftTransport`], and applies committed entries to its own state machine. This crate does not
//! create a network listener, select durable storage, or make leader-only work safe by itself.
//!
//! [`MemoryCluster`] is a deterministic, in-process topology for tests and single-process
//! composition. Its node views implement [`ClusterCoordinator`] and can exercise leader changes
//! without background networking:
//!
//! ```
//! use catga_cluster::{ClusterCoordinator, MemoryCluster};
//!
//! let cluster = MemoryCluster::new("one", ["http://cluster/one", "http://cluster/two"]);
//! let node = cluster.node("one").expect("configured member");
//! assert!(node.is_leader());
//! assert_eq!(node.leader_endpoint().as_deref(), Some("http://cluster/one"));
//! ```
//!
//! # Leadership and safety
//!
//! Leadership is an observation, not a distributed lock. Fence externally visible leader-owned
//! effects with the relevant Raft term, application version, or storage lease, and make them
//! idempotent across retries. [`LeadershipSubscription`] coalesces transitions for slow readers;
//! consumers must compare its epoch and resynchronize rather than assuming every intermediate
//! election was delivered. Shut down [`RaftRuntime`] before dropping application resources that
//! its workers can still access.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use parking_lot::Mutex;
use tokio::sync::{Notify, broadcast};

mod config;
mod execution;
mod forward;
mod health;
mod inbound;
mod leader_only;
mod metrics;
mod raft;
mod runtime;
mod singleton_task;
mod state_machine;
mod state_machine_runtime;
mod storage;

pub use config::{RaftClusterConfig, RaftClusterConfigError, RaftClusterMemberConfig, RaftTiming};
pub use execution::ClusterCoordinatorExt;
pub use forward::{ClusterForwarder, ForwardToLeaderBehavior};
pub use health::{ClusterHealth, cluster_health};
pub use inbound::{
    RaftInboundPolicy, RaftInboundPolicyError, RaftInboundRejection, RaftPeerIdentity,
    StaticRaftInboundPolicy,
};
pub use leader_only::{LeaderOnlyBehavior, LeaderOnlyCommand};
pub use raft::{
    RaftApplicationSnapshot, RaftClusterNode, RaftCommittedEntry, RaftMember, RaftMessage,
    RaftNode, RaftNodeError,
};
pub use runtime::{
    RaftRuntime, RaftRuntimeError, RaftTransport, RaftTransportError, RaftTransportResult,
};
pub use singleton_task::SingletonTaskRunner;
pub use state_machine::{RaftStateMachine, RaftStateMachineDriver, RaftStateMachineError};
pub use state_machine_runtime::{RaftStateMachineRuntime, RaftStateMachineRuntimeError};

/// The latest known elected-leader state.
///
/// A [`LeadershipSubscription`] captures this state on registration and then receives later
/// transitions through a bounded channel. Publication never waits for observers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeadershipSnapshot {
    /// Monotonically increasing version for leadership changes observed by this coordinator.
    pub epoch: u128,
    /// The elected leader's stable identifier when one is known.
    pub leader_node_id: Option<Arc<str>>,
    /// The elected leader's externally reachable endpoint when one is known.
    pub leader_endpoint: Option<Arc<str>>,
}

/// A bounded, non-blocking subscription to leadership transitions.
///
/// The initial [`LeadershipSnapshot`] is captured atomically with registration. Every subsequent
/// transition is delivered through a bounded broadcast channel. A slow subscriber is atomically
/// resynchronized to the latest snapshot before receiving later transitions. Consumers should
/// treat the epoch as a fence and ignore a repeated snapshot after recovery.
pub struct LeadershipSubscription {
    snapshot: Arc<LeadershipSnapshot>,
    receiver: broadcast::Receiver<Arc<LeadershipSnapshot>>,
    publication: std::sync::Weak<LeadershipPublication>,
}

impl LeadershipSubscription {
    /// Returns the snapshot captured when this subscription was registered.
    #[must_use]
    pub fn snapshot(&self) -> Arc<LeadershipSnapshot> {
        Arc::clone(&self.snapshot)
    }

    /// Waits for the next leadership transition after registration.
    ///
    /// When this subscriber falls behind the bounded buffer, this returns the newest snapshot and
    /// resumes at the current tail. Intermediate transitions are intentionally coalesced; callers
    /// should ignore a repeated epoch after recovery.
    pub async fn recv(&mut self) -> Result<Arc<LeadershipSnapshot>, broadcast::error::RecvError> {
        match self.receiver.recv().await {
            Err(broadcast::error::RecvError::Lagged(_)) => {
                let Some(publication) = self.publication.upgrade() else {
                    return Err(broadcast::error::RecvError::Closed);
                };
                let _publication = publication.publication.lock();
                self.receiver = publication.sender.subscribe();
                let snapshot = publication.snapshot();
                self.snapshot = Arc::clone(&snapshot);
                Ok(snapshot)
            }
            result => result,
        }
    }
}

/// Supplies the immutable leadership snapshot carried by a coordinator state.
pub(crate) trait LeadershipSnapshotSource: Send + Sync {
    fn leadership_snapshot(&self) -> Arc<LeadershipSnapshot>;
}

/// Wraps one backend's immutable coordinator state for lock-free reads.
pub(crate) struct CoordinatorStateStore<S> {
    state: ArcSwap<S>,
}

impl<S> CoordinatorStateStore<S> {
    pub(crate) fn new(state: S) -> Self {
        Self {
            state: ArcSwap::from_pointee(state),
        }
    }

    pub(crate) fn load(&self) -> arc_swap::Guard<Arc<S>> {
        self.state.load()
    }

    pub(crate) fn load_full(&self) -> Arc<S> {
        self.state.load_full()
    }

    pub(crate) fn store(&self, state: Arc<S>) {
        self.state.store(state);
    }
}

/// Defines how one immutable coordinator state exposes its leadership snapshot.
pub(crate) trait LeadershipSnapshotState {
    fn leadership_snapshot(&self) -> Arc<LeadershipSnapshot>;
}

impl<S> LeadershipSnapshotSource for CoordinatorStateStore<S>
where
    S: LeadershipSnapshotState + Send + Sync,
{
    fn leadership_snapshot(&self) -> Arc<LeadershipSnapshot> {
        self.load_full().leadership_snapshot()
    }
}

/// Shared bounded publication state for a coordinator's immutable read model.
pub(crate) struct LeadershipPublication {
    source: Arc<dyn LeadershipSnapshotSource>,
    sender: broadcast::Sender<Arc<LeadershipSnapshot>>,
    publication: Mutex<()>,
}

impl LeadershipPublication {
    pub(crate) fn new(source: Arc<dyn LeadershipSnapshotSource>) -> Arc<Self> {
        let (sender, _) = broadcast::channel(64);
        Arc::new(Self {
            source,
            sender,
            publication: Mutex::new(()),
        })
    }

    pub(crate) fn snapshot(&self) -> Arc<LeadershipSnapshot> {
        self.source.leadership_snapshot()
    }

    pub(crate) fn subscribe(self: &Arc<Self>) -> LeadershipSubscription {
        let _publication = self.publication.lock();
        let receiver = self.sender.subscribe();
        LeadershipSubscription {
            snapshot: self.snapshot(),
            receiver,
            publication: Arc::downgrade(self),
        }
    }

    pub(crate) fn publish_locked(&self, snapshot: Arc<LeadershipSnapshot>) {
        let _ = self.sender.send(snapshot);
    }
}

/// Read-only cluster-coordination operations available to an individual node.
pub trait ClusterCoordinator: Send + Sync {
    /// Returns this node's stable identifier.
    fn node_id(&self) -> &str;
    /// Returns whether this node currently owns leadership.
    fn is_leader(&self) -> bool;
    /// Returns the endpoint of the elected leader when it is known.
    fn leader_endpoint(&self) -> Option<Arc<str>>;
    /// Returns the latest known leadership state without registering a subscription.
    fn leadership_snapshot(&self) -> Arc<LeadershipSnapshot>;
    /// Subscribes to leadership transitions after capturing the current state.
    ///
    /// The returned subscription contains its initial snapshot. Later transitions use a bounded
    /// buffer, so slow subscribers receive the newest snapshot and may need to ignore a repeated
    /// epoch before subsequent transitions. Publication never waits for subscribers.
    fn subscribe_leadership(&self) -> LeadershipSubscription;
    /// Returns a compact snapshot of known member endpoints.
    fn member_endpoints(&self) -> Arc<[Arc<str>]>;
    /// Waits until this node is leader or the timeout expires.
    fn wait_for_leadership(
        &self,
        timeout: Duration,
    ) -> impl std::future::Future<Output = bool> + Send;
    /// Waits until an observed leadership transition occurs after this call begins.
    ///
    /// When this node has already changed leadership state, this returns immediately. Otherwise,
    /// it returns after the next published transition. The returned state can equal `was_leader`
    /// when another transition restores the original state before the waiter is polled; callers
    /// that fence leader-owned work must still treat that observed transition as a lost epoch.
    fn wait_for_leadership_change(
        &self,
        was_leader: bool,
    ) -> impl std::future::Future<Output = bool> + Send;
}

/// A process-local cluster used for deterministic tests and single-process deployments.
pub struct MemoryCluster {
    inner: Arc<MemoryClusterInner>,
}

struct MemoryClusterInner {
    state: Arc<CoordinatorStateStore<MemoryClusterState>>,
    publication: Arc<LeadershipPublication>,
    changed: Notify,
}

struct Topology {
    leader: Arc<str>,
    endpoints: Arc<[Arc<str>]>,
}

struct MemoryClusterState {
    topology: Topology,
    snapshot: Arc<LeadershipSnapshot>,
}

impl LeadershipSnapshotState for MemoryClusterState {
    fn leadership_snapshot(&self) -> Arc<LeadershipSnapshot> {
        Arc::clone(&self.snapshot)
    }
}

/// A node view over a [`MemoryCluster`] topology.
pub struct MemoryClusterNode {
    inner: Arc<MemoryClusterInner>,
    node_id: Box<str>,
}

impl MemoryCluster {
    /// Creates a cluster with `leader` initially elected.
    pub fn new<I, E>(leader: impl Into<Box<str>>, endpoints: I) -> Self
    where
        I: IntoIterator<Item = E>,
        E: Into<Box<str>>,
    {
        let leader = Arc::<str>::from(leader.into());
        let endpoints: Arc<[Arc<str>]> = endpoints
            .into_iter()
            .map(|endpoint| Arc::from(endpoint.into()))
            .collect();
        let leader_endpoint = endpoints
            .iter()
            .find(|endpoint| endpoint_node_id(endpoint) == leader.as_ref())
            .map(Arc::clone);
        let snapshot = Arc::new(LeadershipSnapshot {
            epoch: 0,
            leader_node_id: Some(Arc::clone(&leader)),
            leader_endpoint,
        });
        let state = Arc::new(CoordinatorStateStore::new(MemoryClusterState {
            topology: Topology { leader, endpoints },
            snapshot,
        }));
        let source: Arc<dyn LeadershipSnapshotSource> = state.clone();
        let publication = LeadershipPublication::new(source);
        Self {
            inner: Arc::new(MemoryClusterInner {
                state,
                publication,
                changed: Notify::new(),
            }),
        }
    }

    /// Returns a node view when `node_id` is present in the topology endpoints.
    pub fn node(&self, node_id: &str) -> Option<Arc<MemoryClusterNode>> {
        let state = self.inner.state.load();
        state
            .topology
            .endpoints
            .iter()
            .any(|endpoint| endpoint_node_id(endpoint) == node_id)
            .then(|| {
                Arc::new(MemoryClusterNode {
                    inner: Arc::clone(&self.inner),
                    node_id: node_id.into(),
                })
            })
    }

    /// Publishes an atomic leadership change for an existing member.
    pub fn elect(&self, leader: &str) -> Option<()> {
        let _publication = self.inner.publication.publication.lock();
        let current = self.inner.state.load_full();
        let leader_endpoint = current
            .topology
            .endpoints
            .iter()
            .find(|endpoint| endpoint_node_id(endpoint) == leader)
            .map(Arc::clone)?;
        if current.topology.leader.as_ref() == leader {
            return Some(());
        }
        let leader: Arc<str> = Arc::from(leader);
        let snapshot = Arc::new(LeadershipSnapshot {
            epoch: current.snapshot.epoch.saturating_add(1),
            leader_node_id: Some(Arc::clone(&leader)),
            leader_endpoint: Some(leader_endpoint),
        });
        self.inner.state.store(Arc::new(MemoryClusterState {
            topology: Topology {
                leader,
                endpoints: Arc::clone(&current.topology.endpoints),
            },
            snapshot: Arc::clone(&snapshot),
        }));
        self.inner.publication.publish_locked(snapshot);
        self.inner.changed.notify_waiters();
        Some(())
    }
}

impl MemoryClusterNode {
    /// Runs `action` only when this node is the current leader.
    pub async fn execute_if_leader<T, F, Fut>(&self, action: F) -> Option<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        if self.is_leader() {
            Some(action().await)
        } else {
            None
        }
    }
}

impl ClusterCoordinator for MemoryClusterNode {
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn is_leader(&self) -> bool {
        self.inner.state.load().topology.leader.as_ref() == self.node_id()
    }

    fn leader_endpoint(&self) -> Option<Arc<str>> {
        let state = self.inner.state.load();
        state
            .topology
            .endpoints
            .iter()
            .find(|endpoint| endpoint_node_id(endpoint) == state.topology.leader.as_ref())
            .map(Arc::clone)
    }

    fn leadership_snapshot(&self) -> Arc<LeadershipSnapshot> {
        self.inner.state.load().leadership_snapshot()
    }

    fn subscribe_leadership(&self) -> LeadershipSubscription {
        self.inner.publication.subscribe()
    }

    fn member_endpoints(&self) -> Arc<[Arc<str>]> {
        Arc::clone(&self.inner.state.load().topology.endpoints)
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

fn endpoint_node_id(endpoint: &str) -> &str {
    endpoint.rsplit('/').next().unwrap_or(endpoint)
}
