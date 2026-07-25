#![forbid(unsafe_code)]
//! Lock-free cluster coordination contracts and deterministic in-memory implementations.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use tokio::sync::Notify;

mod config;
mod execution;
mod forward;
mod health;
mod leader_only;
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
pub use leader_only::{LeaderOnlyBehavior, LeaderOnlyCommand};
pub use raft::{
    RaftApplicationSnapshot, RaftClusterNode, RaftCommittedEntry, RaftMember, RaftMessage,
    RaftNode, RaftNodeError,
};
pub use runtime::{RaftRuntime, RaftRuntimeError, RaftTransport, RaftTransportResult};
pub use singleton_task::SingletonTaskRunner;
pub use state_machine::{RaftStateMachine, RaftStateMachineDriver, RaftStateMachineError};
pub use state_machine_runtime::{RaftStateMachineRuntime, RaftStateMachineRuntimeError};

/// Read-only cluster-coordination operations available to an individual node.
pub trait ClusterCoordinator: Send + Sync {
    /// Returns this node's stable identifier.
    fn node_id(&self) -> &str;
    /// Returns whether this node currently owns leadership.
    fn is_leader(&self) -> bool;
    /// Returns the endpoint of the elected leader when it is known.
    fn leader_endpoint(&self) -> Option<Arc<str>>;
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
    topology: ArcSwap<Topology>,
    changed: Notify,
}

#[derive(Clone)]
struct Topology {
    leader: Arc<str>,
    endpoints: Arc<[Arc<str>]>,
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
        Self {
            inner: Arc::new(MemoryClusterInner {
                topology: ArcSwap::from_pointee(Topology {
                    leader: Arc::from(leader.into()),
                    endpoints: endpoints
                        .into_iter()
                        .map(|endpoint| Arc::from(endpoint.into()))
                        .collect(),
                }),
                changed: Notify::new(),
            }),
        }
    }

    /// Returns a node view when `node_id` is present in the topology endpoints.
    pub fn node(&self, node_id: &str) -> Option<Arc<MemoryClusterNode>> {
        let topology = self.inner.topology.load();
        topology
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
        let current = self.inner.topology.load_full();
        if !current
            .endpoints
            .iter()
            .any(|endpoint| endpoint_node_id(endpoint) == leader)
        {
            return None;
        }
        if current.leader.as_ref() == leader {
            return Some(());
        }
        self.inner.topology.store(Arc::new(Topology {
            leader: leader.into(),
            endpoints: Arc::clone(&current.endpoints),
        }));
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
        self.inner.topology.load().leader.as_ref() == self.node_id()
    }

    fn leader_endpoint(&self) -> Option<Arc<str>> {
        let topology = self.inner.topology.load();
        topology
            .endpoints
            .iter()
            .find(|endpoint| endpoint_node_id(endpoint) == topology.leader.as_ref())
            .map(Arc::clone)
    }

    fn member_endpoints(&self) -> Arc<[Arc<str>]> {
        Arc::clone(&self.inner.topology.load().endpoints)
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
