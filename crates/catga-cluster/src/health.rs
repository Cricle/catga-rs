//! Immutable cluster readiness snapshots.

use std::sync::Arc;

use crate::ClusterCoordinator;

/// A compact, point-in-time cluster health report.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterHealth {
    node_id: Box<str>,
    is_leader: bool,
    leader_endpoint: Option<Arc<str>>,
    cluster_size: usize,
}

impl ClusterHealth {
    /// Returns whether any leader is known at the snapshot point.
    pub const fn has_leader(&self) -> bool {
        self.leader_endpoint.is_some()
    }
    /// Returns whether this node was leader at the snapshot point.
    pub const fn is_leader(&self) -> bool {
        self.is_leader
    }
    /// Returns the known leader endpoint, if elected.
    pub fn leader_endpoint(&self) -> Option<&str> {
        self.leader_endpoint.as_deref()
    }
    /// Returns the member count at the snapshot point.
    pub const fn cluster_size(&self) -> usize {
        self.cluster_size
    }
    /// Returns the reporting node identifier.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }
}

/// Captures cluster readiness without polling or locking coordinator state.
pub fn cluster_health<C: ClusterCoordinator + ?Sized>(coordinator: &C) -> ClusterHealth {
    let endpoints = coordinator.member_endpoints();
    ClusterHealth {
        node_id: coordinator.node_id().into(),
        is_leader: coordinator.is_leader(),
        leader_endpoint: coordinator.leader_endpoint(),
        cluster_size: endpoints.len(),
    }
}
