//! Backend-agnostic cluster utilities built on the consensus contracts.
//!
//! These pieces depend only on [`ConsensusCoordinator`], so application code
//! can gate leader-only pipelines and snapshot cluster readiness without
//! naming a concrete consensus backend.

use std::sync::Arc;

use async_trait::async_trait;

use crate::{Behavior, CatgaError, CatgaResult, ConsensusCoordinator, ErrorCode, Next, Request};

/// Marker trait for request types intended for leader-only pipelines.
pub trait LeaderOnlyCommand: Request {}

/// Rejects a request before dispatch when its node is not the elected leader.
pub struct LeaderOnlyBehavior<C: ?Sized> {
    coordinator: Arc<C>,
}

impl<C: ?Sized> LeaderOnlyBehavior<C> {
    /// Creates a behavior backed by one consensus coordinator.
    pub fn new(coordinator: Arc<C>) -> Self {
        Self { coordinator }
    }
}

#[async_trait]
impl<M, C> Behavior<M> for LeaderOnlyBehavior<C>
where
    M: Request,
    C: ConsensusCoordinator + ?Sized + 'static,
{
    async fn handle(&self, message: M, next: Next<M>) -> CatgaResult<M::Response> {
        if self.coordinator.is_leader() {
            return next.run(message).await;
        }
        let leader = self
            .coordinator
            .leader_endpoint()
            .unwrap_or_else(|| Arc::from("unknown"));
        Err(CatgaError::new(
            ErrorCode::Conflict,
            format!("request must execute on leader {leader}"),
        ))
    }
}

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
pub fn cluster_health<C: ConsensusCoordinator + ?Sized>(coordinator: &C) -> ClusterHealth {
    let endpoints = coordinator.member_endpoints();
    ClusterHealth {
        node_id: coordinator.node_id().into(),
        is_leader: coordinator.is_leader(),
        leader_endpoint: coordinator.leader_endpoint(),
        cluster_size: endpoints.len(),
    }
}
