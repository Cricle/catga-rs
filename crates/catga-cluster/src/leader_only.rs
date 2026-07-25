//! Pipeline behavior for commands that must execute on the elected leader.

use std::sync::Arc;

use async_trait::async_trait;
use catga_core::{Behavior, CatgaError, CatgaResult, ErrorCode, Next, Request};

use crate::ClusterCoordinator;

/// Marker trait for request types intended for leader-only pipelines.
pub trait LeaderOnlyCommand: Request {}

/// Rejects a request before dispatch when its node is not the elected leader.
pub struct LeaderOnlyBehavior<C: ?Sized> {
    coordinator: Arc<C>,
}

impl<C: ?Sized> LeaderOnlyBehavior<C> {
    /// Creates a behavior backed by one cluster coordinator.
    pub fn new(coordinator: Arc<C>) -> Self {
        Self { coordinator }
    }
}

#[async_trait]
impl<M, C> Behavior<M> for LeaderOnlyBehavior<C>
where
    M: Request,
    C: ClusterCoordinator + ?Sized + 'static,
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
