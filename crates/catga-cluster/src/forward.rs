//! Transport-neutral request forwarding to the elected leader.

use std::sync::Arc;

use async_trait::async_trait;
use catga_core::{Behavior, CatgaError, CatgaResult, ErrorCode, Next, Request};

use crate::ClusterCoordinator;

/// Sends a typed request to a known cluster leader.
#[async_trait]
pub trait ClusterForwarder<M: Request>: Send + Sync {
    /// Forwards `request` to `leader_endpoint` and returns its typed response.
    async fn forward(&self, request: M, leader_endpoint: &str) -> CatgaResult<M::Response>;
}

/// A pipeline behavior that executes locally on the leader and forwards otherwise.
pub struct ForwardToLeaderBehavior<C: ?Sized, F: ?Sized> {
    coordinator: Arc<C>,
    forwarder: Arc<F>,
}

impl<C: ?Sized, F: ?Sized> ForwardToLeaderBehavior<C, F> {
    /// Creates a leader-aware behavior backed by one coordinator and one transport.
    pub fn new(coordinator: Arc<C>, forwarder: Arc<F>) -> Self {
        Self {
            coordinator,
            forwarder,
        }
    }
}

#[async_trait]
impl<M, C, F> Behavior<M> for ForwardToLeaderBehavior<C, F>
where
    M: Request,
    C: ClusterCoordinator + ?Sized + 'static,
    F: ClusterForwarder<M> + ?Sized + 'static,
{
    async fn handle(&self, message: M, next: Next<M>) -> CatgaResult<M::Response> {
        if self.coordinator.is_leader() {
            return next.run(message).await;
        }
        let leader = self.coordinator.leader_endpoint().ok_or_else(|| {
            CatgaError::new(ErrorCode::Conflict, "no cluster leader is currently known")
        })?;
        self.forwarder.forward(message, &leader).await
    }
}
