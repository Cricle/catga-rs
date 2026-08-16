//! Backend-agnostic cluster utilities built on the consensus contracts.
//!
//! These pieces depend only on [`crate::ConsensusCoordinator`], so application code
//! can forward requests to a cluster leader without naming a concrete consensus
//! backend.

use async_trait::async_trait;

use crate::{CatgaResult, Request};

/// Sends a typed request to a known cluster leader.
#[async_trait]
pub trait ClusterForwarder<M: Request>: Send + Sync {
    /// Forwards the given request to the specified leader endpoint.
    async fn forward(&self, request: M, leader_endpoint: &str) -> CatgaResult<M::Response>;
}
