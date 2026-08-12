//! Transport-neutral request forwarding to the elected leader.

use std::sync::Arc;
use std::time::Duration;

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
    retry: Option<ForwardRetry>,
}

#[derive(Clone, Copy)]
struct ForwardRetry {
    max_attempts: usize,
    delay: Duration,
}

impl ForwardRetry {
    fn covers(error: &CatgaError) -> bool {
        matches!(error.code(), ErrorCode::Conflict | ErrorCode::Transient)
    }
}

fn unknown_leader_error() -> CatgaError {
    CatgaError::new(ErrorCode::Conflict, "no cluster leader is currently known")
}

impl<C: ?Sized, F: ?Sized> ForwardToLeaderBehavior<C, F> {
    /// Creates a leader-aware behavior backed by one coordinator and one transport.
    pub fn new(coordinator: Arc<C>, forwarder: Arc<F>) -> Self {
        Self {
            coordinator,
            forwarder,
            retry: None,
        }
    }

    /// Enables bounded forwarding retries while leadership settles after a failover.
    ///
    /// An attempt is retried only when no cluster leader is currently known or when the
    /// forward fails with [`ErrorCode::Conflict`] or [`ErrorCode::Transient`]; any other
    /// error and any locally executed (`next`) result is returned immediately. The
    /// behavior sleeps `delay` between attempts and performs at most `max_attempts`
    /// attempts in total (`0` is treated as `1`, matching the no-retry default).
    ///
    /// Retried requests must be idempotent: a forward that fails or times out may still
    /// have executed on the leader, so a later attempt can apply the request twice.
    pub fn with_retry(mut self, max_attempts: usize, delay: Duration) -> Self {
        self.retry = Some(ForwardRetry {
            max_attempts: max_attempts.max(1),
            delay,
        });
        self
    }
}

#[async_trait]
impl<M, C, F> Behavior<M> for ForwardToLeaderBehavior<C, F>
where
    M: Request + Clone,
    C: ClusterCoordinator + ?Sized + 'static,
    F: ClusterForwarder<M> + ?Sized + 'static,
{
    async fn handle(&self, message: M, next: Next<M>) -> CatgaResult<M::Response> {
        if self.coordinator.is_leader() {
            return next.run(message).await;
        }
        let Some(retry) = self.retry else {
            let leader = self
                .coordinator
                .leader_endpoint()
                .ok_or_else(unknown_leader_error)?;
            return self.forwarder.forward(message, &leader).await;
        };

        let mut attempt = 0_usize;
        loop {
            attempt += 1;
            let result = match self.coordinator.leader_endpoint() {
                Some(leader) => self.forwarder.forward(message.clone(), &leader).await,
                None => Err(unknown_leader_error()),
            };
            match result {
                Err(error) if attempt < retry.max_attempts && ForwardRetry::covers(&error) => {
                    tokio::time::sleep(retry.delay).await;
                }
                result => return result,
            }
        }
    }
}
