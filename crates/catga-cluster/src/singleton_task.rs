//! Leadership-scoped background task execution.

use std::{future::Future, sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;

use crate::ClusterCoordinator;

/// Runs one cancellation-aware background task only while this node leads.
///
/// The runner reacts to coordinator notifications rather than polling. A task
/// receives a fresh cancellation token on every leadership epoch and must end
/// promptly after that token is cancelled.
pub struct SingletonTaskRunner<C: ?Sized> {
    coordinator: Arc<C>,
    restart_delay: Duration,
}

impl<C: ClusterCoordinator + ?Sized> SingletonTaskRunner<C> {
    /// Creates a runner with a one-second delay before restarting completed work.
    pub fn new(coordinator: Arc<C>) -> Self {
        Self::with_restart_delay(coordinator, Duration::from_secs(1))
    }

    /// Creates a runner with an explicit delay before restarting completed work.
    pub fn with_restart_delay(coordinator: Arc<C>, restart_delay: Duration) -> Self {
        Self {
            coordinator,
            restart_delay,
        }
    }

    /// Runs `task` whenever this node is leader until `shutdown` is cancelled.
    ///
    /// A leadership loss cancels the task token and waits for that invocation
    /// to finish before a later leadership epoch can start a new invocation.
    pub async fn run<F, Fut>(&self, shutdown: CancellationToken, mut task: F)
    where
        F: FnMut(CancellationToken) -> Fut,
        Fut: Future<Output = ()> + Send,
    {
        while !shutdown.is_cancelled() {
            if !self.coordinator.is_leader() {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = self.coordinator.wait_for_leadership_change(false) => {}
                }
                continue;
            }

            let leadership_lost = CancellationToken::new();
            let work = task(leadership_lost.clone());
            tokio::pin!(work);
            tokio::select! {
                _ = shutdown.cancelled() => {
                    leadership_lost.cancel();
                    work.await;
                    break;
                }
                _ = self.coordinator.wait_for_leadership_change(true) => {
                    leadership_lost.cancel();
                    work.await;
                }
                _ = &mut work => {
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        _ = self.coordinator.wait_for_leadership_change(true) => {}
                        _ = tokio::time::sleep(self.restart_delay) => {}
                    }
                }
            }
        }
    }
}
