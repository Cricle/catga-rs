//! Leader-gated execution shared by every cluster coordinator implementation.

use std::future::Future;

use crate::ClusterCoordinator;

/// Adds leader-gated execution to a [`ClusterCoordinator`].
///
/// `Some` contains the action result when this node was leader at the execution
/// boundary; `None` means the caller must retry or route the work to the leader.
pub trait ClusterCoordinatorExt: ClusterCoordinator {
    /// Executes `action` only when this node currently owns leadership.
    fn execute_if_leader<T, F, Fut>(&self, action: F) -> impl Future<Output = Option<T>> + Send
    where
        T: Send,
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = T> + Send;
}

impl<C> ClusterCoordinatorExt for C
where
    C: ClusterCoordinator + ?Sized,
{
    async fn execute_if_leader<T, F, Fut>(&self, action: F) -> Option<T>
    where
        T: Send,
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = T> + Send,
    {
        if self.is_leader() {
            Some(action().await)
        } else {
            None
        }
    }
}
