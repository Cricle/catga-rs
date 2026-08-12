//! Leader-gated execution shared by every cluster coordinator implementation.

use std::future::Future;

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use tokio_util::sync::CancellationToken;

use crate::ClusterCoordinator;

/// Adds leader-gated execution to a [`ClusterCoordinator`].
///
/// [`Self::execute_if_leader`] returns an optional action result for simple leader-only work.
/// [`Self::execute_if_leader_cancellable`] returns structured errors and notifies active work
/// when this node loses leadership.
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

    /// Executes a cancellable action while this node owns leadership.
    ///
    /// The action receives a [`CancellationToken`] that is cancelled if leadership is lost.
    /// This method returns [`ErrorCode::Unavailable`] without invoking `action` when this node
    /// is not the leader, and [`ErrorCode::Cancelled`] when leadership is lost while the action
    /// is running.
    fn execute_if_leader_cancellable<T, F, Fut>(
        &self,
        action: F,
    ) -> impl Future<Output = CatgaResult<T>> + Send
    where
        T: Send,
        F: FnOnce(CancellationToken) -> Fut + Send,
        Fut: Future<Output = CatgaResult<T>> + Send;
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

    async fn execute_if_leader_cancellable<T, F, Fut>(&self, action: F) -> CatgaResult<T>
    where
        T: Send,
        F: FnOnce(CancellationToken) -> Fut + Send,
        Fut: Future<Output = CatgaResult<T>> + Send,
    {
        if !self.is_leader() {
            return Err(CatgaError::new(
                ErrorCode::Unavailable,
                "leadership is unavailable",
            ));
        }

        let leadership_lost = CancellationToken::new();
        let action = action(leadership_lost.clone());
        tokio::pin!(action);
        tokio::select! {
            result = &mut action => result,
            _ = self.wait_for_leadership_change(true) => {
                leadership_lost.cancel();
                std::future::poll_fn(|context| {
                    let _ = action.as_mut().poll(context);
                    std::task::Poll::Ready(())
                }).await;
                Err(CatgaError::new(ErrorCode::Cancelled, "leadership was lost"))
            }
        }
    }
}
