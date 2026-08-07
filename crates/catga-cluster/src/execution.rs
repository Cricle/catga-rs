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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClusterCoordinator, LeadershipSnapshot, LeadershipSubscription};
    use std::sync::Arc;
    use tokio::time::{Duration, sleep};

    // Mock ClusterCoordinator for testing
    struct MockCoordinator {
        node_id: Box<str>,
        is_leader_flag: bool,
        leader_endpoint: Option<Arc<str>>,
        leadership_changed_tx: tokio::sync::watch::Sender<bool>,
        member_endpoints: Arc<[Arc<str>]>,
    }

    impl MockCoordinator {
        fn new(node_id: &str, is_leader: bool) -> Self {
            let (tx, _rx) = tokio::sync::watch::channel(is_leader);
            Self {
                node_id: node_id.into(),
                is_leader_flag: is_leader,
                leader_endpoint: if is_leader {
                    Some(Arc::from(format!("http://cluster/{}", node_id)))
                } else {
                    None
                },
                leadership_changed_tx: tx,
                member_endpoints: Arc::new([
                    Arc::from("http://cluster/node1"),
                    Arc::from("http://cluster/node2"),
                ]),
            }
        }
    }

    impl ClusterCoordinator for MockCoordinator {
        fn node_id(&self) -> &str {
            &self.node_id
        }
        fn is_leader(&self) -> bool {
            self.is_leader_flag
        }
        fn leader_endpoint(&self) -> Option<Arc<str>> {
            self.leader_endpoint.clone()
        }
        fn leadership_snapshot(&self) -> Arc<LeadershipSnapshot> {
            Arc::new(LeadershipSnapshot {
                epoch: 0,
                leader_node_id: self.leader_endpoint.clone(),
                leader_endpoint: self.leader_endpoint.clone(),
            })
        }
        fn subscribe_leadership(&self) -> LeadershipSubscription {
            todo!("not needed for tests")
        }
        fn member_endpoints(&self) -> Arc<[Arc<str>]> {
            Arc::clone(&self.member_endpoints)
        }
        async fn wait_for_leadership(&self, timeout: Duration) -> bool {
            if self.is_leader_flag {
                return true;
            }
            sleep(timeout).await;
            self.is_leader_flag
        }
        async fn wait_for_leadership_change(&self, was_leader: bool) -> bool {
            if self.is_leader_flag != was_leader {
                return self.is_leader_flag;
            }
            let mut rx = self.leadership_changed_tx.subscribe();
            rx.changed().await.ok();
            self.is_leader_flag
        }
    }

    // ===== execute_if_leader tests =====

    #[tokio::test]
    async fn execute_if_leader_returns_value_when_leader() {
        let coordinator = MockCoordinator::new("node1", true);
        let result = coordinator.execute_if_leader(|| async { 42 }).await;
        assert_eq!(result, Some(42));
    }

    #[tokio::test]
    async fn execute_if_leader_returns_none_when_not_leader() {
        let coordinator = MockCoordinator::new("node2", false);
        let result = coordinator.execute_if_leader(|| async { 42 }).await;
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn execute_if_leader_returns_value_from_action() {
        let coordinator = MockCoordinator::new("node1", true);
        let result = coordinator
            .execute_if_leader(|| async { "hello world".to_string() })
            .await;
        assert_eq!(result, Some("hello world".to_string()));
    }

    #[tokio::test]
    async fn execute_if_leader_action_always_called_when_leader() {
        let coordinator = MockCoordinator::new("node1", true);
        let result = coordinator.execute_if_leader(|| async { 42 }).await;
        assert_eq!(result, Some(42));
    }

    #[tokio::test]
    async fn execute_if_leader_action_never_called_when_not_leader() {
        let coordinator = MockCoordinator::new("node2", false);
        let result = coordinator
            .execute_if_leader(|| async { panic!("should not be called") })
            .await;
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn execute_if_leader_with_send_future() {
        let coordinator = MockCoordinator::new("node1", true);
        async fn async_fn() -> i32 {
            100
        }
        let result = coordinator.execute_if_leader(async_fn).await;
        assert_eq!(result, Some(100));
    }

    // ===== execute_if_leader_cancellable tests =====

    #[tokio::test]
    async fn cancellable_returns_unavailable_when_not_leader() {
        let coordinator = MockCoordinator::new("node2", false);
        let result = coordinator
            .execute_if_leader_cancellable(|_token| async { Ok(42) })
            .await;
        assert!(result.is_err());
        let err = result.expect_err("error expected");
        assert_eq!(err.code(), ErrorCode::Unavailable);
    }

    #[tokio::test]
    async fn cancellable_returns_action_result_when_leader() {
        let coordinator = MockCoordinator::new("node1", true);
        let result = coordinator
            .execute_if_leader_cancellable(|_token| async { Ok(42) })
            .await;
        assert!(result.is_ok());
        assert_eq!(result.expect("ok result expected"), 42);
    }

    #[tokio::test]
    async fn cancellable_returns_error_from_action() {
        let coordinator = MockCoordinator::new("node1", true);
        let result: CatgaResult<i32> = coordinator
            .execute_if_leader_cancellable(|_token| async {
                Err(CatgaError::new(ErrorCode::Internal, "test error"))
            })
            .await;
        assert!(result.is_err());
        let err = result.expect_err("error expected");
        assert_eq!(err.code(), ErrorCode::Internal);
    }

    #[tokio::test]
    async fn cancellable_action_receives_token() {
        let coordinator = MockCoordinator::new("node1", true);
        let result = coordinator
            .execute_if_leader_cancellable(|token| async move {
                // Verify token is not cancelled at start
                assert!(!token.is_cancelled());
                Ok::<_, CatgaError>(42)
            })
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn cancellable_with_complex_result() {
        let coordinator = MockCoordinator::new("node1", true);
        let result = coordinator
            .execute_if_leader_cancellable(|_token| async {
                Ok::<_, CatgaError>("complex result".to_string())
            })
            .await;
        assert!(result.is_ok());
        assert_eq!(result.expect("ok result expected"), "complex result");
    }

    // Note: trait object tests omitted - ClusterCoordinator is not dyn compatible
    // because wait_for_leadership and wait_for_leadership_change return impl Trait
}
