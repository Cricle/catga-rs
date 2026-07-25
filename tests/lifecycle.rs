use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use catga_core::{
    AutoRecoveryOptions, CatgaError, CatgaResult, ErrorCode, RecoverableComponent, RecoveryManager,
    RecoveryResult, ShutdownCoordinator,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use catga_core::OperationTracker;

struct Recoverable {
    healthy: AtomicBool,
    calls: AtomicUsize,
}

struct FlakyRecoverable {
    healthy: AtomicBool,
    calls: AtomicUsize,
    failures_before_success: usize,
}

struct PanickingRecoverable;

struct BlockingRecoverable {
    started: Notify,
}

#[async_trait]
impl RecoverableComponent for BlockingRecoverable {
    fn name(&self) -> &str {
        "blocking"
    }

    fn is_healthy(&self) -> bool {
        false
    }

    async fn recover(&self) -> CatgaResult<()> {
        self.started.notify_waiters();
        std::future::pending().await
    }
}

#[async_trait]
impl RecoverableComponent for PanickingRecoverable {
    fn name(&self) -> &str {
        "panic"
    }

    fn is_healthy(&self) -> bool {
        false
    }

    async fn recover(&self) -> CatgaResult<()> {
        panic!("simulated component failure")
    }
}

#[async_trait]
impl RecoverableComponent for FlakyRecoverable {
    fn name(&self) -> &str {
        "flaky"
    }

    fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    async fn recover(&self) -> CatgaResult<()> {
        let attempt = self.calls.fetch_add(1, Ordering::AcqRel) + 1;
        if attempt <= self.failures_before_success {
            return Err(CatgaError::new(ErrorCode::Transient, "temporary failure"));
        }
        self.healthy.store(true, Ordering::Release);
        Ok(())
    }
}

#[async_trait]
impl RecoverableComponent for Recoverable {
    fn name(&self) -> &str {
        "test"
    }
    fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }
    async fn recover(&self) -> CatgaResult<()> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        self.healthy.store(true, Ordering::Release);
        Ok(())
    }
}

#[tokio::test]
async fn recovery_and_shutdown_use_explicit_lock_free_lifecycle_state() {
    let shutdown = ShutdownCoordinator::default();
    assert!(!shutdown.is_shutting_down());
    shutdown.request_shutdown();
    assert!(shutdown.is_shutting_down());
    assert!(shutdown.token().is_cancelled());

    let manager = RecoveryManager::default();
    assert!(!manager.is_recovering());
    let component = Arc::new(Recoverable {
        healthy: AtomicBool::new(false),
        calls: AtomicUsize::new(0),
    });
    let registered: Arc<dyn RecoverableComponent> = component.clone();
    manager.register(registered);

    assert!(matches!(
        manager.recover_unhealthy().await,
        RecoveryResult::Completed {
            succeeded: 1,
            failed: 0,
            ..
        }
    ));
    assert_eq!(component.calls.load(Ordering::Acquire), 1);
    assert!(component.is_healthy());
    assert!(matches!(
        manager.recover_unhealthy().await,
        RecoveryResult::Completed {
            succeeded: 0,
            failed: 0,
            ..
        }
    ));
}

#[tokio::test]
async fn automatic_recovery_retries_an_unhealthy_component_and_stops_on_cancellation() {
    let manager = Arc::new(RecoveryManager::default());
    let component = Arc::new(FlakyRecoverable {
        healthy: AtomicBool::new(false),
        calls: AtomicUsize::new(0),
        failures_before_success: 2,
    });
    let registered: Arc<dyn RecoverableComponent> = component.clone();
    manager.register(registered);

    let cancellation = CancellationToken::new();
    let worker = tokio::spawn({
        let manager = Arc::clone(&manager);
        let cancellation = cancellation.clone();
        async move {
            manager
                .run_auto_recovery(
                    AutoRecoveryOptions {
                        check_interval: Duration::from_secs(60),
                        max_retries: 3,
                        retry_delay: Duration::ZERO,
                        exponential_backoff: true,
                    },
                    cancellation,
                )
                .await
        }
    });

    assert!(
        tokio::time::timeout(Duration::from_secs(1), async {
            while component.calls.load(Ordering::Acquire) < 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok()
    );
    assert!(component.is_healthy());
    cancellation.cancel();
    assert!(worker.await.is_ok_and(|result| result.is_ok()));
}

#[tokio::test]
async fn recovery_manager_isolates_component_panics_and_continues_the_sweep() {
    let manager = RecoveryManager::default();
    manager.register(Arc::new(PanickingRecoverable));
    let healthy_candidate = Arc::new(Recoverable {
        healthy: AtomicBool::new(false),
        calls: AtomicUsize::new(0),
    });
    let registered: Arc<dyn RecoverableComponent> = healthy_candidate.clone();
    manager.register(registered);

    assert!(matches!(
        manager.recover_unhealthy().await,
        RecoveryResult::Completed {
            succeeded: 1,
            failed: 1,
            ..
        }
    ));
    assert_eq!(healthy_candidate.calls.load(Ordering::Acquire), 1);
    assert!(healthy_candidate.is_healthy());
    assert!(!manager.is_recovering());
}

#[tokio::test]
async fn recovery_manager_cancels_an_in_progress_component_recovery() {
    let manager = Arc::new(RecoveryManager::default());
    let component = Arc::new(BlockingRecoverable {
        started: Notify::new(),
    });
    let registered: Arc<dyn RecoverableComponent> = component.clone();
    manager.register(registered);

    let cancellation = CancellationToken::new();
    let recovery = tokio::spawn({
        let manager = Arc::clone(&manager);
        let cancellation = cancellation.clone();
        async move { manager.recover_unhealthy_until(cancellation).await }
    });
    component.started.notified().await;
    cancellation.cancel();

    let error = tokio::time::timeout(Duration::from_secs(1), recovery)
        .await
        .expect("recovery observes cancellation")
        .expect("recovery task does not panic")
        .expect_err("cancelled recovery returns an error");
    assert_eq!(error.code(), ErrorCode::Cancelled);
    assert!(!manager.is_recovering());
}

#[tokio::test]
async fn operation_tracker_drains_when_a_delivery_guard_is_dropped() {
    let tracker = OperationTracker::default();
    let operation = tracker.begin_operation();
    assert_eq!(tracker.pending_operations(), 1);

    let wait = tracker.wait_for_completion(CancellationToken::new());
    tokio::pin!(wait);
    assert!(
        tokio::time::timeout(Duration::from_millis(5), &mut wait)
            .await
            .is_err()
    );

    drop(operation);
    wait.await.unwrap();
    assert_eq!(tracker.pending_operations(), 0);
}
