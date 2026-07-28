//! Recovery manager cancellation, isolation, and automatic retry contracts.

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
use tokio::{sync::Notify, time::timeout};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy)]
enum RecoveryMode {
    Succeeds,
    Fails,
    Panics,
    Blocks,
    FailsThenSucceeds,
}

struct TestComponent {
    name: &'static str,
    healthy: AtomicBool,
    attempts: AtomicUsize,
    failures_remaining: AtomicUsize,
    mode: RecoveryMode,
    started: Option<Arc<Notify>>,
    release: Option<Arc<Notify>>,
    recovered: Arc<Notify>,
}

impl TestComponent {
    fn new(name: &'static str, healthy: bool, mode: RecoveryMode) -> Arc<Self> {
        Arc::new(Self {
            name,
            healthy: AtomicBool::new(healthy),
            attempts: AtomicUsize::new(0),
            failures_remaining: AtomicUsize::new(0),
            mode,
            started: None,
            release: None,
            recovered: Arc::new(Notify::new()),
        })
    }

    fn blocking(name: &'static str) -> Arc<Self> {
        Arc::new(Self {
            name,
            healthy: AtomicBool::new(false),
            attempts: AtomicUsize::new(0),
            failures_remaining: AtomicUsize::new(0),
            mode: RecoveryMode::Blocks,
            started: Some(Arc::new(Notify::new())),
            release: Some(Arc::new(Notify::new())),
            recovered: Arc::new(Notify::new()),
        })
    }

    fn flaky(name: &'static str, failures: usize) -> Arc<Self> {
        Arc::new(Self {
            name,
            healthy: AtomicBool::new(false),
            attempts: AtomicUsize::new(0),
            failures_remaining: AtomicUsize::new(failures),
            mode: RecoveryMode::FailsThenSucceeds,
            started: None,
            release: None,
            recovered: Arc::new(Notify::new()),
        })
    }

    fn attempts(&self) -> usize {
        self.attempts.load(Ordering::Acquire)
    }

    fn started(&self) -> Arc<Notify> {
        Arc::clone(
            self.started
                .as_ref()
                .expect("blocking component has a start signal"),
        )
    }

    fn release(&self) -> Arc<Notify> {
        Arc::clone(
            self.release
                .as_ref()
                .expect("blocking component has a release signal"),
        )
    }
}

#[async_trait]
impl RecoverableComponent for TestComponent {
    fn name(&self) -> &str {
        self.name
    }

    fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    async fn recover(&self) -> CatgaResult<()> {
        self.attempts.fetch_add(1, Ordering::AcqRel);
        match self.mode {
            RecoveryMode::Succeeds => {
                self.healthy.store(true, Ordering::Release);
                Ok(())
            }
            RecoveryMode::Fails => Err(CatgaError::new(
                ErrorCode::Transient,
                "test component recovery failed",
            )),
            RecoveryMode::Panics => panic!("test component recovery panicked"),
            RecoveryMode::Blocks => {
                self.started().notify_one();
                self.release().notified().await;
                self.healthy.store(true, Ordering::Release);
                Ok(())
            }
            RecoveryMode::FailsThenSucceeds => {
                if self
                    .failures_remaining
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                        remaining.checked_sub(1)
                    })
                    .is_ok()
                {
                    return Err(CatgaError::new(
                        ErrorCode::Transient,
                        "test component is still recovering",
                    ));
                }
                self.healthy.store(true, Ordering::Release);
                self.recovered.notify_waiters();
                Ok(())
            }
        }
    }
}

#[tokio::test]
async fn recovery_sweeps_skip_healthy_components_and_isolate_failed_or_panicking_components() {
    let manager = RecoveryManager::default();
    let healthy = TestComponent::new("healthy", true, RecoveryMode::Succeeds);
    let succeeds = TestComponent::new("succeeds", false, RecoveryMode::Succeeds);
    let fails = TestComponent::new("fails", false, RecoveryMode::Fails);
    let panics = TestComponent::new("panics", false, RecoveryMode::Panics);
    manager.register(Arc::clone(&healthy) as Arc<dyn RecoverableComponent>);
    manager.register(Arc::clone(&succeeds) as Arc<dyn RecoverableComponent>);
    manager.register(Arc::clone(&fails) as Arc<dyn RecoverableComponent>);
    manager.register(Arc::clone(&panics) as Arc<dyn RecoverableComponent>);

    assert!(matches!(
        manager.recover_unhealthy().await,
        RecoveryResult::Completed {
            succeeded: 1,
            failed: 2,
            ..
        }
    ));
    assert_eq!(healthy.attempts(), 0);
    assert_eq!(succeeds.attempts(), 1);
    assert_eq!(fails.attempts(), 1);
    assert_eq!(panics.attempts(), 1);
    assert!(!manager.is_recovering());

    assert!(matches!(
        manager.recover_all().await,
        RecoveryResult::Completed {
            succeeded: 2,
            failed: 2,
            ..
        }
    ));
    assert_eq!(healthy.attempts(), 1);
    assert_eq!(succeeds.attempts(), 2);
    assert_eq!(fails.attempts(), 2);
    assert_eq!(panics.attempts(), 2);
    assert!(!manager.is_recovering());
}

#[tokio::test]
async fn concurrent_recovery_is_rejected_without_abandoning_the_active_component() -> CatgaResult<()>
{
    let manager = Arc::new(RecoveryManager::default());
    let component = TestComponent::blocking("blocked");
    manager.register(Arc::clone(&component) as Arc<dyn RecoverableComponent>);

    let started = component.started();
    let first_manager = Arc::clone(&manager);
    let first = tokio::spawn(async move { first_manager.recover_unhealthy().await });
    timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("recovery starts");
    assert!(manager.is_recovering());
    assert_eq!(
        manager.recover_unhealthy().await,
        RecoveryResult::AlreadyRecovering
    );

    component.release().notify_one();
    assert!(matches!(
        timeout(Duration::from_secs(1), first)
            .await
            .expect("first recovery completes")
            .expect("first recovery task does not panic"),
        RecoveryResult::Completed {
            succeeded: 1,
            failed: 0,
            ..
        }
    ));
    assert!(!manager.is_recovering());
    Ok(())
}

#[tokio::test]
async fn cancellation_drops_a_pending_recovery_and_releases_the_next_sweep() -> CatgaResult<()> {
    let manager = Arc::new(RecoveryManager::default());
    let component = TestComponent::blocking("blocked");
    manager.register(Arc::clone(&component) as Arc<dyn RecoverableComponent>);
    let cancellation = CancellationToken::new();
    let started = component.started();
    let task_manager = Arc::clone(&manager);
    let task_cancellation = cancellation.clone();
    let task = tokio::spawn(async move { task_manager.recover_all_until(task_cancellation).await });

    timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("recovery starts");
    cancellation.cancel();
    assert!(matches!(
        timeout(Duration::from_secs(1), task)
            .await
            .expect("cancelled recovery completes")
            .expect("cancelled recovery task does not panic"),
        Err(error) if error.code() == ErrorCode::Cancelled
    ));
    assert_eq!(component.attempts(), 1);
    assert!(!manager.is_recovering());
    Ok(())
}

#[tokio::test]
async fn automatic_recovery_retries_a_failed_sweep_then_stops_without_a_background_task()
-> CatgaResult<()> {
    let manager = Arc::new(RecoveryManager::default());
    let component = TestComponent::flaky("flaky", 1);
    manager.register(Arc::clone(&component) as Arc<dyn RecoverableComponent>);

    let invalid = manager
        .run_auto_recovery(
            AutoRecoveryOptions {
                check_interval: Duration::ZERO,
                ..AutoRecoveryOptions::default()
            },
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(invalid, Err(error) if error.code() == ErrorCode::Validation));

    let cancellation = CancellationToken::new();
    let recovered = Arc::clone(&component.recovered);
    let task_manager = Arc::clone(&manager);
    let task_cancellation = cancellation.clone();
    let task = tokio::spawn(async move {
        task_manager
            .run_auto_recovery(
                AutoRecoveryOptions {
                    check_interval: Duration::from_secs(60),
                    max_retries: 2,
                    retry_delay: Duration::ZERO,
                    exponential_backoff: true,
                },
                task_cancellation,
            )
            .await
    });

    timeout(Duration::from_secs(1), recovered.notified())
        .await
        .expect("automatic recovery succeeds");
    cancellation.cancel();
    timeout(Duration::from_secs(1), task)
        .await
        .expect("automatic recovery stops after cancellation")
        .expect("automatic recovery task does not panic")?;
    assert_eq!(component.attempts(), 2);
    assert!(component.is_healthy());
    assert!(!manager.is_recovering());
    Ok(())
}

#[test]
fn shutdown_coordinator_cancels_all_existing_and_future_tokens_idempotently() {
    let coordinator = ShutdownCoordinator::default();
    let first = coordinator.token();
    assert!(!coordinator.is_shutting_down());
    coordinator.request_shutdown();
    coordinator.request_shutdown();
    assert!(coordinator.is_shutting_down());
    assert!(first.is_cancelled());
    assert!(coordinator.token().is_cancelled());
}
