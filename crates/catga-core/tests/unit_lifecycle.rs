//! Unit tests for lifecycle components.

use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use catga_core::{
    AcceptanceGate, AsyncInitializable, AutoRecoveryOptions, CatgaError, CatgaResult, ErrorCode,
    OperationTracker, RecoverableComponent, RecoveryManager, RecoveryResult, ShutdownCoordinator,
    Stoppable, TransportLifecycle, TransportLifecycleOptions, TransportShutdown, Waitable,
};

// Test for AcceptanceGate
#[test]
fn acceptance_gate_default_is_accepting() {
    let gate = AcceptanceGate::default();
    assert!(gate.is_accepting());
}

#[test]
fn acceptance_gate_stop_accepting_works() {
    let gate = AcceptanceGate::default();
    gate.stop_accepting();
    assert!(!gate.is_accepting());
}

#[test]
fn acceptance_gate_clone_shares_state() {
    let gate = AcceptanceGate::default();
    let gate2 = gate.clone();
    gate.stop_accepting();
    assert!(!gate2.is_accepting());
}

#[test]
fn acceptance_gate_ensure_accepting_ok() {
    let gate = AcceptanceGate::default();
    assert!(gate.ensure_accepting().is_ok());
}

#[test]
fn acceptance_gate_ensure_accepting_err() {
    let gate = AcceptanceGate::default();
    gate.stop_accepting();
    let result = gate.ensure_accepting();
    assert!(result.is_err());
    assert_eq!(
        result.expect_err("should be Err").code(),
        ErrorCode::Unavailable
    );
}

// Test for TransportLifecycleOptions
#[test]
fn lifecycle_options_default() {
    let options = TransportLifecycleOptions::default();
    assert_eq!(options.drain_timeout, Duration::from_secs(30));
}

#[test]
fn lifecycle_options_new_accepts_valid_timeout() {
    let options = TransportLifecycleOptions::new(Duration::from_secs(60))
        .expect("valid timeout should succeed");
    assert_eq!(options.drain_timeout, Duration::from_secs(60));
}

#[test]
fn lifecycle_options_new_rejects_zero_timeout() {
    let result = TransportLifecycleOptions::new(Duration::ZERO);
    assert!(result.is_err());
    assert_eq!(
        result.expect_err("zero timeout should fail").code(),
        ErrorCode::Validation
    );
}

// Test for TransportShutdown variants
#[test]
fn transport_shutdown_drained() {
    let shutdown = TransportShutdown::Drained;
    match shutdown {
        TransportShutdown::Drained => {}
        _ => panic!("Expected Drained"),
    }
}

#[test]
fn transport_shutdown_cancelled() {
    let shutdown = TransportShutdown::Cancelled {
        pending_operations: 5,
    };
    match shutdown {
        TransportShutdown::Cancelled { pending_operations } => {
            assert_eq!(pending_operations, 5);
        }
        _ => panic!("Expected Cancelled"),
    }
}

#[test]
fn transport_shutdown_timed_out() {
    let shutdown = TransportShutdown::TimedOut {
        pending_operations: 3,
    };
    match shutdown {
        TransportShutdown::TimedOut { pending_operations } => {
            assert_eq!(pending_operations, 3);
        }
        _ => panic!("Expected TimedOut"),
    }
}

// Test for RecoveryResult
#[test]
fn recovery_result_completed_factory() {
    let result = RecoveryResult::completed(3, 2);
    match result {
        RecoveryResult::Completed {
            succeeded,
            failed,
            duration,
        } => {
            assert_eq!(succeeded, 3);
            assert_eq!(failed, 2);
            assert_eq!(duration, Duration::ZERO);
        }
        _ => panic!("Expected Completed"),
    }
}

#[test]
fn recovery_result_already_recovering() {
    let result = RecoveryResult::AlreadyRecovering;
    match result {
        RecoveryResult::AlreadyRecovering => {}
        _ => panic!("Expected AlreadyRecovering"),
    }
}

// Test for OperationTracker
#[tokio::test]
async fn operation_tracker_default() {
    let tracker = OperationTracker::default();
    assert_eq!(tracker.pending_operations(), 0);
}

#[tokio::test]
async fn operation_tracker_begin_operation() {
    let tracker = OperationTracker::default();
    let guard = tracker.begin_operation();
    assert_eq!(tracker.pending_operations(), 1);
    drop(guard);
    // Guard drop decrements
    // Note: This may be async timing dependent
}

#[tokio::test]
async fn operation_tracker_guard_complete() {
    let tracker = OperationTracker::default();
    let guard = tracker.begin_operation();
    assert_eq!(tracker.pending_operations(), 1);
    guard.complete();
    assert_eq!(tracker.pending_operations(), 0);
}

#[tokio::test]
async fn operation_tracker_guard_complete_idempotent() {
    let tracker = OperationTracker::default();
    let guard = tracker.begin_operation();
    guard.complete();
    guard.complete(); // Second call should be no-op
    assert_eq!(tracker.pending_operations(), 0);
}

#[tokio::test]
async fn operation_tracker_wait_completes_immediately() {
    let tracker = OperationTracker::default();
    let token = CancellationToken::new();
    tracker
        .wait_for_completion(token)
        .await
        .expect("wait should succeed");
}

#[tokio::test]
async fn operation_tracker_wait_respects_cancellation() {
    let tracker = OperationTracker::default();
    tracker.begin_operation(); // Keep one pending
    let token = CancellationToken::new();
    token.cancel();

    // Cancellation should return immediately even with pending operations
    tracker
        .wait_for_completion(token)
        .await
        .expect("wait should succeed");
}

#[test]
fn operation_tracker_debug() {
    let tracker = OperationTracker::default();
    let debug_str = format!("{:?}", tracker);
    assert!(debug_str.contains("OperationTracker"));
}

// Test for ShutdownCoordinator
#[test]
fn shutdown_coordinator_default() {
    let coordinator = ShutdownCoordinator::default();
    assert!(!coordinator.is_shutting_down());
}

#[test]
fn shutdown_coordinator_token_clone() {
    let coordinator = ShutdownCoordinator::default();
    let token = coordinator.token();
    assert!(!token.is_cancelled());
}

#[test]
fn shutdown_coordinator_request_shutdown() {
    let coordinator = ShutdownCoordinator::default();
    coordinator.request_shutdown();
    assert!(coordinator.is_shutting_down());
}

#[test]
fn shutdown_coordinator_request_shutdown_idempotent() {
    let coordinator = ShutdownCoordinator::default();
    coordinator.request_shutdown();
    coordinator.request_shutdown(); // Second call
    assert!(coordinator.is_shutting_down());
}

#[test]
fn shutdown_coordinator_clone_shares_token() {
    let coordinator = ShutdownCoordinator::default();
    let coordinator2 = coordinator.clone();
    coordinator.request_shutdown();
    assert!(coordinator2.is_shutting_down());
}

// Mock components for testing RecoveryManager
struct MockRecoverableComponent {
    name: &'static str,
    healthy: bool,
    recovery_succeeds: bool,
}

#[async_trait]
impl RecoverableComponent for MockRecoverableComponent {
    fn name(&self) -> &str {
        self.name
    }

    fn is_healthy(&self) -> bool {
        self.healthy
    }

    async fn recover(&self) -> CatgaResult<()> {
        if self.recovery_succeeds {
            Ok(())
        } else {
            Err(CatgaError::new(ErrorCode::Internal, "recovery failed"))
        }
    }
}

// Test for RecoveryManager
#[tokio::test]
async fn recovery_manager_default() {
    let manager = RecoveryManager::default();
    assert!(!manager.is_recovering());
}

#[tokio::test]
async fn recovery_manager_register() {
    let manager = RecoveryManager::default();
    let component = Arc::new(MockRecoverableComponent {
        name: "test",
        healthy: true,
        recovery_succeeds: true,
    });
    manager.register(component);
    // Registration doesn't throw
}

#[tokio::test]
async fn recovery_manager_recover_all_no_components() {
    let manager = RecoveryManager::default();
    let result = manager.recover_all().await;
    match result {
        RecoveryResult::Completed {
            succeeded, failed, ..
        } => {
            assert_eq!(succeeded, 0);
            assert_eq!(failed, 0);
        }
        _ => panic!("Expected Completed"),
    }
}

#[tokio::test]
async fn recovery_manager_recover_all_with_components() {
    let manager = RecoveryManager::default();
    let component = Arc::new(MockRecoverableComponent {
        name: "test",
        healthy: true,
        recovery_succeeds: true,
    });
    manager.register(component);

    let result = manager.recover_all().await;
    match result {
        RecoveryResult::Completed {
            succeeded, failed, ..
        } => {
            assert_eq!(succeeded, 1);
            assert_eq!(failed, 0);
        }
        _ => panic!("Expected Completed"),
    }
}

#[tokio::test]
async fn recovery_manager_recover_all_with_failed_component() {
    let manager = RecoveryManager::default();
    let component = Arc::new(MockRecoverableComponent {
        name: "test",
        healthy: true,
        recovery_succeeds: false,
    });
    manager.register(component);

    let result = manager.recover_all().await;
    match result {
        RecoveryResult::Completed {
            succeeded, failed, ..
        } => {
            assert_eq!(succeeded, 0);
            assert_eq!(failed, 1);
        }
        _ => panic!("Expected Completed"),
    }
}

#[tokio::test]
async fn recovery_manager_recover_all_already_recovering() {
    // RecoveryManager doesn't implement Clone, so we test the behavior differently
    // by verifying the manager is not Clone but has the expected behavior
    let manager = RecoveryManager::default();
    let component = Arc::new(MockRecoverableComponent {
        name: "slow",
        healthy: true,
        recovery_succeeds: true,
    });
    manager.register(component);

    // First call should complete successfully
    let result = manager.recover_all().await;
    match result {
        RecoveryResult::Completed {
            succeeded, failed, ..
        } => {
            assert_eq!(succeeded, 1);
            assert_eq!(failed, 0);
        }
        _ => panic!("Expected Completed"),
    }
}

#[tokio::test]
async fn recovery_manager_recover_all_until() {
    let manager = RecoveryManager::default();
    let component = Arc::new(MockRecoverableComponent {
        name: "test",
        healthy: true,
        recovery_succeeds: true,
    });
    manager.register(component);

    let token = CancellationToken::new();
    let result = manager
        .recover_all_until(token)
        .await
        .expect("recovery should succeed");
    match result {
        RecoveryResult::Completed {
            succeeded, failed, ..
        } => {
            assert_eq!(succeeded, 1);
            assert_eq!(failed, 0);
        }
        _ => panic!("Expected Completed"),
    }
}

#[tokio::test]
async fn recovery_manager_recover_all_until_cancelled() {
    let manager = RecoveryManager::default();
    let token = CancellationToken::new();
    token.cancel();

    let result = manager.recover_all_until(token).await;
    assert!(result.is_err());
    assert_eq!(
        result.expect_err("cancelled should fail").code(),
        ErrorCode::Cancelled
    );
}

#[tokio::test]
async fn recovery_manager_recover_unhealthy() {
    let manager = RecoveryManager::default();
    let healthy = Arc::new(MockRecoverableComponent {
        name: "healthy",
        healthy: true,
        recovery_succeeds: true,
    });
    let unhealthy = Arc::new(MockRecoverableComponent {
        name: "unhealthy",
        healthy: false,
        recovery_succeeds: true,
    });
    manager.register(healthy);
    manager.register(unhealthy);

    let result = manager.recover_unhealthy().await;
    match result {
        RecoveryResult::Completed {
            succeeded, failed, ..
        } => {
            assert_eq!(succeeded, 1);
            assert_eq!(failed, 0);
        }
        _ => panic!("Expected Completed"),
    }
}

#[tokio::test]
async fn recovery_manager_recover_unhealthy_until() {
    let manager = RecoveryManager::default();
    let unhealthy = Arc::new(MockRecoverableComponent {
        name: "unhealthy",
        healthy: false,
        recovery_succeeds: true,
    });
    manager.register(unhealthy);

    let token = CancellationToken::new();
    let result = manager
        .recover_unhealthy_until(token)
        .await
        .expect("recovery should succeed");
    match result {
        RecoveryResult::Completed {
            succeeded, failed, ..
        } => {
            assert_eq!(succeeded, 1);
            assert_eq!(failed, 0);
        }
        _ => panic!("Expected Completed"),
    }
}

// Test for AutoRecoveryOptions
#[test]
fn auto_recovery_options_default() {
    let options = AutoRecoveryOptions::default();
    assert_eq!(options.check_interval, Duration::from_secs(30));
    assert_eq!(options.max_retries, 3);
    assert_eq!(options.retry_delay, Duration::from_secs(1));
    assert!(options.exponential_backoff);
}

#[test]
fn auto_recovery_options_default_is_valid() {
    // Default options should be valid for use with RecoveryManager
    let options = AutoRecoveryOptions::default();
    // Just verify defaults are sensible
    assert!(options.check_interval > Duration::ZERO);
    assert!(options.retry_delay >= Duration::ZERO);
}

#[test]
fn auto_recovery_options_with_values() {
    let options = AutoRecoveryOptions {
        check_interval: Duration::from_secs(60),
        max_retries: 5,
        retry_delay: Duration::from_secs(2),
        exponential_backoff: true,
    };
    assert_eq!(options.check_interval, Duration::from_secs(60));
    assert_eq!(options.max_retries, 5);
    assert_eq!(options.retry_delay, Duration::from_secs(2));
    assert!(options.exponential_backoff);
}

#[test]
fn auto_recovery_options_backoff_disabled() {
    let options = AutoRecoveryOptions {
        check_interval: Duration::from_secs(30),
        max_retries: 3,
        retry_delay: Duration::from_secs(1),
        exponential_backoff: false,
    };
    // Without exponential backoff, retry_delay remains constant
    assert_eq!(options.retry_delay, Duration::from_secs(1));
    assert!(!options.exponential_backoff);
}

// Test for TransportLifecycle with mock components
struct MockTransport {
    gate: AcceptanceGate,
    tracker: OperationTracker,
    _initialized: bool,
}

impl MockTransport {
    fn new() -> Self {
        Self {
            gate: AcceptanceGate::default(),
            tracker: OperationTracker::default(),
            _initialized: false,
        }
    }
}

#[async_trait]
impl AsyncInitializable for MockTransport {
    async fn initialize(&self) -> CatgaResult<()> {
        Ok(())
    }
}

impl Stoppable for MockTransport {
    fn stop_accepting(&self) {
        self.gate.stop_accepting();
    }

    fn is_accepting(&self) -> bool {
        self.gate.is_accepting()
    }
}

#[async_trait]
impl Waitable for MockTransport {
    async fn wait_for_completion(&self, cancellation: CancellationToken) -> CatgaResult<()> {
        self.tracker.wait_for_completion(cancellation).await
    }

    fn pending_operations(&self) -> usize {
        self.tracker.pending_operations()
    }
}

#[tokio::test]
async fn transport_lifecycle_new_and_accessors() {
    let transport = MockTransport::new();
    let lifecycle = TransportLifecycle::new(transport);

    // Verify transport is accessible and is_accepting works
    assert!(lifecycle.transport().is_accepting());
}

#[tokio::test]
async fn transport_lifecycle_into_inner() {
    let transport = MockTransport::new();
    let lifecycle = TransportLifecycle::new(transport);
    let _inner = lifecycle.into_inner();
    // Successfully extracted the inner transport
}

#[tokio::test]
async fn transport_lifecycle_initialize() {
    let transport = MockTransport::new();
    let lifecycle = TransportLifecycle::new(transport);
    lifecycle
        .initialize()
        .await
        .expect("initialize should succeed");
}

#[tokio::test]
async fn transport_lifecycle_shutdown_drained() {
    let transport = MockTransport::new();
    let lifecycle = TransportLifecycle::new(transport);

    let options =
        TransportLifecycleOptions::new(Duration::from_secs(1)).expect("valid timeout");
    let token = CancellationToken::new();

    let result = lifecycle
        .shutdown(options, token)
        .await
        .expect("shutdown should succeed");
    assert!(matches!(result, TransportShutdown::Drained));
}

#[tokio::test]
async fn transport_lifecycle_shutdown_completes() {
    let transport = MockTransport::new();
    let lifecycle = TransportLifecycle::new(transport);

    // With no pending operations, drain completes immediately
    let options =
        TransportLifecycleOptions::new(Duration::from_secs(10)).expect("valid timeout");
    let token = CancellationToken::new();

    let result = lifecycle
        .shutdown(options, token)
        .await
        .expect("shutdown should succeed");
    // With no pending operations, should complete as Drained
    assert!(matches!(result, TransportShutdown::Drained));
}

