use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use async_trait::async_trait;
use catga_core::{
    CatgaResult, RecoverableComponent, RecoveryManager, RecoveryResult, ShutdownCoordinator,
};

struct Recoverable {
    healthy: AtomicBool,
    calls: AtomicUsize,
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
