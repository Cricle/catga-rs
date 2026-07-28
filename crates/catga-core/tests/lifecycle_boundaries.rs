use std::{
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::Duration,
};

use async_trait::async_trait;
use catga_core::{
    AcceptanceGate, AsyncInitializable, CatgaResult, ErrorCode, OperationTracker, Stoppable,
    TransportLifecycle, TransportLifecycleOptions, TransportShutdown, Waitable,
};
use tokio_util::sync::CancellationToken;

struct LifecycleFixture {
    initialized: AtomicBool,
    accepting: AtomicBool,
    pending: AtomicUsize,
}

impl LifecycleFixture {
    fn new(pending: usize) -> Self {
        Self {
            initialized: AtomicBool::new(false),
            accepting: AtomicBool::new(true),
            pending: AtomicUsize::new(pending),
        }
    }
}

#[async_trait]
impl AsyncInitializable for LifecycleFixture {
    async fn initialize(&self) -> CatgaResult<()> {
        self.initialized.store(true, Ordering::Release);
        Ok(())
    }
}

impl Stoppable for LifecycleFixture {
    fn stop_accepting(&self) {
        self.accepting.store(false, Ordering::Release);
    }

    fn is_accepting(&self) -> bool {
        self.accepting.load(Ordering::Acquire)
    }
}

#[async_trait]
impl Waitable for LifecycleFixture {
    async fn wait_for_completion(&self, _cancellation: CancellationToken) -> CatgaResult<()> {
        if self.pending_operations() != 0 {
            std::future::pending::<()>().await;
        }
        Ok(())
    }

    fn pending_operations(&self) -> usize {
        self.pending.load(Ordering::Acquire)
    }
}

#[test]
fn acceptance_gate_and_lifecycle_options_reject_invalid_shutdown_state() {
    let gate = AcceptanceGate::default();
    let clone = gate.clone();
    assert!(gate.is_accepting());
    clone.stop_accepting();
    assert!(!gate.is_accepting());
    assert!(matches!(
        gate.ensure_accepting(),
        Err(error) if error.code() == ErrorCode::Unavailable
    ));
    assert!(matches!(
        TransportLifecycleOptions::new(Duration::ZERO),
        Err(error) if error.code() == ErrorCode::Validation
    ));
}

#[tokio::test]
async fn lifecycle_shutdown_reports_drained_cancelled_and_timed_out_work() -> CatgaResult<()> {
    let drained = TransportLifecycle::new(LifecycleFixture::new(0));
    drained.initialize().await?;
    assert!(drained.transport().initialized.load(Ordering::Acquire));
    assert_eq!(
        drained
            .shutdown(
                TransportLifecycleOptions::new(Duration::from_millis(10))?,
                CancellationToken::new(),
            )
            .await?,
        TransportShutdown::Drained
    );

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancelled = TransportLifecycle::new(LifecycleFixture::new(2));
    assert_eq!(
        cancelled
            .shutdown(
                TransportLifecycleOptions::new(Duration::from_millis(10))?,
                cancellation,
            )
            .await?,
        TransportShutdown::Cancelled {
            pending_operations: 2
        }
    );

    let timed_out = TransportLifecycle::new(LifecycleFixture::new(3));
    assert_eq!(
        timed_out
            .shutdown(
                TransportLifecycleOptions::new(Duration::from_millis(1))?,
                CancellationToken::new(),
            )
            .await?,
        TransportShutdown::TimedOut {
            pending_operations: 3
        }
    );
    Ok(())
}

#[tokio::test]
async fn operation_tracker_releases_each_guard_once_and_unblocks_waiters() -> CatgaResult<()> {
    let tracker = OperationTracker::default();
    let first = tracker.begin_operation();
    let second = tracker.begin_operation();
    assert_eq!(tracker.pending_operations(), 2);

    first.complete();
    first.complete();
    assert_eq!(tracker.pending_operations(), 1);

    let wait = tracker.wait_for_completion(CancellationToken::new());
    tokio::pin!(wait);
    assert!(
        tokio::time::timeout(Duration::from_millis(1), &mut wait)
            .await
            .is_err()
    );
    drop(second);
    wait.await?;
    assert_eq!(tracker.pending_operations(), 0);
    Ok(())
}
