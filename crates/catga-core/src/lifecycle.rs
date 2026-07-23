//! Runtime-neutral lifecycle, health, recovery, and shutdown contracts.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::CatgaResult;

/// Performs asynchronous component startup.
#[async_trait]
pub trait AsyncInitializable: Send + Sync {
    /// Initializes the component.
    async fn initialize(&self) -> CatgaResult<()>;
}

/// Exposes a cheap current health indicator.
pub trait HealthCheckable: Send + Sync {
    /// Returns whether the component can currently serve work.
    fn is_healthy(&self) -> bool;
    /// Returns an optional human-readable health detail.
    fn health_status(&self) -> Option<&str> {
        None
    }
}

/// Stops new work while allowing already-running work to drain.
pub trait Stoppable: Send + Sync {
    /// Stops accepting new messages or requests.
    fn stop_accepting(&self);
    /// Returns whether new work is still accepted.
    fn is_accepting(&self) -> bool;
}

/// A named component that can restore itself after becoming unhealthy.
#[async_trait]
pub trait RecoverableComponent: Send + Sync {
    /// Returns the stable component name.
    fn name(&self) -> &str;
    /// Returns whether recovery is currently needed.
    fn is_healthy(&self) -> bool;
    /// Attempts recovery without holding the manager's registration state.
    async fn recover(&self) -> CatgaResult<()>;
}

/// Outcome of one recovery sweep.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryResult {
    /// Recovery was skipped because another sweep owns it.
    AlreadyRecovering,
    /// The sweep examined unhealthy components and counted outcomes.
    Completed {
        /// Components recovered successfully.
        succeeded: u32,
        /// Components that still failed recovery.
        failed: u32,
        /// Wall-clock duration of the recovery sweep.
        duration: Duration,
    },
}

impl RecoveryResult {
    /// Creates a completed result with zero duration for deterministic callers and tests.
    pub const fn completed(succeeded: u32, failed: u32) -> Self {
        Self::Completed {
            succeeded,
            failed,
            duration: Duration::ZERO,
        }
    }
}

/// Coordinates graceful shutdown through a clonable cancellation token.
#[derive(Clone, Default)]
pub struct ShutdownCoordinator {
    token: CancellationToken,
}

impl ShutdownCoordinator {
    /// Returns a token cancelled when shutdown is requested.
    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }
    /// Returns whether shutdown has been requested.
    pub fn is_shutting_down(&self) -> bool {
        self.token.is_cancelled()
    }
    /// Idempotently requests shutdown.
    pub fn request_shutdown(&self) {
        self.token.cancel();
    }
}

/// Runs at most one lock-free recovery sweep at a time.
pub struct RecoveryManager {
    components: ArcSwap<Vec<Arc<dyn RecoverableComponent>>>,
    recovering: AtomicBool,
}

impl Default for RecoveryManager {
    fn default() -> Self {
        Self {
            components: ArcSwap::from_pointee(Vec::new()),
            recovering: AtomicBool::new(false),
        }
    }
}

impl RecoveryManager {
    /// Registers a recoverable component; duplicate handles are retained deliberately.
    pub fn register(&self, component: Arc<dyn RecoverableComponent>) {
        loop {
            let current = self.components.load_full();
            let mut next = Vec::with_capacity(current.len().saturating_add(1));
            next.extend(current.iter().cloned());
            next.push(Arc::clone(&component));
            let previous = self.components.compare_and_swap(&current, Arc::new(next));
            if Arc::ptr_eq(&*previous, &current) {
                return;
            }
        }
    }

    /// Recovers every currently unhealthy component, without blocking registration or reads.
    pub async fn recover_unhealthy(&self) -> RecoveryResult {
        if self
            .recovering
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return RecoveryResult::AlreadyRecovering;
        }
        let started = std::time::Instant::now();
        let mut succeeded = 0;
        let mut failed = 0;
        for component in self.components.load_full().iter() {
            if !component.is_healthy() {
                if component.recover().await.is_ok() {
                    succeeded += 1;
                } else {
                    failed += 1;
                }
            }
        }
        self.recovering.store(false, Ordering::Release);
        RecoveryResult::Completed {
            succeeded,
            failed,
            duration: started.elapsed(),
        }
    }
}
