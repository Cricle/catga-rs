use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use tokio::time::sleep;

use crate::{
    Behavior, CatgaError, CatgaResult, ErrorCode, LeaseStore, Next, Request,
    telemetry::{
        distributed_lock_held, record_distributed_lock_acquire, record_distributed_lock_release,
    },
};

/// Supplies the named resource guarded by a distributed lock.
///
/// The returned key is owned because the request moves into the next pipeline
/// stage while the lease remains held.
pub trait DistributedLockKey {
    /// Returns the globally stable resource name for this request.
    fn distributed_lock_key(&self) -> Box<str>;
}

/// Acquires a unique-owner expiring lease around one request handler invocation.
///
/// This behavior is intentionally explicit instead of reflecting handler
/// attributes: each Rust request owns the code that derives its lock key. The
/// supplied lease duration must exceed the maximum expected handler duration;
/// use a fenced state machine for operations that must survive arbitrary pauses.
pub struct DistributedLockBehavior {
    store: Arc<dyn LeaseStore>,
    owner_prefix: Arc<str>,
    lease_duration: Duration,
    wait_timeout: Duration,
    owner_sequence: AtomicU64,
}

impl DistributedLockBehavior {
    /// Creates a behavior using `owner_prefix` plus a monotonic per-process suffix.
    pub fn new(
        store: Arc<dyn LeaseStore>,
        owner_prefix: impl Into<Arc<str>>,
        lease_duration: Duration,
    ) -> Self {
        Self {
            store,
            owner_prefix: owner_prefix.into(),
            lease_duration,
            wait_timeout: Duration::ZERO,
            owner_sequence: AtomicU64::new(1),
        }
    }

    fn next_owner(&self) -> Box<str> {
        format!(
            "{}:{}",
            self.owner_prefix,
            self.owner_sequence.fetch_add(1, Ordering::Relaxed)
        )
        .into_boxed_str()
    }

    /// Waits for at most `wait_timeout` when another owner currently holds the resource.
    pub fn with_wait_timeout(mut self, wait_timeout: Duration) -> Self {
        self.wait_timeout = wait_timeout;
        self
    }

    async fn acquire(&self, resource: &str, owner: &str) -> CatgaResult<bool> {
        let started = Instant::now();
        let deadline = Instant::now() + self.wait_timeout;
        let result = loop {
            match self
                .store
                .try_acquire(resource, owner, self.lease_duration)
                .await
            {
                Ok(true) => break Ok(true),
                Ok(false) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break Ok(false);
                    }
                    sleep(remaining.min(Duration::from_millis(10))).await;
                }
                Err(error) => break Err(error),
            }
        };
        record_distributed_lock_acquire(
            match &result {
                Ok(true) => "success",
                Ok(false) => "contention",
                Err(_) => "failure",
            },
            started,
        );
        result
    }
}

#[async_trait]
impl<M> Behavior<M> for DistributedLockBehavior
where
    M: Request + DistributedLockKey,
{
    async fn handle(&self, message: M, next: Next<M>) -> CatgaResult<M::Response> {
        let resource = message.distributed_lock_key();
        let owner = self.next_owner();
        if !self.acquire(&resource, &owner).await? {
            return Err(CatgaError::new(
                ErrorCode::Conflict,
                "distributed lock is already held",
            ));
        }

        let _held = distributed_lock_held();
        let result = next.run(message).await;
        match self.store.release(&resource, &owner).await {
            Ok(true) => {
                record_distributed_lock_release("success");
                result
            }
            Ok(false) => {
                record_distributed_lock_release("ownership_lost");
                if result.is_ok() {
                    Err(CatgaError::new(
                        ErrorCode::Internal,
                        "distributed lock ownership was lost before release",
                    ))
                } else {
                    result
                }
            }
            Err(error) => {
                record_distributed_lock_release("failure");
                if result.is_ok() { Err(error) } else { result }
            }
        }
    }
}
