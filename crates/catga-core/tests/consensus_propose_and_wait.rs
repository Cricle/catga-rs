//! Contract tests for the default `ConsensusRuntime::propose_and_wait`
//! implementation the trait supplies to backends without an
//! applied-notification path.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use catga_core::{CatgaResult, ConsensusCoordinator, ConsensusRuntime, ErrorCode};

struct StubCoordinator;

impl ConsensusCoordinator for StubCoordinator {
    fn node_id(&self) -> &str {
        "node-1"
    }

    fn is_leader(&self) -> bool {
        true
    }

    fn leader_endpoint(&self) -> Option<Arc<str>> {
        Some("127.0.0.1:9001".into())
    }

    fn member_endpoints(&self) -> Arc<[Arc<str>]> {
        vec!["127.0.0.1:9001".into()].into()
    }
}

/// A minimal runtime: proposals are accepted, and while `applies` is set each
/// one advances the applied index immediately (like a backend whose write RPC
/// resolves after application).
struct StubRuntime {
    applied: AtomicU64,
    applies: bool,
    coordinator: Arc<StubCoordinator>,
}

impl StubRuntime {
    fn new(applies: bool) -> Self {
        Self {
            applied: AtomicU64::new(0),
            applies,
            coordinator: Arc::new(StubCoordinator),
        }
    }
}

#[async_trait]
impl ConsensusRuntime for StubRuntime {
    async fn propose(&self, _data: Vec<u8>) -> CatgaResult<()> {
        if self.applies {
            self.applied.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }

    async fn add_member(&self, _id: u64, _endpoint: String) -> CatgaResult<()> {
        Ok(())
    }

    async fn remove_member(&self, _id: u64) -> CatgaResult<()> {
        Ok(())
    }

    async fn applied_index(&self) -> CatgaResult<u64> {
        Ok(self.applied.load(Ordering::SeqCst))
    }

    fn is_alive(&self) -> bool {
        true
    }

    fn coordinator(&self) -> Arc<dyn ConsensusCoordinator> {
        Arc::clone(&self.coordinator) as Arc<dyn ConsensusCoordinator>
    }

    fn shutdown(&self) {}

    async fn join(self: Box<Self>) -> CatgaResult<()> {
        Ok(())
    }
}

#[tokio::test]
async fn default_propose_and_wait_resolves_after_the_entry_applies() {
    let runtime = StubRuntime::new(true);

    let index = runtime
        .propose_and_wait(b"set a=1".to_vec(), Duration::from_secs(2))
        .await
        .expect("an immediately applied entry resolves");
    assert_eq!(index, 1);

    let next = runtime
        .propose_and_wait(b"set b=2".to_vec(), Duration::from_secs(2))
        .await
        .expect("the second entry resolves at the next index");
    assert_eq!(next, 2);
}

#[tokio::test]
async fn default_propose_and_wait_times_out_without_progress() {
    let runtime = StubRuntime::new(false);

    let started = Instant::now();
    let result = runtime
        .propose_and_wait(b"set a=1".to_vec(), Duration::from_millis(50))
        .await;
    assert!(matches!(
        result,
        Err(ref error) if error.code() == ErrorCode::Timeout
    ));
    assert!(
        started.elapsed() >= Duration::from_millis(50),
        "the default implementation must honor the deadline"
    );
}
