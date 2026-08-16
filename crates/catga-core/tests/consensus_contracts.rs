//! Contract tests for the backend-agnostic consensus traits.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use async_trait::async_trait;
use catga_core::{CatgaResult, ConsensusCoordinator, ConsensusRuntime, ConsensusStateMachine};

/// In-memory single-node state machine recording every applied entry.
#[derive(Default)]
struct DummyStateMachine {
    entries: Vec<(u64, Vec<u8>)>,
}

impl ConsensusStateMachine for DummyStateMachine {
    fn apply(&mut self, index: u64, data: &[u8]) -> CatgaResult<()> {
        self.entries.push((index, data.to_vec()));
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        let mut bytes = Vec::new();
        for (index, data) in &self.entries {
            bytes.extend_from_slice(&index.to_le_bytes());
            bytes.extend_from_slice(&(data.len() as u64).to_le_bytes());
            bytes.extend_from_slice(data);
        }
        Ok(bytes)
    }

    fn restore(&mut self, data: &[u8]) -> CatgaResult<()> {
        self.entries.clear();
        let mut cursor = 0;
        while cursor + 16 <= data.len() {
            let index =
                u64::from_le_bytes(data[cursor..cursor + 8].try_into().expect("index field"));
            let length = u64::from_le_bytes(
                data[cursor + 8..cursor + 16]
                    .try_into()
                    .expect("length field"),
            ) as usize;
            cursor += 16;
            self.entries
                .push((index, data[cursor..cursor + length].to_vec()));
            cursor += length;
        }
        Ok(())
    }
}

struct DummyCoordinator {
    node_id: String,
    endpoint: Arc<str>,
    leader: AtomicBool,
}

impl ConsensusCoordinator for DummyCoordinator {
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn is_leader(&self) -> bool {
        self.leader.load(Ordering::SeqCst)
    }

    fn leader_endpoint(&self) -> Option<Arc<str>> {
        self.is_leader().then(|| Arc::clone(&self.endpoint))
    }

    fn member_endpoints(&self) -> Arc<[Arc<str>]> {
        vec![Arc::clone(&self.endpoint)].into()
    }
}

/// Single-node runtime that applies each proposal immediately.
struct DummyRuntime {
    machine: Mutex<DummyStateMachine>,
    applied: AtomicU64,
    alive: AtomicBool,
    coordinator: Arc<DummyCoordinator>,
}

impl DummyRuntime {
    fn new() -> Self {
        Self {
            machine: Mutex::new(DummyStateMachine::default()),
            applied: AtomicU64::new(0),
            alive: AtomicBool::new(true),
            coordinator: Arc::new(DummyCoordinator {
                node_id: "node-1".to_owned(),
                endpoint: "127.0.0.1:9001".into(),
                leader: AtomicBool::new(true),
            }),
        }
    }
}

#[async_trait]
impl ConsensusRuntime for DummyRuntime {
    async fn propose(&self, data: Vec<u8>) -> CatgaResult<()> {
        let index = self.applied.fetch_add(1, Ordering::SeqCst) + 1;
        self.machine
            .lock()
            .expect("machine lock")
            .apply(index, &data)
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
        self.alive.load(Ordering::SeqCst)
    }

    fn coordinator(&self) -> Arc<dyn ConsensusCoordinator> {
        Arc::clone(&self.coordinator) as Arc<dyn ConsensusCoordinator>
    }

    fn shutdown(&self) {
        self.alive.store(false, Ordering::SeqCst);
    }

    async fn join(self: Box<Self>) -> CatgaResult<()> {
        Ok(())
    }
}

fn assert_send<T: Send>(_: T) {}

/// Static assertion: every borrowed `ConsensusRuntime` future is `Send`.
fn assert_borrowed_futures_send<R: ConsensusRuntime>(runtime: &R) {
    assert_send(runtime.propose(Vec::new()));
    assert_send(runtime.add_member(2, "127.0.0.1:9002".to_owned()));
    assert_send(runtime.remove_member(2));
    assert_send(runtime.applied_index());
}

/// Static assertion: the owned `join` future (through its `Box<Self>`
/// receiver) is `Send`.
fn assert_join_future_send<R: ConsensusRuntime>(runtime: Box<R>) {
    assert_send(runtime.join());
}

#[test]
fn coordinator_is_object_safe() {
    let runtime = DummyRuntime::new();
    let coordinator: Arc<dyn ConsensusCoordinator> = runtime.coordinator();
    assert_eq!(coordinator.node_id(), "node-1");
    assert!(coordinator.is_leader());
    assert_eq!(
        coordinator.leader_endpoint().as_deref(),
        Some("127.0.0.1:9001")
    );
    assert_eq!(coordinator.member_endpoints().len(), 1);
}

#[test]
fn runtime_futures_are_send() {
    let runtime = DummyRuntime::new();
    assert_borrowed_futures_send(&runtime);
    assert_join_future_send(Box::new(DummyRuntime::new()));
}

#[tokio::test]
async fn propose_then_applied_index_observes_progress() {
    let runtime = DummyRuntime::new();
    assert_eq!(runtime.applied_index().await.expect("initial index"), 0);

    runtime
        .propose(b"set a=1".to_vec())
        .await
        .expect("propose a");
    runtime
        .propose(b"set b=2".to_vec())
        .await
        .expect("propose b");
    assert_eq!(runtime.applied_index().await.expect("final index"), 2);
    runtime
        .add_member(2, "127.0.0.1:9002".to_owned())
        .await
        .expect("add member");
    runtime.remove_member(2).await.expect("remove member");

    let snapshot = runtime
        .machine
        .lock()
        .expect("machine lock")
        .snapshot()
        .expect("snapshot");
    let mut restored = DummyStateMachine::default();
    restored.restore(&snapshot).expect("restore");
    assert_eq!(restored.entries.len(), 2);
    assert_eq!(restored.entries[0], (1, b"set a=1".to_vec()));

    assert!(runtime.is_alive());
    runtime.shutdown();
    assert!(!runtime.is_alive());
    Box::new(runtime).join().await.expect("join");
}

/// The trait is object-safe: a runtime erased behind
/// `Arc<dyn ConsensusRuntime>` still serves every borrowed method.
#[tokio::test]
async fn runtime_is_object_safe_behind_arc_dyn() {
    let runtime: Arc<dyn ConsensusRuntime> = Arc::new(DummyRuntime::new());

    runtime.propose(b"set a=1".to_vec()).await.expect("propose");
    assert_eq!(runtime.applied_index().await.expect("applied index"), 1);
    assert_eq!(runtime.coordinator().node_id(), "node-1");
    runtime
        .add_member(2, "127.0.0.1:9002".to_owned())
        .await
        .expect("add member");
    runtime.remove_member(2).await.expect("remove member");
    assert!(runtime.is_alive());
    runtime.shutdown();
    assert!(!runtime.is_alive());
}

/// `join` is callable through the trait object itself via its `Box<Self>`
/// receiver.
#[tokio::test]
async fn runtime_join_through_box_dyn() {
    let runtime: Box<dyn ConsensusRuntime> = Box::new(DummyRuntime::new());
    runtime.shutdown();
    runtime.join().await.expect("join through the trait object");
}
