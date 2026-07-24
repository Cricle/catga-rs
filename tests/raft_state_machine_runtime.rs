use std::{
    collections::HashMap,
    io,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use catga_cluster::{
    ClusterCoordinator, RaftCommittedEntry, RaftMember, RaftMessage, RaftNode, RaftStateMachine,
    RaftStateMachineDriver, RaftStateMachineRuntime, RaftTransport, RaftTransportResult,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use raft::eraftpb::{ConfState, MessageType, Snapshot};
use tokio::sync::{RwLock, mpsc};

#[derive(Default)]
struct SharedCounter {
    value: Arc<AtomicU64>,
}

impl RaftStateMachine for SharedCounter {
    fn apply(&mut self, entry: &RaftCommittedEntry) -> CatgaResult<()> {
        let bytes: [u8; 8] = entry.data.as_slice().try_into().map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "counter commands must contain eight bytes",
            )
        })?;
        self.value
            .fetch_add(u64::from_le_bytes(bytes), Ordering::Relaxed);
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        Ok(self.value.load(Ordering::Relaxed).to_le_bytes().to_vec())
    }

    fn restore(&mut self, bytes: &[u8]) -> CatgaResult<()> {
        let bytes: [u8; 8] = bytes.try_into().map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "counter snapshots must contain eight bytes",
            )
        })?;
        self.value
            .store(u64::from_le_bytes(bytes), Ordering::Relaxed);
        Ok(())
    }
}

struct SinkTransport;

#[async_trait]
impl RaftTransport for SinkTransport {
    async fn send(&self, _message: RaftMessage) -> RaftTransportResult {
        Ok(())
    }
}

#[derive(Clone, Default)]
struct RoutedTransport {
    routes: Arc<RwLock<HashMap<u64, mpsc::Sender<RaftMessage>>>>,
}

impl RoutedTransport {
    async fn register(&self, runtime: &RaftStateMachineRuntime) {
        self.routes
            .write()
            .await
            .insert(runtime.id(), runtime.inbox());
    }
}

#[async_trait]
impl RaftTransport for RoutedTransport {
    async fn send(&self, message: RaftMessage) -> RaftTransportResult {
        let route = self
            .routes
            .read()
            .await
            .get(&message.to)
            .cloned()
            .ok_or_else(|| io::Error::other("unknown Raft peer"))?;
        route
            .send(message)
            .await
            .map_err(|_| io::Error::other("Raft peer stopped"))?;
        Ok(())
    }
}

#[tokio::test]
async fn state_machine_runtime_applies_a_committed_command_without_shared_state_locks() {
    let value = Arc::new(AtomicU64::new(0));
    let machine = SharedCounter {
        value: Arc::clone(&value),
    };
    let node = RaftNode::new(
        1,
        "http://node-1",
        vec![RaftMember::new(1, "http://node-1")],
    )
    .unwrap();
    let driver = RaftStateMachineDriver::new(node, machine).unwrap();
    let runtime =
        RaftStateMachineRuntime::spawn(driver, Arc::new(SinkTransport), Duration::from_millis(1))
            .unwrap();

    runtime.campaign().await.unwrap();
    runtime.propose(7_u64.to_le_bytes()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while value.load(Ordering::Relaxed) != 7 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    runtime.shutdown();
    runtime.join().await.unwrap();
}

#[tokio::test]
async fn state_machine_runtime_stops_before_acknowledging_a_failed_application() {
    let machine = SharedCounter::default();
    let node = RaftNode::new(
        1,
        "http://node-1",
        vec![RaftMember::new(1, "http://node-1")],
    )
    .unwrap();
    let driver = RaftStateMachineDriver::new(node, machine).unwrap();
    let runtime =
        RaftStateMachineRuntime::spawn(driver, Arc::new(SinkTransport), Duration::from_millis(1))
            .unwrap();

    runtime.campaign().await.unwrap();
    assert!(matches!(
        runtime.propose([1_u8]).await,
        Err(catga_cluster::RaftStateMachineRuntimeError::StateMachine(_))
    ));
    assert!(matches!(
        runtime.join().await,
        Err(catga_cluster::RaftStateMachineRuntimeError::StateMachine(_))
    ));
}

#[tokio::test]
async fn state_machine_runtime_restores_an_incoming_snapshot_before_later_commands() {
    let value = Arc::new(AtomicU64::new(0));
    let directory = tempfile::tempdir().unwrap();
    let node = RaftNode::open_persistent(
        1,
        "http://node-1",
        vec![RaftMember::new(1, "http://node-1")],
        directory.path(),
    )
    .unwrap();
    let driver = RaftStateMachineDriver::new(
        node,
        SharedCounter {
            value: Arc::clone(&value),
        },
    )
    .unwrap();
    let runtime =
        RaftStateMachineRuntime::spawn(driver, Arc::new(SinkTransport), Duration::from_millis(1))
            .unwrap();

    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = 1;
    snapshot.mut_metadata().term = 1;
    snapshot
        .mut_metadata()
        .set_conf_state(ConfState::from((vec![1], Vec::new())));
    snapshot.set_data(10_u64.to_le_bytes().to_vec().into());
    let mut message = RaftMessage::default();
    message.set_msg_type(MessageType::MsgSnapshot);
    message.from = 2;
    message.to = 1;
    message.term = 1;
    message.set_snapshot(snapshot);
    runtime.inbox().send(message).await.unwrap();

    tokio::time::timeout(Duration::from_secs(1), async {
        while value.load(Ordering::Relaxed) != 10 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    runtime.campaign().await.unwrap();
    runtime.propose(2_u64.to_le_bytes()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while value.load(Ordering::Relaxed) != 12 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    runtime.shutdown();
    runtime.join().await.unwrap();
}

#[tokio::test]
async fn state_machine_runtimes_replicate_and_apply_on_every_node() {
    let transport = RoutedTransport::default();
    let members = vec![
        RaftMember::new(1, "http://node-1"),
        RaftMember::new(2, "http://node-2"),
        RaftMember::new(3, "http://node-3"),
    ];
    let values = (0..members.len())
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect::<Vec<_>>();
    let runtimes = members
        .iter()
        .zip(&values)
        .map(|(member, value)| {
            let node = RaftNode::new(member.id(), member.endpoint(), members.clone()).unwrap();
            let driver = RaftStateMachineDriver::new(
                node,
                SharedCounter {
                    value: Arc::clone(value),
                },
            )
            .unwrap();
            RaftStateMachineRuntime::spawn(
                driver,
                Arc::new(transport.clone()),
                Duration::from_millis(1),
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    for runtime in &runtimes {
        transport.register(runtime).await;
    }

    runtimes[0].campaign().await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while runtimes.iter().any(|runtime| {
            runtime.coordinator().leader_endpoint().as_deref() != Some("http://node-1")
        }) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    runtimes[0].propose(11_u64.to_le_bytes()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while values
            .iter()
            .any(|value| value.load(Ordering::Relaxed) != 11)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    for runtime in runtimes {
        runtime.shutdown();
        runtime.join().await.unwrap();
    }
}
