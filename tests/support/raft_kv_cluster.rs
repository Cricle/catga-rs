//! Shared in-process Raft key/value cluster fixtures for the cluster + CQRS + flow
//! combination contracts in `cluster_cqrs_flow.rs`.
//!
//! Mirrors the single-purpose fixture style of `catga-cluster/tests/common`, adapted to
//! this package's `tests/support` conventions: a channel transport hub, a key/value
//! state machine with an observable applied log, and small boot/election/failover
//! helpers shared by every combination scenario.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use catga_cluster::{
    ClusterCoordinator, RaftCommittedEntry, RaftMember, RaftMessage, RaftNode, RaftStateMachine,
    RaftStateMachineDriver, RaftStateMachineRuntime, RaftTransport, RaftTransportError,
    RaftTransportResult,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use tokio::sync::{RwLock, mpsc};

/// Logical Raft clock interval shared by every fixture runtime.
const TICK: Duration = Duration::from_millis(10);

/// Channel-based transport hub routing Raft messages between in-process runtimes.
///
/// Sending to a closed inbox (a stopped peer) is a retryable failure, mirroring a
/// fast TCP refusal from a dead process.
#[derive(Clone, Default)]
struct ChannelTransport {
    routes: Arc<RwLock<HashMap<u64, mpsc::Sender<RaftMessage>>>>,
}

impl ChannelTransport {
    async fn register(&self, runtime: &RaftStateMachineRuntime) {
        self.routes
            .write()
            .await
            .insert(runtime.id(), runtime.inbox());
    }
}

#[async_trait]
impl RaftTransport for ChannelTransport {
    async fn send(&self, message: RaftMessage) -> RaftTransportResult {
        let route = self.routes.read().await.get(&message.to).cloned();
        let Some(route) = route else {
            return Err(RaftTransportError::retryable(std::io::Error::other(
                "peer never registered",
            )));
        };
        route
            .send(message)
            .await
            .map_err(|_| RaftTransportError::retryable(std::io::Error::other("peer stopped")))?;
        Ok(())
    }
}

/// Encodes one key/value write as the replicated command payload.
pub(crate) fn encode_put(key: &str, value: &str) -> Vec<u8> {
    format!("{key}={value}").into_bytes()
}

fn decode_put(data: &[u8]) -> CatgaResult<(String, String)> {
    let text = std::str::from_utf8(data)
        .map_err(|_| CatgaError::new(ErrorCode::Validation, "kv command must be UTF-8"))?;
    let (key, value) = text
        .split_once('=')
        .ok_or_else(|| CatgaError::new(ErrorCode::Validation, "kv command must be key=value"))?;
    Ok((key.to_string(), value.to_string()))
}

/// One applied write annotated with the Raft log index that carried it.
#[derive(Debug)]
pub(crate) struct KvApplied {
    pub(crate) index: u64,
    pub(crate) key: String,
    pub(crate) value: String,
}

/// Observable replica state shared between a state machine and the test driver.
#[derive(Default)]
pub(crate) struct KvState {
    pub(crate) map: BTreeMap<String, String>,
    pub(crate) log: Vec<KvApplied>,
}

/// Shared handle to one replica's observable state.
pub(crate) type KvStore = Arc<Mutex<KvState>>;

/// Replicated key/value state machine.
///
/// The applied log keeps each write's Raft log index, which the combination tests use
/// as the idempotency key for post-commit event fan-out.
struct KvStateMachine {
    state: KvStore,
}

impl RaftStateMachine for KvStateMachine {
    fn apply(&mut self, entry: &RaftCommittedEntry) -> CatgaResult<()> {
        let (key, value) = decode_put(&entry.data)?;
        let mut state = self.state.lock().expect("kv store mutex poisoned");
        state.map.insert(key.clone(), value.clone());
        state.log.push(KvApplied {
            index: entry.index,
            key,
            value,
        });
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        let state = self.state.lock().expect("kv store mutex poisoned");
        let mut data = Vec::new();
        for (key, value) in &state.map {
            data.extend_from_slice(&encode_put(key, value));
            data.push(b'\n');
        }
        Ok(data)
    }

    fn restore(&mut self, data: &[u8]) -> CatgaResult<()> {
        let text = std::str::from_utf8(data)
            .map_err(|_| CatgaError::new(ErrorCode::Validation, "kv snapshot must be UTF-8"))?;
        let mut map = BTreeMap::new();
        for line in text.lines().filter(|line| !line.is_empty()) {
            let (key, value) = decode_put(line.as_bytes())?;
            map.insert(key, value);
        }
        self.state.lock().expect("kv store mutex poisoned").map = map;
        Ok(())
    }
}

/// One running replica: its runtime plus the observable store handle.
pub(crate) struct ClusterNode {
    pub(crate) runtime: RaftStateMachineRuntime,
    pub(crate) store: KvStore,
}

/// The live node set shared between the test driver and mediator command handlers.
pub(crate) type SharedNodes = Arc<RwLock<Vec<ClusterNode>>>;

/// Boots a `count`-voter in-process cluster on one channel transport hub.
pub(crate) async fn boot_nodes(count: u64) -> SharedNodes {
    let transport = ChannelTransport::default();
    let members: Vec<RaftMember> = (1..=count)
        .map(|id| RaftMember::new(id, format!("http://combo-node-{id}")))
        .collect();
    let mut nodes = Vec::new();
    for member in &members {
        let store = KvStore::default();
        let node = RaftNode::new(member.id(), member.endpoint(), members.clone())
            .expect("Raft node constructs");
        let driver = RaftStateMachineDriver::new(
            node,
            KvStateMachine {
                state: Arc::clone(&store),
            },
        )
        .expect("Raft driver constructs");
        let runtime = RaftStateMachineRuntime::spawn(driver, Arc::new(transport.clone()), TICK)
            .expect("Raft runtime starts");
        transport.register(&runtime).await;
        nodes.push(ClusterNode { runtime, store });
    }
    Arc::new(RwLock::new(nodes))
}

/// Campaigns the first node and waits until every voter sees it as leader.
pub(crate) async fn elect_first(nodes: &SharedNodes) {
    nodes.read().await[0]
        .runtime
        .campaign()
        .await
        .expect("the first node starts the election");
    let converged = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if nodes.read().await.iter().all(|node| {
                node.runtime.coordinator().leader_endpoint().as_deref()
                    == Some("http://combo-node-1")
            }) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        converged.is_ok(),
        "every voter must converge on the first leader"
    );
}

/// Waits until some surviving node holds leadership, returning its position.
pub(crate) async fn wait_for_leader(nodes: &SharedNodes, budget: Duration) -> usize {
    let elected = tokio::time::timeout(budget, async {
        loop {
            {
                let nodes = nodes.read().await;
                if let Some(position) = nodes
                    .iter()
                    .position(|node| node.runtime.coordinator().is_leader())
                {
                    return position;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    elected.expect("a leader must emerge within the budget")
}

/// Removes, stops, and joins the current leader; returns its member id.
pub(crate) async fn kill_leader(nodes: &SharedNodes) -> u64 {
    let position = {
        let nodes = nodes.read().await;
        nodes
            .iter()
            .position(|node| node.runtime.coordinator().is_leader())
            .expect("a leader must be running")
    };
    let node = nodes.write().await.remove(position);
    let id = node.runtime.id();
    node.runtime.shutdown();
    node.runtime
        .join()
        .await
        .expect("killed leader stops cleanly");
    id
}

/// Waits until one replica's map equals the expected state.
pub(crate) async fn wait_for_map(store: &KvStore, expected: &BTreeMap<String, String>) {
    let converged = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if store.lock().expect("kv store mutex poisoned").map == *expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(
        converged.is_ok(),
        "replica must converge to {expected:?}, got {:?}",
        store.lock().expect("kv store mutex poisoned").map
    );
}

/// Stops every remaining node gracefully.
pub(crate) async fn shutdown_all(nodes: &SharedNodes) {
    let drained: Vec<ClusterNode> = nodes.write().await.drain(..).collect();
    for node in &drained {
        node.runtime.shutdown();
    }
    for node in drained {
        node.runtime.join().await.expect("node stops cleanly");
    }
}
