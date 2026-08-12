//! Integration contract for [`RaftHttpCluster`]: a three-node in-process cluster
//! bootstrapped through the builder elects a leader, forwards a follower write to
//! it, applies it on every node, and flips `/healthz` to 503 once a node stops.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{Json, Router, routing::post};
use catga_axum::{CatgaHttpError, HttpClusterForwarder, RaftHttpCluster, leader_forward_route};
use catga_cluster::{
    ClusterCoordinator, ForwardToLeaderBehavior, RaftClusterConfig, RaftCommittedEntry,
    RaftStateMachine, RaftStateMachineRuntime,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode, Mediator, Pipeline};
use serde::{Deserialize, Serialize};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

const ELECTION_TIMEOUT: Duration = Duration::from_secs(10);
const APPLY_TIMEOUT: Duration = Duration::from_secs(10);

#[catga_core::catga_request(response = TestPutResult)]
#[derive(Clone, Debug, Serialize, Deserialize)]
struct TestPut {
    key: String,
    value: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TestPutResult {
    key: String,
}

#[derive(Clone, Default)]
struct TestMachine {
    values: Arc<Mutex<BTreeMap<String, String>>>,
}

fn lock_error() -> CatgaError {
    CatgaError::new(ErrorCode::Internal, "test state lock poisoned")
}

fn decode_error(error: serde_json::Error) -> CatgaError {
    CatgaError::new(ErrorCode::SerializationFailed, error.to_string())
}

impl RaftStateMachine for TestMachine {
    fn apply(&mut self, entry: &RaftCommittedEntry) -> CatgaResult<()> {
        let command: TestPut = serde_json::from_slice(&entry.data).map_err(decode_error)?;
        self.values
            .lock()
            .map_err(|_| lock_error())?
            .insert(command.key, command.value);
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        let values = self.values.lock().map_err(|_| lock_error())?;
        serde_json::to_vec(&*values).map_err(decode_error)
    }

    fn restore(&mut self, bytes: &[u8]) -> CatgaResult<()> {
        *self.values.lock().map_err(|_| lock_error())? =
            serde_json::from_slice(bytes).map_err(decode_error)?;
        Ok(())
    }
}

#[derive(Clone)]
struct TestKvService {
    runtime: Arc<RaftStateMachineRuntime>,
}

#[catga_core::catga_service]
impl TestKvService {
    async fn put(&self, msg: TestPut) -> CatgaResult<TestPutResult> {
        let payload = serde_json::to_vec(&msg).map_err(decode_error)?;
        self.runtime.propose(payload).await.map_err(|error| {
            CatgaError::new(
                ErrorCode::Unavailable,
                format!("raft proposal rejected: {error}"),
            )
        })?;
        Ok(TestPutResult { key: msg.key })
    }
}

struct TestNode {
    endpoint: String,
    runtime: Arc<RaftStateMachineRuntime>,
    values: Arc<Mutex<BTreeMap<String, String>>>,
    shutdown: Option<oneshot::Sender<()>>,
    server: Option<JoinHandle<CatgaResult<()>>>,
}

async fn spawn_node(id: u64, ports: &[u16], listener: TcpListener) -> TestNode {
    let members: Vec<serde_json::Value> = ports
        .iter()
        .enumerate()
        .filter(|(index, _)| *index as u64 + 1 != id)
        .map(|(index, port)| {
            serde_json::json!({
                "id": index as u64 + 1,
                "endpoint": format!("http://127.0.0.1:{port}"),
            })
        })
        .collect();
    let config: RaftClusterConfig = serde_json::from_value(serde_json::json!({
        "nodeId": id,
        "localNodeEndpoint": format!("http://127.0.0.1:{}", ports[(id - 1) as usize]),
        "members": members,
    }))
    .expect("cluster config must deserialize");

    let values = Arc::new(Mutex::new(BTreeMap::new()));
    let machine = TestMachine {
        values: Arc::clone(&values),
    };
    let cluster = RaftHttpCluster::builder(config)
        .state_machine(machine)
        .build()
        .expect("cluster bootstrap must build");
    let runtime = Arc::clone(cluster.runtime());

    let service = TestKvService {
        runtime: Arc::clone(&runtime),
    };
    let mediator = Arc::new(Mediator::new(
        service.registry().expect("service registry must build"),
    ));
    let forwarder = Arc::new(HttpClusterForwarder::new(reqwest::Client::new()));
    let pipeline = Arc::new(
        Pipeline::new().with(
            ForwardToLeaderBehavior::new(Arc::clone(cluster.coordinator()), forwarder)
                .with_retry(50, Duration::from_millis(50)),
        ),
    );

    let write = {
        let mediator = Arc::clone(&mediator);
        let pipeline = Arc::clone(&pipeline);
        move |Json(body): Json<TestPut>| {
            let mediator = Arc::clone(&mediator);
            let pipeline = Arc::clone(&pipeline);
            async move {
                mediator
                    .send_with(body, &pipeline)
                    .await
                    .map(Json)
                    .map_err(CatgaHttpError::from)
            }
        }
    };
    let routes = Router::new()
        .route("/kv", post(write))
        .merge(leader_forward_route::<TestPut>(mediator));

    let endpoint = format!("http://127.0.0.1:{}", ports[(id - 1) as usize]);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(cluster.serve_until(listener, routes, async move {
        let _ = shutdown_rx.await;
    }));
    TestNode {
        endpoint,
        runtime,
        values,
        shutdown: Some(shutdown_tx),
        server: Some(server),
    }
}

async fn wait_until(timeout: Duration, description: &str, mut condition: impl FnMut() -> bool) {
    let started = Instant::now();
    loop {
        if condition() {
            return;
        }
        assert!(
            started.elapsed() < timeout,
            "{description} within {timeout:.1?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_elects_forwards_and_reports_health() {
    let mut listeners = Vec::new();
    for _ in 0..3 {
        listeners.push(
            TcpListener::bind("127.0.0.1:0")
                .await
                .expect("ephemeral listener must bind"),
        );
    }
    let ports: Vec<u16> = listeners
        .iter()
        .map(|listener| listener.local_addr().expect("local address").port())
        .collect();

    let mut nodes = Vec::new();
    for (index, listener) in listeners.into_iter().enumerate() {
        nodes.push(spawn_node(index as u64 + 1, &ports, listener).await);
    }

    // Every node must observe the same elected leader before writes are forwarded.
    wait_until(ELECTION_TIMEOUT, "a leader must be elected", || {
        nodes
            .iter()
            .any(|node| node.runtime.coordinator().is_leader())
    })
    .await;
    wait_until(
        ELECTION_TIMEOUT,
        "every node must learn the leader endpoint",
        || {
            nodes
                .iter()
                .all(|node| node.runtime.coordinator().leader_endpoint().is_some())
        },
    )
    .await;

    let leader_index = nodes
        .iter()
        .position(|node| node.runtime.coordinator().is_leader())
        .expect("a leader must exist");
    let follower_index = (leader_index + 1) % nodes.len();

    let client = reqwest::Client::new();

    // The readiness probe reports the leader endpoint from the leader itself.
    let status: serde_json::Value = client
        .get(format!("{}/status", nodes[leader_index].endpoint))
        .send()
        .await
        .expect("status endpoint responds")
        .json()
        .await
        .expect("status body is JSON");
    assert_eq!(status["is_leader"], true);
    assert_eq!(
        status["leader_endpoint"].as_str(),
        Some(nodes[leader_index].endpoint.as_str())
    );
    assert_eq!(status["raft_alive"], true);

    // A write sent to a follower is forwarded to the leader and applied on all nodes.
    let write = client
        .post(format!("{}/kv", nodes[follower_index].endpoint))
        .json(&serde_json::json!({"key": "alpha", "value": "1"}))
        .send()
        .await
        .expect("follower write responds");
    assert!(
        write.status().is_success(),
        "follower write forwards to the leader, got HTTP {}",
        write.status()
    );
    wait_until(APPLY_TIMEOUT, "the write applies on every node", || {
        nodes.iter().all(|node| {
            node.values
                .lock()
                .map(|values| values.get("alpha").map(String::as_str) == Some("1"))
                .unwrap_or(false)
        })
    })
    .await;

    // The liveness probe flips to 503 once that node's Raft owner task stops.
    let health_before = client
        .get(format!("{}/healthz", nodes[follower_index].endpoint))
        .send()
        .await
        .expect("healthz responds before shutdown");
    assert_eq!(health_before.status(), 200);
    nodes[follower_index].runtime.shutdown();
    wait_until(APPLY_TIMEOUT, "the node runtime stops", || {
        !nodes[follower_index].runtime.is_alive()
    })
    .await;
    let health_after = client
        .get(format!("{}/healthz", nodes[follower_index].endpoint))
        .send()
        .await
        .expect("healthz still responds after shutdown");
    assert_eq!(health_after.status(), 503);

    for node in &mut nodes {
        if let Some(shutdown) = node.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
    for node in &mut nodes {
        if let Some(server) = node.server.take() {
            server
                .await
                .expect("server task joins")
                .expect("graceful serve succeeds");
        }
    }
}
