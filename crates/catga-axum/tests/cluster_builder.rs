//! Contract tests for the [`RaftHttpCluster`] builder: configuration error
//! mapping, custom wiring options, the status probe, and the graceful
//! serve-then-campaign startup ordering.

use std::time::Duration;

use axum::Router;
use catga_axum::{RAFT_HTTP_HEALTH_PATH, RAFT_HTTP_STATUS_PATH, RaftHttpCluster};
use catga_cluster::{ClusterCoordinator, RaftClusterConfig, RaftCommittedEntry, RaftStateMachine};
use catga_core::{CatgaResult, ErrorCode};
use http::StatusCode;
use tokio::net::TcpListener;

struct NullMachine;

impl RaftStateMachine for NullMachine {
    fn apply(&mut self, _entry: &RaftCommittedEntry) -> CatgaResult<()> {
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        Ok(Vec::new())
    }

    fn restore(&mut self, _bytes: &[u8]) -> CatgaResult<()> {
        Ok(())
    }
}

fn in_memory_config(node_id: u64, port: u16) -> RaftClusterConfig {
    serde_json::from_value(serde_json::json!({
        "nodeId": node_id,
        "localNodeEndpoint": format!("http://127.0.0.1:{port}"),
        "members": [],
        "persistentStatePath": null,
    }))
    .expect("cluster config must deserialize")
}

#[test]
fn build_rejects_invalid_timing_and_membership() {
    let bad_timing: RaftClusterConfig = serde_json::from_value(serde_json::json!({
        "nodeId": 1,
        "localNodeEndpoint": "http://127.0.0.1:9100",
        "members": [],
        "persistentStatePath": null,
        "tickIntervalMs": 0,
    }))
    .expect("cluster config must deserialize");
    let error = RaftHttpCluster::builder(bad_timing)
        .state_machine(NullMachine)
        .build()
        .map(|_| ())
        .expect_err("zero tick must fail validation");
    assert_eq!(error.code(), ErrorCode::Validation);

    let bad_member: RaftClusterConfig = serde_json::from_value(serde_json::json!({
        "nodeId": 1,
        "localNodeEndpoint": "http://127.0.0.1:9100",
        "members": [{ "id": 0, "endpoint": "http://127.0.0.1:9101" }],
        "persistentStatePath": null,
    }))
    .expect("cluster config must deserialize");
    let error = RaftHttpCluster::builder(bad_member)
        .state_machine(NullMachine)
        .build()
        .map(|_| ())
        .expect_err("a zero member id must fail validation");
    assert_eq!(error.code(), ErrorCode::Validation);
}

#[tokio::test]
async fn serve_until_campaigns_serves_probes_and_stops_cleanly() {
    let config = in_memory_config(1, 19_700);
    let cluster = RaftHttpCluster::builder(config)
        .state_machine(NullMachine)
        .with_request_timeout(Duration::from_millis(250))
        .with_peer_naming(|id| format!("probe-node-{id}"))
        .with_client(reqwest::Client::new())
        .build()
        .expect("cluster must build");

    assert_eq!(cluster.runtime().id(), 1);
    assert!(!cluster.coordinator().is_leader());
    let _router: Router = cluster.router();

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener binds");
    let address = listener.local_addr().expect("local address");
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let serving = tokio::spawn(async move {
        cluster
            .serve_until(listener, Router::new(), async move {
                let _ = stopped.await;
            })
            .await
    });

    // The single node elects itself once serving has begun.
    let client = reqwest::Client::new();
    let mut status = None;
    for attempt in 0..100 {
        match client
            .get(format!("http://{address}{RAFT_HTTP_STATUS_PATH}"))
            .send()
            .await
        {
            Ok(response) if response.status() == StatusCode::OK => {
                let body: serde_json::Value = response.json().await.expect("status JSON");
                if body["is_leader"] == serde_json::json!(true) {
                    status = Some(body);
                    break;
                }
            }
            _ if attempt < 99 => tokio::time::sleep(Duration::from_millis(20)).await,
            other => panic!("status probe must come up: {other:?}"),
        }
    }
    let status = status.expect("the node must elect itself");
    assert_eq!(status["raft_node_id"], serde_json::json!(1));
    assert_eq!(status["raft_alive"], serde_json::json!(true));
    assert_eq!(
        status["leader_endpoint"],
        serde_json::json!("http://127.0.0.1:19700")
    );
    assert!(status["applied_index"].is_number());

    let health = client
        .get(format!("http://{address}{RAFT_HTTP_HEALTH_PATH}"))
        .send()
        .await
        .expect("health probe succeeds");
    assert_eq!(health.status(), StatusCode::OK);

    let _ = stop.send(());
    serving
        .await
        .expect("server task must not panic")
        .expect("serve_until must stop cleanly");
}
