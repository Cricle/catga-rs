//! Raft branch: raft-rs over HTTP. The API port also carries the inbound Raft
//! route, the leader-forward route, and the probes (all from the
//! [`RaftHttpCluster`] router).

use std::{sync::Arc, time::Duration};

use axum::Router;
use catga_axum::{HttpClusterForwarder, RaftHttpCluster};
use catga_cluster::{RaftClusterConfig, SingletonTaskRunner};
use catga_core::CatgaResult;
use tokio_util::sync::CancellationToken;

use super::{KvRuntime, api_and_mediator, map_err, read_route};
use crate::messages::{PutPipeline, forward_router, write_route};
use crate::state::{KvMachine, SharedState};
use crate::{Args, K8sTopology};

const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(10);
const FORWARD_HTTP_TIMEOUT: Duration = Duration::from_secs(10);

pub(super) async fn run_raft(
    args: &Args,
    k8s: Option<K8sTopology>,
    state: Arc<SharedState>,
) -> CatgaResult<()> {
    let (raft_node_id, addr, config) = match k8s {
        Some(topology) => (
            topology.raft_node_id,
            format!("0.0.0.0:{}", topology.api_port),
            topology.config,
        ),
        None => (
            args.node + 1,
            format!("127.0.0.1:{}", args.base_port + args.node as u16),
            local_raft_config(args)?,
        ),
    };
    let cluster = RaftHttpCluster::builder_with_core_sm(config, KvMachine::new(Arc::clone(&state)))
        .build()?;

    let forward_client = reqwest::Client::builder()
        .timeout(FORWARD_HTTP_TIMEOUT)
        .build()
        .map_err(map_err)?;
    let pipeline = PutPipeline::new(
        Arc::clone(cluster.coordinator()),
        Arc::new(HttpClusterForwarder::new(forward_client)),
    );
    let (app_state, mediator) = api_and_mediator(
        Arc::new(KvRuntime::Raft(Arc::clone(cluster.runtime()))),
        Arc::clone(&state),
        raft_node_id,
        pipeline,
    )?;
    let routes = Router::new()
        .merge(read_route())
        .merge(write_route())
        .with_state(app_state)
        .merge(forward_router(mediator));

    // Leader duty: checkpoint periodically while holding leadership.
    let shutdown = CancellationToken::new();
    let singleton = SingletonTaskRunner::new(Arc::clone(cluster.coordinator()));
    let (checkpoint_runtime, checkpoint_state, singleton_shutdown) = (
        Arc::clone(cluster.runtime()),
        Arc::clone(&state),
        shutdown.clone(),
    );
    let singleton_task = tokio::spawn(async move {
        singleton
            .run(singleton_shutdown, move |leadership_lost| {
                let runtime = Arc::clone(&checkpoint_runtime);
                let state = Arc::clone(&checkpoint_state);
                async move {
                    loop {
                        tokio::select! {
                            _ = leadership_lost.cancelled() => break,
                            () = tokio::time::sleep(CHECKPOINT_INTERVAL) => {
                                // Nothing to snapshot before the first applied command.
                                if state.applied_index() == 0 {
                                    continue;
                                }
                                match runtime.checkpoint().await {
                                    Ok(()) => tracing::info!("raft checkpoint written"),
                                    Err(error) => tracing::warn!(%error, "raft checkpoint failed"),
                                }
                            }
                        }
                    }
                }
            })
            .await;
    });

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(map_err)?;
    tracing::info!(%addr, raft_node_id, "distributed-kv node listening (raft backend)");
    // serve() accepts connections before campaigning, drains on SIGINT/SIGTERM,
    // then stops the Raft runtime.
    let result = cluster.serve(listener, routes).await;
    shutdown.cancel();
    let _ = singleton_task.await;
    result
}

/// Local-mode cluster config with a state directory keyed by base port as well
/// as node index, so a second local cluster on a different port range never
/// picks up the first cluster's stale state. The config fields are private
/// (Deserialize only), so this goes through the same serde_json derivation as
/// the k8s topology.
fn local_raft_config(args: &Args) -> CatgaResult<RaftClusterConfig> {
    let endpoint_of = |i: u64| format!("http://localhost:{}", args.base_port + i as u16);
    let members: Vec<serde_json::Value> = (0..args.nodes)
        .filter(|i| *i != args.node)
        .map(|i| serde_json::json!({ "id": i + 1, "endpoint": endpoint_of(i) }))
        .collect();
    serde_json::from_value(serde_json::json!({
        "nodeId": args.node + 1,
        "localNodeEndpoint": endpoint_of(args.node),
        "members": members,
        "persistentStatePath": format!("./raft-state-node{}-p{}", args.node, args.base_port),
    }))
    .map_err(map_err)
}
