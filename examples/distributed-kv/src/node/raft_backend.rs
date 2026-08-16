//! Raft node wiring: catga-raft multi-Raft over gRPC.

use std::sync::Arc;

use axum::{Json, Router, http::StatusCode, routing::get};
use catga_core::{CatgaResult, ConsensusCoordinator, ConsensusRuntime};
use catga_raft::CatgaRaftRuntimeBuilder;
use serde::Serialize;
use tokio::signal;

use super::{api_and_mediator, kv_grpc, map_err, read_route};
use crate::messages::write_route;
use crate::state::{KvMachine, SharedState};
use crate::{Args, K8sTopology};

const RAFT_HTTP_HEALTH_PATH: &str = "/healthz";
const RAFT_HTTP_STATUS_PATH: &str = "/status";

/// Offset from a pod's raft gRPC endpoint to its KV gRPC endpoint in
/// Kubernetes mode (10100 -> 10500). Pods each own their network identity,
/// so a fixed offset cannot collide; local mode uses the band layout in
/// [`run_raft`] instead.
const KV_GRPC_PORT_OFFSET: u16 = 400;

/// Extracts the port from an endpoint URL such as the builder's raft self
/// endpoint, so the KV gRPC port derives from the same source as the raft
/// endpoint instead of re-deriving the port formula.
fn endpoint_port(endpoint: &str) -> CatgaResult<u16> {
    endpoint
        .rsplit_once(':')
        .and_then(|(_, port)| port.parse().ok())
        .ok_or_else(|| map_err(format!("endpoint {endpoint:?} has no parseable port")))
}

async fn shutdown_signal() {
    let ctrl_c = async { signal::ctrl_c().await.expect("failed to install CTRL+C handler"); };
    #[cfg(unix)]
    let terminate = async { signal::unix::signal(signal::unix::SignalKind::terminate()).expect("failed to install signal handler").recv().await; };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = terminate => {} }
}

#[derive(Serialize)]
struct RaftStatus {
    backend: &'static str,
    node_id: String,
    is_leader: bool,
    leader_endpoint: Option<String>,
    alive: bool,
    applied_index: Option<u64>,
}

fn raft_probes(runtime: Arc<dyn ConsensusRuntime>, coordinator: Arc<dyn ConsensusCoordinator>) -> Router {
    let healthz = {
        let runtime = Arc::clone(&runtime);
        move || {
            let runtime = Arc::clone(&runtime);
            async move { if runtime.is_alive() { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE } }
        }
    };
    let status = {
        move || {
            let runtime = Arc::clone(&runtime);
            let coordinator = Arc::clone(&coordinator);
            async move {
                Json(RaftStatus {
                    backend: "raft",
                    node_id: coordinator.node_id().to_owned(),
                    is_leader: coordinator.is_leader(),
                    leader_endpoint: coordinator.leader_endpoint().map(|l| l.to_string()),
                    alive: runtime.is_alive(),
                    applied_index: runtime.applied_index().await.ok(),
                })
            }
        }
    };
    Router::new().route(RAFT_HTTP_HEALTH_PATH, get(healthz)).route(RAFT_HTTP_STATUS_PATH, get(status))
}

/// Tuned proposal pipeline: wide in-flight window + large batches so batch
/// clients are limited by consensus, not by admission (defaults 64/1024
/// capped batch=128 clients into backpressure retries).
fn tuned_pipeline_config() -> catga_raft::PipelineConfig {
    catga_raft::PipelineConfig {
        batch_size: 256,
        flush_interval: std::time::Duration::from_millis(1),
        max_inflight: 8192,
    }
}

pub(super) async fn run_raft(args: &Args, k8s: Option<K8sTopology>, state: Arc<SharedState>, raft_dir: std::path::PathBuf) -> CatgaResult<()> {
    let (builder, api_port, kv_grpc_port): (CatgaRaftRuntimeBuilder, u16, u16) = match k8s {
        Some(topology) => {
            let ordinal = topology.ordinal();
            let self_endpoint = topology.grpc_uri(ordinal);
            let kv_grpc_port = endpoint_port(&self_endpoint)? + KV_GRPC_PORT_OFFSET;
            let builder = CatgaRaftRuntimeBuilder::from_cli(RAFT_API_PORT, ordinal, topology.replicas)?
                .with_pipeline_config(tuned_pipeline_config())
                .with_members((0..topology.replicas).filter(|i| *i != ordinal).map(|i| (i + 1, topology.grpc_uri(i))).collect())
                .with_self_endpoint(self_endpoint);
            (builder, RAFT_API_PORT, kv_grpc_port)
        }
        None => {
            let builder = CatgaRaftRuntimeBuilder::from_cli(args.base_port, args.node, args.nodes)?
                .with_pipeline_config(tuned_pipeline_config());
            let raft_port = endpoint_port(builder.self_endpoint().ok_or_else(|| map_err("builder has no raft self endpoint"))?)?;
            // Band layout, collision-free for any cluster size N: raft takes
            // [base, base + N*100), the HTTP API band starts at +N*100 and
            // the KV gRPC band at +2N*100, so the three bands are disjoint
            // contiguous blocks of width N*100; inside each band node i sits
            // at offset i*100 with 0 <= i < N, i.e. strictly inside its block.
            // (A fixed raft+400 offset would collide with the HTTP band once
            // N >= 3, hence bands instead of a constant.)
            let band = (args.nodes as u16) * 100;
            (builder, raft_port + band, raft_port + 2 * band)
        }
    };

    let builder = builder.with_data_dir(raft_dir);
    let node_id = builder.config().node_id;
    let addr = format!("0.0.0.0:{}", api_port);

    let kv_machine = KvMachine::new(Arc::clone(&state));
    let runtime = builder.start(kv_machine).await?;
    let runtime: Arc<catga_raft::CatgaRaftRuntime<KvMachine>> = Arc::new(runtime);
    let coordinator = Arc::clone(runtime.coordinator());
    let coordinator: Arc<dyn ConsensusCoordinator> = coordinator;
    // The trait is object-safe: the probes run off the erased handle.
    let consensus_runtime = Arc::clone(&runtime) as Arc<dyn ConsensusRuntime>;
    let (app_state, _mediator) = api_and_mediator(Arc::clone(&runtime), Arc::clone(&state), node_id)?;
    let grpc_service = kv_grpc::router(app_state.clone());

    let routes = Router::new()
        .merge(read_route())
        .merge(write_route())
        .with_state(app_state)
        .merge(raft_probes(Arc::clone(&consensus_runtime), coordinator));

    let listener = tokio::net::TcpListener::bind(&addr).await.map_err(map_err)?;
    let grpc_addr: std::net::SocketAddr = format!("0.0.0.0:{kv_grpc_port}").parse().map_err(map_err)?;
    tracing::info!(%addr, %grpc_addr, node_id, "distributed-kv node listening (raft backend)");

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
    let server = tokio::spawn(async move {
        axum::serve(listener, routes)
            .with_graceful_shutdown(async move { let _ = shutdown_rx.recv().await; })
            .await
            .map_err(|error| map_err(format!("raft http server: {error}")))
    });
    let mut grpc_shutdown_rx = shutdown_tx.subscribe();
    let grpc_server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(grpc_service)
            .serve_with_shutdown(grpc_addr, async move { let _ = grpc_shutdown_rx.recv().await; })
            .await
            .map_err(|error| map_err(format!("kv grpc server: {error}")))
    });

    shutdown_signal().await;
    let _ = shutdown_tx.send(());
    let _ = consensus_runtime.shutdown_and_join().await;
    let http_result = server.await.map_err(|error| map_err(format!("raft http server task: {error}")))?;
    let grpc_result = grpc_server.await.map_err(|error| map_err(format!("kv grpc server task: {error}")))?;
    http_result?;
    grpc_result
}

const RAFT_API_PORT: u16 = 9100;
