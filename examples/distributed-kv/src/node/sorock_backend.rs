//! Sorock branch: sorock multi-Raft over gRPC. The gRPC server (bound by
//! [`SorockRuntimeBuilder`]) carries all
//! consensus traffic; the axum server on the API port serves only the KV
//! routes and probes — sorock speaks gRPC, so there is no inbound consensus
//! HTTP route, and proposals reach the leader inside sorock's transport.

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use axum::{Json, Router, http::StatusCode, routing::get};
use catga_axum::{RAFT_HTTP_HEALTH_PATH, RAFT_HTTP_STATUS_PATH, shutdown_signal};
use catga_core::{CatgaResult, ConsensusCoordinator, ConsensusRuntime};
use catga_sorock::{SorockRuntimeBuilder, SorockStorage};
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use super::{KvRuntime, api_and_mediator, map_err, read_route};
use crate::messages::{PutPipeline, write_route};
use crate::state::{KvMachine, SharedState};
use crate::{Args, K8sTopology};

/// `/status` payload on the sorock backend.
///
/// sorock 0.12 exposes no leadership query, so `is_leader` is always `false`
/// and `leader_endpoint` always `null` (documented in the catga-sorock crate
/// docs); `/healthz` still reflects runtime liveness.
#[derive(Serialize)]
struct SorockStatus {
    backend: &'static str,
    node_id: String,
    is_leader: bool,
    leader_endpoint: Option<String>,
    alive: bool,
    applied_index: Option<u64>,
}

/// Liveness/readiness probes for the sorock backend, mounted on the same
/// paths the raft branch serves via `RaftHttpCluster` so operators see one
/// contract.
fn sorock_probes(runtime: Arc<KvRuntime>, coordinator: Arc<dyn ConsensusCoordinator>) -> Router {
    let healthz = {
        let runtime = Arc::clone(&runtime);
        move || {
            let runtime = Arc::clone(&runtime);
            async move {
                if runtime.is_alive() {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            }
        }
    };
    let status = {
        move || {
            let runtime = Arc::clone(&runtime);
            let coordinator = Arc::clone(&coordinator);
            async move {
                Json(SorockStatus {
                    backend: "sorock",
                    node_id: coordinator.node_id().to_owned(),
                    is_leader: coordinator.is_leader(),
                    leader_endpoint: coordinator
                        .leader_endpoint()
                        .map(|leader| leader.to_string()),
                    alive: runtime.is_alive(),
                    applied_index: runtime.applied_index().await.ok(),
                })
            }
        }
    };
    Router::new()
        .route(RAFT_HTTP_HEALTH_PATH, get(healthz))
        .route(RAFT_HTTP_STATUS_PATH, get(status))
}

pub(super) async fn run_sorock(
    args: &Args,
    k8s: Option<K8sTopology>,
    state: Arc<SharedState>,
) -> CatgaResult<()> {
    let (builder, member_id) = match k8s {
        Some(topology) => {
            let ordinal = topology.ordinal();
            let builder =
                SorockRuntimeBuilder::from_cli(topology.api_port, ordinal, topology.replicas)?
                    .with_api_host("0.0.0.0")
                    .with_api_port(topology.api_port)
                    .with_peers(
                        (0..topology.replicas)
                            .filter(|i| *i != ordinal)
                            .map(|i| (i + 1, topology.grpc_uri(i))),
                    )
                    .with_node_config(|config| {
                        config.bind_addr = SocketAddr::from(([0, 0, 0, 0], topology.grpc_port));
                        config.public_uri = Some(topology.grpc_uri(ordinal));
                        config.storage =
                            SorockStorage::RedbFile(PathBuf::from("/data/sorock/raft.redb"));
                    });
            (builder, topology.raft_node_id)
        }
        None => (
            SorockRuntimeBuilder::from_cli(args.base_port, args.node, args.nodes)?,
            args.node + 1,
        ),
    };
    let addr = builder.api_addr();
    let runtime = Arc::new(builder.start(KvMachine::new(Arc::clone(&state))).await?);
    let coordinator = runtime.coordinator();
    let consensus_runtime = Arc::new(KvRuntime::Sorock(Arc::clone(&runtime)));
    let (app_state, _mediator) = api_and_mediator(
        Arc::clone(&consensus_runtime),
        Arc::clone(&state),
        member_id,
        PutPipeline::local(),
    )?;
    let routes = Router::new()
        .merge(read_route())
        .merge(write_route())
        .with_state(app_state)
        .merge(sorock_probes(consensus_runtime, coordinator));

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(map_err)?;
    tracing::info!(
        %addr,
        grpc_uri = %runtime.advertised_uri(),
        member_id,
        "distributed-kv node listening (sorock backend)"
    );

    // Serve before the group finishes forming: sorock adds members
    // imperatively and peers may still be starting.
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, routes)
            .with_graceful_shutdown(async move { server_shutdown.cancelled().await })
            .await
            .map_err(|error| map_err(format!("sorock http server: {error}")))
    });

    shutdown_signal().await;
    shutdown.cancel();
    runtime.shutdown();
    let http_result = server
        .await
        .map_err(|error| map_err(format!("sorock http server task: {error}")))?;
    // All other runtime clones live inside the just-drained router, so this
    // normally unwraps and joins the gRPC server gracefully.
    if let Ok(runtime) = Arc::try_unwrap(runtime) {
        runtime.join().await?;
    }
    http_result
}
