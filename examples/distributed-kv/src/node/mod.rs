//! Node wiring: consensus backend bootstrap, mediator pipeline, and shared types.
//!
//! The KV application (state machine, routes, bench) depends only on the
//! `catga-core` consensus traits; [`raft_backend`] wires
//! [`RaftHttpCluster`](catga_axum::RaftHttpCluster) (raft-rs over HTTP, with
//! leader-forward writes) and [`sorock_backend`] wires
//! [`SorockRuntimeBuilder`](catga_sorock::SorockRuntimeBuilder) (sorock
//! multi-Raft over gRPC, forwarding inside its transport).

mod raft_backend;
mod sorock_backend;

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::get,
};
use catga_cluster::RaftStateMachineRuntime;
use catga_core::{
    CatgaError, CatgaResult, ConsensusCoordinator, ConsensusRuntime, ErrorCode, Mediator,
};
use catga_sorock::SorockRuntime;
use serde::Serialize;

use crate::messages::{KvService, PutPipeline};
use crate::state::SharedState;
use crate::{Args, Backend, K8sTopology};

// ---------------------------------------------------------------------------
// HTTP API surface shared by both backends
// ---------------------------------------------------------------------------

/// Shared HTTP handler state.
#[derive(Clone)]
pub(crate) struct ApiState {
    pub mediator: Arc<Mediator>,
    pub pipeline: Arc<PutPipeline>,
    pub state: Arc<SharedState>,
}

#[derive(Serialize)]
struct KvEntry {
    key: String,
    value: String,
}

pub(crate) fn error_response(error: CatgaError) -> (StatusCode, String) {
    let status = match error.code() {
        ErrorCode::Validation => StatusCode::BAD_REQUEST,
        ErrorCode::Timeout | ErrorCode::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::Conflict => StatusCode::CONFLICT,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, error.to_string())
}

async fn get_kv(
    State(app): State<ApiState>,
    Path(key): Path<String>,
) -> Result<Json<KvEntry>, StatusCode> {
    app.state
        .get(&key)
        .map(|value| Json(KvEntry { key, value }))
        .ok_or(StatusCode::NOT_FOUND)
}

/// The read endpoint both backends mount on their API router.
pub(crate) fn read_route() -> Router<ApiState> {
    Router::new().route("/kv/{key}", get(get_kv))
}

// ---------------------------------------------------------------------------
// Bench
// ---------------------------------------------------------------------------

/// Measures sequential write latency against one node with a keep-alive client.
pub(crate) async fn bench(addr: &str, writes: u32) -> CatgaResult<()> {
    let client = reqwest::Client::new();
    let url = format!("http://{addr}/kv");
    let mut latencies = Vec::with_capacity(writes as usize);
    let started = std::time::Instant::now();
    for i in 0..writes {
        let body = format!(r#"{{"key":"bench-{i}","value":"v{i}"}}"#);
        let one = std::time::Instant::now();
        let response = client
            .post(&url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(map_err)?;
        if !response.status().is_success() {
            return Err(CatgaError::new(
                ErrorCode::Unavailable,
                format!("bench write {i} failed with HTTP {}", response.status()),
            ));
        }
        latencies.push(one.elapsed());
    }
    let total = started.elapsed();
    latencies.sort_unstable();
    let pick = |p: f64| {
        latencies
            .get(((latencies.len() - 1) as f64 * p) as usize)
            .copied()
            .unwrap_or_default()
    };
    let mean = total / writes.max(1);
    println!(
        "writes={writes} total={total:.2?} throughput={:.0}/s mean={mean:.2?} p50={:.2?} p99={:.2?}",
        f64::from(writes) / total.as_secs_f64(),
        pick(0.50),
        pick(0.99),
    );
    Ok(())
}

/// Backend-erased consensus runtime handle for the KV application.
///
/// [`ConsensusRuntime`] is not dyn-compatible (its async methods return `impl
/// Future`), so application code cannot hold `Arc<dyn ConsensusRuntime>`. It
/// holds this enum instead — itself nothing but a `ConsensusRuntime` — and
/// never names a concrete backend.
pub(crate) enum KvRuntime {
    /// raft-rs runtime (catga-cluster), serving consensus over HTTP.
    Raft(Arc<RaftStateMachineRuntime>),
    /// sorock multi-Raft runtime (catga-sorock), serving consensus over gRPC.
    Sorock(Arc<SorockRuntime>),
}

// Thin hand-written delegation: every method matches on the variant and
// forwards to the wrapped runtime — the irreducible cost of enum dispatch
// over a non-dyn-safe (RPITIT) trait.
impl ConsensusRuntime for KvRuntime {
    async fn propose(&self, data: Vec<u8>) -> CatgaResult<()> {
        match self {
            Self::Raft(runtime) => ConsensusRuntime::propose(&**runtime, data).await,
            Self::Sorock(runtime) => ConsensusRuntime::propose(&**runtime, data).await,
        }
    }

    async fn add_member(&self, id: u64, endpoint: String) -> CatgaResult<()> {
        match self {
            Self::Raft(runtime) => ConsensusRuntime::add_member(&**runtime, id, endpoint).await,
            Self::Sorock(runtime) => ConsensusRuntime::add_member(&**runtime, id, endpoint).await,
        }
    }

    async fn remove_member(&self, id: u64) -> CatgaResult<()> {
        match self {
            Self::Raft(runtime) => ConsensusRuntime::remove_member(&**runtime, id).await,
            Self::Sorock(runtime) => ConsensusRuntime::remove_member(&**runtime, id).await,
        }
    }

    async fn applied_index(&self) -> CatgaResult<u64> {
        match self {
            Self::Raft(runtime) => ConsensusRuntime::applied_index(&**runtime).await,
            Self::Sorock(runtime) => ConsensusRuntime::applied_index(&**runtime).await,
        }
    }

    fn is_alive(&self) -> bool {
        match self {
            Self::Raft(runtime) => ConsensusRuntime::is_alive(&**runtime),
            Self::Sorock(runtime) => ConsensusRuntime::is_alive(&**runtime),
        }
    }

    fn coordinator(&self) -> Arc<dyn ConsensusCoordinator> {
        match self {
            Self::Raft(runtime) => ConsensusRuntime::coordinator(&**runtime),
            Self::Sorock(runtime) => ConsensusRuntime::coordinator(&**runtime),
        }
    }

    fn shutdown(&self) {
        match self {
            Self::Raft(runtime) => ConsensusRuntime::shutdown(&**runtime),
            Self::Sorock(runtime) => ConsensusRuntime::shutdown(&**runtime),
        }
    }

    async fn join(self) -> CatgaResult<()> {
        // The owned runtimes sit behind `Arc` handles shared with the router
        // and mediator; join only when this handle is the last one, mirroring
        // the best-effort join in `RaftHttpCluster::serve_until`.
        match self {
            Self::Raft(runtime) => match Arc::try_unwrap(runtime) {
                Ok(runtime) => ConsensusRuntime::join(runtime).await,
                Err(_) => Ok(()),
            },
            Self::Sorock(runtime) => match Arc::try_unwrap(runtime) {
                Ok(runtime) => ConsensusRuntime::join(runtime).await,
                Err(_) => Ok(()),
            },
        }
    }
}

/// Wires the mediator write path shared by both backends: the service proposes
/// through the backend-agnostic runtime handle; only the pipeline differs
/// (HTTP leader forwarding on raft, behavior-free local execution on sorock).
fn api_and_mediator(
    runtime: Arc<KvRuntime>,
    state: Arc<SharedState>,
    node_id: u64,
    pipeline: PutPipeline,
) -> CatgaResult<(ApiState, Arc<Mediator>)> {
    let service = KvService::new(runtime, Arc::clone(&state), node_id);
    let mediator = Arc::new(Mediator::new(service.registry()?));
    let api = ApiState {
        mediator: Arc::clone(&mediator),
        pipeline: Arc::new(pipeline),
        state,
    };
    Ok((api, mediator))
}

/// Runs one KV node until SIGINT/SIGTERM: consensus, HTTP API, leader duties.
pub(crate) async fn run(args: Args, k8s: Option<K8sTopology>) -> CatgaResult<()> {
    let state = Arc::new(SharedState::new());
    match args.backend {
        Backend::Raft => raft_backend::run_raft(&args, k8s, state).await,
        Backend::Sorock => sorock_backend::run_sorock(&args, k8s, state).await,
    }
}

fn map_err(error: impl std::fmt::Display) -> CatgaError {
    CatgaError::new(ErrorCode::Internal, error.to_string())
}
