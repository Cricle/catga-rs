//! Public write path: request type, leader-side mediator service, and route builders.
//!
//! `PutKv` stays module-private because `catga_request` generates a private `{Name}TypeId`
//! type; a `pub(crate)` message would leak it through the `Request::TypeId` associated type
//! (E0446). Cross-module use goes through the opaque wrappers below.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use catga_axum::{HttpClusterForwarder, leader_forward_route};
use catga_cluster::{ClusterCoordinator, ForwardToLeaderBehavior};
use catga_core::{CatgaError, CatgaResult, ConsensusRuntime, ErrorCode, Mediator, Pipeline};
use serde::{Deserialize, Serialize};

use crate::node::{ApiState, KvRuntime};
use crate::state::{KvCommand, SharedState};

const APPLY_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
const FORWARD_MAX_ATTEMPTS: usize = 50;
const FORWARD_RETRY_DELAY: Duration = Duration::from_millis(100);

#[catga_core::catga_request(response = PutKvResult)]
#[derive(Clone, Debug, Serialize, Deserialize)]
struct PutKv {
    key: String,
    value: String,
}

/// Result of a replicated write once it is applied locally on the leader.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PutKvResult {
    pub op_id: u64,
    pub key: String,
    pub value: String,
    pub applied_index: u64,
}

/// Inbound JSON body for the public write endpoint.
#[derive(Deserialize)]
pub(crate) struct PutKvBody {
    key: String,
    value: String,
}

/// Mediator service that turns validated writes into consensus proposals.
///
/// It talks to whichever backend `node.rs` wired in, purely through the
/// `catga-core` [`ConsensusRuntime`] contract ([`KvRuntime`] is the
/// backend-erased handle; the trait's RPITIT methods rule out `dyn`).
#[derive(Clone)]
pub(crate) struct KvService {
    runtime: Arc<KvRuntime>,
    state: Arc<SharedState>,
    node_id: u64,
    op_counter: Arc<AtomicU64>,
}

impl KvService {
    pub(crate) fn new(runtime: Arc<KvRuntime>, state: Arc<SharedState>, node_id: u64) -> Self {
        Self {
            runtime,
            state,
            node_id,
            op_counter: Arc::new(AtomicU64::new(1)),
        }
    }

    fn next_op_id(&self) -> u64 {
        (self.node_id << 56) | self.op_counter.fetch_add(1, Ordering::Relaxed)
    }
}

#[catga_core::catga_service]
impl KvService {
    async fn put(&self, msg: PutKv) -> CatgaResult<PutKvResult> {
        if msg.key.is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "key must not be empty",
            ));
        }
        let op_id = self.next_op_id();
        let command = KvCommand::Put {
            op_id,
            key: msg.key.clone(),
            value: msg.value.clone(),
        };
        let payload = serde_json::to_vec(&command).map_err(|error| {
            CatgaError::new(
                ErrorCode::SerializationFailed,
                format!("kv command encode failed: {error}"),
            )
        })?;
        self.runtime.propose(payload).await.map_err(|error| {
            CatgaError::new(
                ErrorCode::Unavailable,
                format!("consensus proposal rejected: {error}"),
            )
        })?;
        if !self.state.wait_applied(op_id, APPLY_WAIT_TIMEOUT).await {
            return Err(CatgaError::new(
                ErrorCode::Timeout,
                "write was not applied before the deadline",
            ));
        }
        Ok(PutKvResult {
            op_id,
            key: msg.key,
            value: msg.value,
            applied_index: self.state.applied_index(),
        })
    }
}

/// Hides the concrete `Pipeline<PutKv>` so the private request type stays in this module.
pub(crate) struct PutPipeline(Pipeline<PutKv>);

impl PutPipeline {
    /// Raft-style pipeline: writes landing on a follower are forwarded over
    /// HTTP to the elected leader ([`ForwardToLeaderBehavior`]).
    pub(crate) fn new<C>(coordinator: Arc<C>, forwarder: Arc<HttpClusterForwarder>) -> Self
    where
        C: ClusterCoordinator + 'static,
    {
        Self(
            Pipeline::new().with(
                ForwardToLeaderBehavior::new(coordinator, forwarder)
                    .with_retry(FORWARD_MAX_ATTEMPTS, FORWARD_RETRY_DELAY),
            ),
        )
    }

    /// Behavior-free pipeline for backends that reach the leader on their own:
    /// sorock forwards proposals to the current leader inside its gRPC
    /// transport, and its coordinator cannot observe leadership, so any
    /// HTTP-level forwarding behavior would only fail.
    pub(crate) fn local() -> Self {
        Self(Pipeline::new())
    }

    pub(crate) async fn put(
        &self,
        mediator: &Mediator,
        key: String,
        value: String,
    ) -> CatgaResult<PutKvResult> {
        mediator.send_with(PutKv { key, value }, &self.0).await
    }
}

/// Registers the public write endpoint on the shared API router.
pub(crate) fn write_route() -> Router<ApiState> {
    Router::new().route("/kv", post(put_kv))
}

/// Builds the leader-side forwarding endpoint for replicated writes.
pub(crate) fn forward_router(mediator: Arc<Mediator>) -> Router {
    leader_forward_route::<PutKv>(mediator)
}

async fn put_kv(
    State(app): State<ApiState>,
    Json(body): Json<PutKvBody>,
) -> Result<Json<PutKvResult>, (StatusCode, String)> {
    app.pipeline
        .put(&app.mediator, body.key, body.value)
        .await
        .map(Json)
        .map_err(crate::node::error_response)
}
