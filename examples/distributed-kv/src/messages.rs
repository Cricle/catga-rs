//! Public write path: request type, mediator service, and route builders.
//!
//! Endpoints: `POST /kv` writes one `{"key", "value"}` pair;
//! `POST /kv/batch` writes `{"items": [{"key", "value"}, ...]}` (1 to
//! [`MAX_BATCH_ITEMS`] pairs) in one request and responds with a single
//! `{applied, applied_index}` summary once every item is applied.
//!
//! `PutKv` stays module-private; cross-module use goes through the opaque wrappers below.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use catga_core::{CatgaError, CatgaResult, ConsensusRuntime, ErrorCode};
use catga_raft::CatgaRaftRuntime;
use serde::{Deserialize, Serialize};

use crate::node::ApiState;
use crate::state::{KvCommand, KvMachine};

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

/// Inbound JSON body for the batch write endpoint: one request carries
/// `items.len()` puts.
#[derive(Deserialize)]
pub(crate) struct PutBatchBody {
    items: Vec<PutKvBody>,
}

/// Summary of a replicated batch write once every item is applied locally.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PutBatchResult {
    /// Number of items in the batch (all applied on success).
    pub applied: usize,
    /// Applied raft index after the last item of the batch.
    pub applied_index: u64,
}

/// Maximum number of items in one batch write.
///
/// The cap stays well inside the 4096-op-id dedup window in `state.rs`, so
/// a full batch's op ids cannot be evicted from the window by concurrent
/// traffic before `wait_applied` observes them.
pub(crate) const MAX_BATCH_ITEMS: usize = 1024;

/// Mediator service that turns validated writes into consensus proposals.
///
/// It talks to the catga-raft runtime directly: consensus mutations go
/// through the `catga-core` [`ConsensusRuntime`] contract (object-safe, so
/// the handle could equally be erased behind `Arc<dyn ConsensusRuntime>`),
/// while linearizable reads use the runtime's inherent `read_index`.
///
#[derive(Clone)]
pub(crate) struct KvService {
    runtime: Arc<CatgaRaftRuntime<KvMachine>>,
    node_id: u64,
    state: Arc<crate::state::SharedState>,
    op_counter: Arc<AtomicU64>,
}

/// How long a write waits for the committed entry to be applied locally.
const APPLY_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

impl KvService {
    pub(crate) fn new(
        runtime: Arc<CatgaRaftRuntime<KvMachine>>,
        node_id: u64,
        state: Arc<crate::state::SharedState>,
    ) -> Self {
        Self {
            runtime,
            node_id,
            state,
            op_counter: Arc::new(AtomicU64::new(1)),
        }
    }

    fn next_op_id(&self) -> u64 {
        (self.node_id << 56) | self.op_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Put a key-value pair into the replicated state machine.
    ///
    /// Returns after the entry is committed and applied locally, matching
    /// TiKV's write path; followers forward the proposal to the leader.
    pub(crate) async fn put(&self, key: String, value: String) -> CatgaResult<PutKvResult> {
        if key.is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "key must not be empty",
            ));
        }
        let op_id = self.next_op_id();
        let command = KvCommand::Put {
            op_id,
            key: key.clone(),
            value: value.clone(),
        };
        let payload = command.encode()?;
        self.runtime.propose(payload).await.map_err(|error| {
            CatgaError::new(
                ErrorCode::Unavailable,
                format!("consensus proposal rejected: {error}"),
            )
        })?;
        // TiKV-style: respond only after the committed entry is applied
        // locally, so an immediate read on this node sees the write.
        if !self.state.wait_applied(op_id, APPLY_WAIT_TIMEOUT).await {
            return Err(CatgaError::new(
                ErrorCode::Timeout,
                format!("write committed but not applied within {APPLY_WAIT_TIMEOUT:?}"),
            ));
        }
        Ok(PutKvResult {
            op_id,
            key,
            value,
            applied_index: self.state.applied_index(),
        })
    }

    /// Put a batch of key-value pairs into the replicated state machine.
    ///
    /// Every item is validated up front, then proposed as its own
    /// [`KvCommand::Put`] entry (the same payload format as [`Self::put`]);
    /// back-to-back proposals land in the runtime's append batching, so one
    /// request amortizes the HTTP, JSON, and raft-append overhead over the
    /// whole batch. Returns after the last item is applied locally.
    ///
    /// # Errors
    ///
    /// - [`ErrorCode::Validation`]: empty batch, more than [`MAX_BATCH_ITEMS`]
    ///   items, or an empty key (message names the offending item).
    /// - [`ErrorCode::Unavailable`]: a proposal was rejected (e.g. no leader);
    ///   the message reports how many items were proposed first.
    /// - [`ErrorCode::Timeout`]: the batch was not applied within the wait
    ///   budget.
    pub(crate) async fn put_batch(
        &self,
        items: Vec<(String, String)>,
    ) -> CatgaResult<PutBatchResult> {
        if items.is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "batch must contain at least one item",
            ));
        }
        if items.len() > MAX_BATCH_ITEMS {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                format!(
                    "batch has {} items; at most {MAX_BATCH_ITEMS} are allowed",
                    items.len()
                ),
            ));
        }
        for (position, (key, _)) in items.iter().enumerate() {
            if key.is_empty() {
                return Err(CatgaError::new(
                    ErrorCode::Validation,
                    format!("item {position}: key must not be empty"),
                ));
            }
        }
        // Reserve a contiguous op-id range in one atomic step so concurrent
        // writers cannot interleave their ids inside this batch's range.
        let base_counter = self
            .op_counter
            .fetch_add(items.len() as u64, Ordering::Relaxed);
        let count = items.len();
        let mut proposed = 0usize;
        for (offset, (key, value)) in items.iter().enumerate() {
            let op_id = (self.node_id << 56) | (base_counter + offset as u64);
            let command = KvCommand::Put {
                op_id,
                key: key.clone(),
                value: value.clone(),
            };
            let payload = command.encode()?;
            if let Err(error) = self.runtime.propose(payload).await {
                return Err(CatgaError::new(
                    ErrorCode::Unavailable,
                    format!("consensus proposal rejected after {proposed}/{count} items: {error}"),
                ));
            }
            proposed += 1;
        }
        // Wait only for the LAST op id: proposals enter the log in order and
        // the state machine applies entries monotonically by raft index, so
        // once the final item of the batch is applied every earlier item is
        // applied too. One wait therefore covers the whole batch.
        let last_op_id = (self.node_id << 56) | (base_counter + count as u64 - 1);
        if !self
            .state
            .wait_applied(last_op_id, APPLY_WAIT_TIMEOUT)
            .await
        {
            return Err(CatgaError::new(
                ErrorCode::Timeout,
                format!("batch committed but not applied within {APPLY_WAIT_TIMEOUT:?}"),
            ));
        }
        Ok(PutBatchResult {
            applied: count,
            applied_index: self.state.applied_index(),
        })
    }

    /// Linearizable read barrier against the cluster (works from any node):
    /// quorum-checked commit index, then wait for the local apply.
    pub(crate) async fn read_barrier(&self, timeout: std::time::Duration) -> CatgaResult<u64> {
        self.runtime
            .read_barrier(timeout)
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Unavailable, error.to_string()))
    }

    pub(crate) fn registry(&self) -> catga_core::CatgaResult<catga_core::Registry> {
        let mut registry = catga_core::Registry::new();
        let svc = self.clone();
        registry.register_request::<PutKv, _>(catga_core::request_handler(move |msg: PutKv| {
            let svc = svc.clone();
            async move { svc.put(msg.key, msg.value).await }
        }))?;
        Ok(registry)
    }
}

/// Registers the public write endpoints on the shared API router.
pub(crate) fn write_route() -> Router<ApiState> {
    Router::new()
        .route("/kv", post(put_kv))
        .route("/kv/batch", post(put_kv_batch))
}

async fn put_kv(
    State(app): State<ApiState>,
    Json(body): Json<PutKvBody>,
) -> Result<Json<PutKvResult>, (StatusCode, String)> {
    app.service
        .put(body.key, body.value)
        .await
        .map(Json)
        .map_err(crate::node::error_response)
}

async fn put_kv_batch(
    State(app): State<ApiState>,
    Json(body): Json<PutBatchBody>,
) -> Result<Json<PutBatchResult>, (StatusCode, String)> {
    let items = body
        .items
        .into_iter()
        .map(|item| (item.key, item.value))
        .collect();
    app.service
        .put_batch(items)
        .await
        .map(Json)
        .map_err(crate::node::error_response)
}
