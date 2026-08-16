//! gRPC fast path for the KV API: the binary alternative to the HTTP/JSON
//! routes (`node/kv_grpc.rs` serves what `messages.rs` + `node/mod.rs` serve
//! over axum).
//!
//! Semantics are identical to the HTTP path: `Put`/`PutBatch` are
//! acknowledged only after raft commit + local apply (linearizable writes,
//! followers forward proposals through the raft runtime), and `Get` serves a
//! ReadIndex-barriered linearizable read. A missing key is `found = false`,
//! not an error, mirroring how `GET /kv/{key}` distinguishes 404 from 5xx.

use std::sync::Arc;
use std::time::Duration;

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use tonic::{Request, Response, Status};

use super::ApiState;
use crate::messages::KvService;
use crate::state::SharedState;

/// Generated bindings for the `distributed.kv` package (`proto/kv.proto`,
/// compiled by `build.rs`); never edit by hand.
pub(crate) mod proto {
    tonic::include_proto!("distributed.kv");
}

use proto::kv_server::{Kv, KvServer};
use proto::{GetReply, GetRequest, PutBatchReply, PutBatchRequest, PutReply, PutRequest};

/// How long a linearizable read waits for the state machine to catch up to
/// the ReadIndex commit point (same budget as the HTTP read path).
const READ_WAIT_TIMEOUT: Duration = Duration::from_secs(2);

/// gRPC face of the KV node: a thin adapter from the generated [`Kv`] trait
/// onto the shared [`KvService`] write path and [`SharedState`] read model.
pub(crate) struct KvGrpcService {
    service: Arc<KvService>,
    state: Arc<SharedState>,
}

/// Maps a Catga failure onto the matching tonic status, keeping the message.
fn grpc_status(error: CatgaError) -> Status {
    let message = error.to_string();
    match error.code() {
        ErrorCode::Validation => Status::invalid_argument(message),
        ErrorCode::Timeout => Status::deadline_exceeded(message),
        ErrorCode::Unavailable => Status::unavailable(message),
        ErrorCode::Conflict => Status::aborted(message),
        _ => Status::internal(message),
    }
}

#[tonic::async_trait]
impl Kv for KvGrpcService {
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutReply>, Status> {
        let request = request.into_inner();
        let result = self
            .service
            .put(request.key, request.value)
            .await
            .map_err(grpc_status)?;
        Ok(Response::new(PutReply {
            op_id: result.op_id,
            key: result.key,
            value: result.value,
            applied_index: result.applied_index,
        }))
    }

    async fn put_batch(
        &self,
        request: Request<PutBatchRequest>,
    ) -> Result<Response<PutBatchReply>, Status> {
        let request = request.into_inner();
        // Empty and over-cap batches fall through to `put_batch`'s own
        // validation so both API surfaces reject identically.
        let items = request
            .items
            .into_iter()
            .map(|item| (item.key, item.value))
            .collect();
        let result = self.service.put_batch(items).await.map_err(grpc_status)?;
        Ok(Response::new(PutBatchReply {
            applied: result.applied as u64,
            applied_index: result.applied_index,
        }))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetReply>, Status> {
        let request = request.into_inner();
        // Linearizable read (TiKV-style ReadIndex) via the runtime barrier:
        // quorum-checked commit index, then wait for local apply, then read.
        self.service
            .read_barrier(READ_WAIT_TIMEOUT)
            .await
            .map_err(grpc_status)?;
        let reply = match self.state.get(&request.key) {
            Some(value) => GetReply { key: request.key, value, found: true },
            None => GetReply { key: request.key, value: String::new(), found: false },
        };
        Ok(Response::new(reply))
    }
}

/// Builds the tonic service wrapper around the shared API state.
pub(crate) fn router(api: ApiState) -> KvServer<KvGrpcService> {
    KvServer::new(KvGrpcService { service: api.service, state: api.state })
}

/// One-off gRPC `Get` probe (smoke-check helper): prints what the cluster
/// answered for `key`.
pub(crate) async fn probe(addr: &str, key: &str) -> CatgaResult<()> {
    use proto::kv_client::KvClient;
    let mut client = KvClient::connect(format!("http://{addr}"))
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, format!("grpc connect to {addr}: {error}")))?;
    let reply = client
        .get(Request::new(GetRequest { key: key.to_string() }))
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Unavailable, format!("grpc get {key:?} from {addr}: {error}")))?
        .into_inner();
    println!("found={} key={} value={}", reply.found, reply.key, reply.value);
    Ok(())
}
