//! gRPC server for the raft wire transport.
//!
//! Exposes the generated `catga.raft.Raft` service: each incoming payload is
//! decoded into a `raft::prelude::Message` and handed to the step callback
//! that feeds the local raft node.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::task::JoinHandle;
use tonic::{Request, Response, Status};

use crate::{CatgaRaftError, CatgaRaftResult};

use super::proto_gen::pb::{self, raft_server::RaftServer};

/// Maximum size of a single gRPC message on the raft wire (8 MiB).
///
/// tonic's server-side decode limit defaults to 4 MiB, while
/// `CatgaRaftConfig::max_size_per_msg` used to default to 64 MiB — any
/// append batch between the two failed on the wire. Both sides are now
/// aligned on this constant: the server decodes up to this size explicitly,
/// and the config default sits at the same bound.
pub const MAX_GRPC_MESSAGE_SIZE: usize = 8 * 1024 * 1024;

/// gRPC service implementation for the raft transport.
///
/// Decodes `RaftMessage` payloads into `raft::prelude::Message` values and
/// forwards them to the step callback. Decode failures are reported as
/// `INVALID_ARGUMENT`, step callback errors as `INTERNAL`.
pub struct RaftGrpcService<F>
where
    F: Fn(raft::prelude::Message) -> CatgaRaftResult<()> + Send + Sync + 'static,
{
    step: Arc<F>,
}

impl<F> RaftGrpcService<F>
where
    F: Fn(raft::prelude::Message) -> CatgaRaftResult<()> + Send + Sync + 'static,
{
    /// Create a service that forwards decoded raft messages to `step`.
    pub fn new(step: F) -> Self {
        Self {
            step: Arc::new(step),
        }
    }
}

#[tonic::async_trait]
impl<F> pb::raft_server::Raft for RaftGrpcService<F>
where
    F: Fn(raft::prelude::Message) -> CatgaRaftResult<()> + Send + Sync + 'static,
{
    async fn step(
        &self,
        request: Request<pb::RaftMessage>,
    ) -> Result<Response<pb::StepReply>, Status> {
        let raft_msg = decode_payload(&request.into_inner().payload)?;
        (self.step)(raft_msg).map_err(step_status)?;
        Ok(Response::new(pb::StepReply::default()))
    }

    async fn step_batch(
        &self,
        request: Request<pb::RaftMessageBatch>,
    ) -> Result<Response<pb::StepReply>, Status> {
        for payload in &request.into_inner().payloads {
            let raft_msg = decode_payload(payload)?;
            (self.step)(raft_msg).map_err(step_status)?;
        }
        Ok(Response::new(pb::StepReply::default()))
    }
}

/// Decode one opaque payload into a raft message.
#[allow(clippy::result_large_err)]
fn decode_payload(payload: &[u8]) -> Result<raft::prelude::Message, Status> {
    <raft::prelude::Message as protobuf::Message>::parse_from_bytes(payload)
        .map_err(|e| Status::invalid_argument(format!("invalid raft message payload: {}", e)))
}

/// Map a step callback error onto an `INTERNAL` status carrying the message.
fn step_status(err: CatgaRaftError) -> Status {
    Status::internal(format!("step failed: {}", err))
}

/// Bind `addr` and serve the raft gRPC transport.
///
/// Returns a `JoinHandle` the runtime can abort to stop the server. Use
/// [`serve_with_bound_addr`] when the caller also needs the actually bound
/// address (e.g. when binding port 0).
///
/// # Errors
///
/// Returns [`CatgaRaftError::Transport`] if binding the listener fails.
pub async fn serve<F>(
    addr: SocketAddr,
    service: RaftGrpcService<F>,
) -> CatgaRaftResult<JoinHandle<()>>
where
    F: Fn(raft::prelude::Message) -> CatgaRaftResult<()> + Send + Sync + 'static,
{
    let (_, handle) = serve_with_bound_addr(addr, service).await?;
    Ok(handle)
}

/// Bind `addr` and serve the raft gRPC transport, stopping when `shutdown`
/// resolves.
///
/// Returns a `JoinHandle` that completes once the server has drained.
///
/// # Errors
///
/// Returns [`CatgaRaftError::Transport`] if binding the listener fails.
pub async fn serve_with_shutdown<F, S>(
    addr: SocketAddr,
    service: RaftGrpcService<F>,
    shutdown: S,
) -> CatgaRaftResult<JoinHandle<()>>
where
    F: Fn(raft::prelude::Message) -> CatgaRaftResult<()> + Send + Sync + 'static,
    S: std::future::Future<Output = ()> + Send + 'static,
{
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| CatgaRaftError::Transport(format!("failed to bind {}: {}", addr, e)))?;
    let bound = listener.local_addr().map_err(|e| {
        CatgaRaftError::Transport(format!("failed to resolve bound address: {}", e))
    })?;
    let incoming = tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
        .map_err(|e| CatgaRaftError::Transport(format!("failed to serve {}: {}", bound, e)))?;

    let handle = tokio::spawn(async move {
        tracing::info!(addr = %bound, "raft gRPC transport listening");
        let result = tonic::transport::Server::builder()
            .add_service(
                // tonic's default decode limit (4 MiB) is below the raft
                // wire bound; raise it explicitly so large append batches
                // reach the step callback instead of failing on the wire.
                RaftServer::new(service).max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE),
            )
            .serve_with_incoming_shutdown(incoming, shutdown)
            .await;
        match result {
            Ok(()) => tracing::info!(addr = %bound, "raft gRPC server stopped"),
            Err(e) => tracing::error!(addr = %bound, error = %e, "raft gRPC server exited"),
        }
    });

    Ok(handle)
}

/// Bind `addr` and serve the raft gRPC transport, reporting the bound address.
///
/// Returns the actually bound address (which differs from `addr` when port 0
/// is requested) together with a `JoinHandle` the runtime can abort to stop
/// the server.
///
/// # Errors
///
/// Returns [`CatgaRaftError::Transport`] if binding the listener fails.
pub async fn serve_with_bound_addr<F>(
    addr: SocketAddr,
    service: RaftGrpcService<F>,
) -> CatgaRaftResult<(SocketAddr, JoinHandle<()>)>
where
    F: Fn(raft::prelude::Message) -> CatgaRaftResult<()> + Send + Sync + 'static,
{
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| CatgaRaftError::Transport(format!("failed to bind {}: {}", addr, e)))?;
    let bound = listener.local_addr().map_err(|e| {
        CatgaRaftError::Transport(format!("failed to resolve bound address: {}", e))
    })?;
    let incoming = tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
        .map_err(|e| CatgaRaftError::Transport(format!("failed to serve {}: {}", bound, e)))?;

    let handle = tokio::spawn(async move {
        tracing::info!(addr = %bound, "raft gRPC transport listening");
        // Shutdown is driven by aborting the returned handle; the pending
        // future keeps the graceful-shutdown hook inert until then.
        let result = tonic::transport::Server::builder()
            .add_service(RaftServer::new(service).max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE))
            .serve_with_incoming_shutdown(incoming, futures::future::pending::<()>())
            .await;
        match result {
            Ok(()) => tracing::info!(addr = %bound, "raft gRPC server stopped"),
            Err(e) => tracing::error!(addr = %bound, error = %e, "raft gRPC server exited"),
        }
    });

    Ok((bound, handle))
}
