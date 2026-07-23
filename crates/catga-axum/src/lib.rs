#![forbid(unsafe_code)]
//! Axum adapters for Catga's framework-independent result types.

use std::{
    collections::HashMap,
    io,
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
};

use async_trait::async_trait;
use axum::{
    Json, Router,
    body::Bytes,
    extract::Request as AxumRequest,
    http::{HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    routing::post,
};
use catga_cluster::{
    ClusterForwarder, RaftMember, RaftMessage, RaftTransport, RaftTransportResult,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode, Mediator, Request};
use protobuf::Message as ProtobufMessage;
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::mpsc;

/// Header used to propagate request correlation identifiers.
pub const CORRELATION_ID_HEADER: &str = "x-correlation-id";

/// HTTP endpoint used to receive raw protobuf Raft protocol messages.
pub const RAFT_MESSAGE_PATH: &str = "/api/catga/raft";

static NEXT_CORRELATION_ID: AtomicU64 = AtomicU64::new(1);

/// HTTP implementation of [`ClusterForwarder`] for Serde request and response types.
pub struct HttpClusterForwarder {
    client: reqwest::Client,
}

impl HttpClusterForwarder {
    /// Creates a forwarder using the supplied reusable HTTP client.
    pub const fn new(client: reqwest::Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl<M> ClusterForwarder<M> for HttpClusterForwarder
where
    M: Request + Serialize,
    M::Response: DeserializeOwned,
{
    async fn forward(&self, request: M, leader_endpoint: &str) -> CatgaResult<M::Response> {
        let request_type = request
            .message_type()
            .rsplit("::")
            .next()
            .unwrap_or("request");
        let url = format!(
            "{}/api/catga/forward/{request_type}",
            leader_endpoint.trim_end_matches('/')
        );
        let response = self
            .client
            .post(url)
            .json(&request)
            .send()
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;
        if !response.status().is_success() {
            return Err(CatgaError::new(
                ErrorCode::Transient,
                format!("leader forwarding failed with status {}", response.status()),
            ));
        }
        response
            .json()
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))
    }
}

/// HTTP implementation of [`RaftTransport`] using compact protobuf protocol frames.
pub struct HttpRaftTransport {
    client: reqwest::Client,
    endpoints: Arc<HashMap<u64, Arc<str>>>,
}

impl HttpRaftTransport {
    /// Creates a transport whose immutable member map routes Raft IDs to endpoints.
    pub fn new<I>(client: reqwest::Client, members: I) -> Self
    where
        I: IntoIterator<Item = RaftMember>,
    {
        Self {
            client,
            endpoints: Arc::new(
                members
                    .into_iter()
                    .map(|member| (member.id(), Arc::from(member.endpoint())))
                    .collect(),
            ),
        }
    }
}

#[async_trait]
impl RaftTransport for HttpRaftTransport {
    async fn send(&self, message: RaftMessage) -> RaftTransportResult {
        let endpoint = self
            .endpoints
            .get(&message.to)
            .ok_or_else(|| io::Error::other(format!("unknown Raft peer {}", message.to)))?;
        let response = self
            .client
            .post(format!(
                "{}{RAFT_MESSAGE_PATH}",
                endpoint.trim_end_matches('/')
            ))
            .header(reqwest::header::CONTENT_TYPE, "application/x-protobuf")
            .body(message.write_to_bytes()?)
            .send()
            .await?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(io::Error::other(format!("Raft peer returned HTTP {}", response.status())).into())
        }
    }
}

/// Builds the server route that decodes protobuf Raft frames into a runtime inbox.
///
/// A full inbox returns HTTP 429 immediately so the sender can apply transport
/// backpressure instead of creating unbounded request tasks.
pub fn raft_message_route(inbox: mpsc::Sender<RaftMessage>) -> Router {
    Router::new().route(
        RAFT_MESSAGE_PATH,
        post(move |body: Bytes| {
            let inbox = inbox.clone();
            async move {
                let message = match RaftMessage::parse_from_bytes(&body) {
                    Ok(message) => message,
                    Err(_) => return StatusCode::BAD_REQUEST,
                };
                match inbox.try_send(message) {
                    Ok(()) => StatusCode::NO_CONTENT,
                    Err(mpsc::error::TrySendError::Full(_)) => StatusCode::TOO_MANY_REQUESTS,
                    Err(mpsc::error::TrySendError::Closed(_)) => StatusCode::SERVICE_UNAVAILABLE,
                }
            }
        }),
    )
}

/// Builds the leader-side forwarding route for one explicitly registered request type.
pub fn leader_forward_route<M>(mediator: Arc<Mediator>) -> Router
where
    M: Request + DeserializeOwned,
    M::Response: Serialize,
{
    let request_type = std::any::type_name::<M>()
        .rsplit("::")
        .next()
        .unwrap_or("request");
    let path = format!("/api/catga/forward/{request_type}");
    Router::new().route(
        &path,
        post(move |Json(message): Json<M>| {
            let mediator = Arc::clone(&mediator);
            async move {
                mediator
                    .send(message)
                    .await
                    .map(Json)
                    .map_err(CatgaHttpError::from)
            }
        }),
    )
}

/// Reads a numeric correlation identifier or allocates a monotonic process-local fallback.
pub fn correlation_id(headers: &axum::http::HeaderMap) -> u64 {
    headers
        .get(CORRELATION_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| NEXT_CORRELATION_ID.fetch_add(1, Ordering::Relaxed))
}

/// Scopes a request correlation id through the downstream future and echoes it in the response.
pub async fn correlation_middleware(request: AxumRequest, next: Next) -> Response {
    let correlation_id = correlation_id(request.headers());
    let mut response = catga_core::scope_correlation_id(correlation_id, next.run(request)).await;
    response.headers_mut().insert(
        CORRELATION_ID_HEADER,
        HeaderValue::from_str(&correlation_id.to_string()).expect("u64 is a valid HTTP header"),
    );
    response
}

/// An Axum response wrapper for a [`CatgaError`].
pub struct CatgaHttpError(CatgaError);

impl From<CatgaError> for CatgaHttpError {
    fn from(error: CatgaError) -> Self {
        Self(error)
    }
}

impl IntoResponse for CatgaHttpError {
    fn into_response(self) -> Response {
        let body = ErrorBody {
            code: error_code_name(self.0.code()),
            message: self.0.message(),
        };
        (status_code(self.0.code()), Json(body)).into_response()
    }
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    code: &'static str,
    message: &'a str,
}

fn status_code(code: ErrorCode) -> StatusCode {
    match code {
        ErrorCode::Validation => StatusCode::UNPROCESSABLE_ENTITY,
        ErrorCode::NotFound => StatusCode::NOT_FOUND,
        ErrorCode::Conflict => StatusCode::CONFLICT,
        ErrorCode::Cancelled | ErrorCode::Timeout => StatusCode::REQUEST_TIMEOUT,
        ErrorCode::Unsupported => StatusCode::NOT_IMPLEMENTED,
        ErrorCode::Transient => StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn error_code_name(code: ErrorCode) -> &'static str {
    match code {
        ErrorCode::Validation => "validation",
        ErrorCode::NotFound => "not_found",
        ErrorCode::Conflict => "conflict",
        ErrorCode::Cancelled => "cancelled",
        ErrorCode::Timeout => "timeout",
        ErrorCode::Unsupported => "unsupported",
        ErrorCode::Transient => "transient",
        ErrorCode::Internal => "internal",
    }
}
