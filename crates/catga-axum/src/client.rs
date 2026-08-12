//! Outgoing HTTP clients and cluster/Raft transports.
//!
//! These types propagate Catga correlation and W3C trace context headers on outgoing requests.
//! They own no background work and share a caller-supplied reusable [`reqwest::Client`].

use std::{collections::HashMap, io, num::NonZeroUsize, sync::Arc, time::Duration};

use async_trait::async_trait;
use catga_cluster::{
    ClusterForwarder, RaftMember, RaftMessage, RaftTransport, RaftTransportError,
    RaftTransportResult,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode, Request};
use futures::StreamExt;
use http::{HeaderMap, HeaderValue};
use protobuf::Message as ProtobufMessage;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::{
    RAFT_MESSAGE_PATH, RAFT_PEER_IDENTITY_HEADER, propagate_correlation_header,
    propagate_trace_context_headers,
};

/// An explicit Reqwest client wrapper that propagates task-scoped Catga correlation and trace
/// headers to outgoing requests.
///
/// The wrapper owns no background work and shares the supplied reusable [`reqwest::Client`]. Each
/// request begins with caller-provided headers, so an explicit correlation or W3C trace header
/// takes precedence over ambient Catga context.
#[derive(Clone)]
pub struct CorrelationHttpClient {
    client: reqwest::Client,
}

impl CorrelationHttpClient {
    /// Wraps an application-owned reusable Reqwest client.
    ///
    /// ```
    /// use catga_axum::{CORRELATION_ID_HEADER, CorrelationHttpClient};
    /// use catga_core::scope_correlation_value;
    /// use http::HeaderMap;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = CorrelationHttpClient::new(reqwest::Client::new());
    /// let request = scope_correlation_value("corr-7".into(), async {
    ///     client.post("http://localhost/orders", HeaderMap::new()).build()
    /// })
    /// .await?;
    /// // Ambient correlation context was stamped onto the outgoing request.
    /// assert_eq!(request.headers()[CORRELATION_ID_HEADER], "corr-7");
    /// # Ok(())
    /// # }
    /// ```
    pub const fn new(client: reqwest::Client) -> Self {
        Self { client }
    }

    /// Builds an outgoing request with caller headers followed by ambient Catga propagation.
    pub fn request(
        &self,
        method: reqwest::Method,
        url: impl reqwest::IntoUrl,
        mut headers: HeaderMap,
    ) -> reqwest::RequestBuilder {
        propagate_trace_context_headers(&mut headers);
        propagate_correlation_header(&mut headers);
        self.client.request(method, url).headers(headers)
    }

    /// Builds a POST request with caller headers followed by ambient Catga propagation.
    pub fn post(&self, url: impl reqwest::IntoUrl, headers: HeaderMap) -> reqwest::RequestBuilder {
        self.request(reqwest::Method::POST, url, headers)
    }
}

/// Caller-customizable cluster forward URL builder: `(leader_endpoint, request_type) -> url`.
type PathBuilder = Arc<dyn Fn(&str, &str) -> String + Send + Sync>;

/// HTTP implementation of [`ClusterForwarder`] for Serde request and response types.
///
/// The forward path is customizable: the default pattern is
/// `{leader}/api/catga/forward/{RequestType}`, but deployments can override the
/// prefix or supply a fully custom path builder.
///
/// ```no_run
/// use catga_axum::HttpClusterForwarder;
/// use catga_cluster::ClusterForwarder;
/// use catga_core::{CatgaResult, Message, MessageTypeId, Request};
///
/// #[derive(serde::Serialize)]
/// struct GetBalance;
/// impl Message for GetBalance {}
/// struct GetBalanceTypeId;
/// impl MessageTypeId for GetBalanceTypeId { const NAME: &'static str = "GetBalance"; }
/// impl Request for GetBalance { type Response = u64; type TypeId = GetBalanceTypeId; }
///
/// # async fn run() -> CatgaResult<()> {
/// let forwarder = HttpClusterForwarder::new(reqwest::Client::new());
/// // Non-leader nodes forward the typed request to the elected leader.
/// let balance: u64 = forwarder.forward(GetBalance, "http://node-1:9000").await?;
/// # Ok(())
/// # }
/// ```
pub struct HttpClusterForwarder {
    client: reqwest::Client,
    response_limit: usize,
    path_builder: PathBuilder,
}

/// Default maximum JSON response body accepted from a cluster leader.
///
/// A one-mebibyte limit bounds memory retained while decoding a successful forwarded response.
pub const DEFAULT_HTTP_CLUSTER_FORWARD_RESPONSE_LIMIT_BYTES: usize = 1024 * 1024;

/// Default path prefix for cluster forwarding when no custom builder is supplied.
pub const DEFAULT_FORWARD_PATH_PREFIX: &str = "/api/catga/forward";

impl HttpClusterForwarder {
    /// Creates a forwarder using the supplied reusable HTTP client and default response limit.
    ///
    /// The forward path defaults to `{leader}/api/catga/forward/{RequestType}`.
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            response_limit: DEFAULT_HTTP_CLUSTER_FORWARD_RESPONSE_LIMIT_BYTES,
            path_builder: Arc::new(default_forward_path),
        }
    }

    /// Creates a forwarder with a strict nonzero JSON response body limit in bytes.
    ///
    /// The limit is enforced while streaming the body, so a peer cannot bypass it by omitting or
    /// lying about `Content-Length`. Use [`Self::new`] to retain the default one-mebibyte limit.
    pub fn with_response_limit(client: reqwest::Client, response_limit: NonZeroUsize) -> Self {
        Self {
            client,
            response_limit: response_limit.get(),
            path_builder: Arc::new(default_forward_path),
        }
    }

    /// Replaces the default path prefix with a custom one.
    ///
    /// The resulting URL is `{leader}{prefix}/{RequestType}`.
    pub fn with_path_prefix(mut self, prefix: impl Into<Arc<str>>) -> Self {
        let prefix: Arc<str> = prefix.into();
        self.path_builder =
            Arc::new(move |leader, request_type| format!("{leader}{prefix}/{request_type}"));
        self
    }

    /// Supplies a fully custom path builder.
    ///
    /// The builder receives `(leader_endpoint, request_type_name)` and must return the
    /// complete target URL. This gives deployments full control over routing topology.
    pub fn with_path_builder(
        mut self,
        builder: impl Fn(&str, &str) -> String + Send + Sync + 'static,
    ) -> Self {
        self.path_builder = Arc::new(builder);
        self
    }
}

fn default_forward_path(leader: &str, request_type: &str) -> String {
    format!("{leader}{DEFAULT_FORWARD_PATH_PREFIX}/{request_type}")
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
        let url = (self.path_builder)(leader_endpoint.trim_end_matches('/'), request_type);
        let mut headers = HeaderMap::new();
        propagate_trace_context_headers(&mut headers);
        propagate_correlation_header(&mut headers);
        let request = self.client.post(url).headers(headers).json(&request);
        let response = request
            .send()
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;
        if !response.status().is_success() {
            return Err(CatgaError::new(
                ErrorCode::Transient,
                format!("leader forwarding failed with status {}", response.status()),
            ));
        }
        let body = read_limited_json_response(response, self.response_limit).await?;
        serde_json::from_slice(&body)
            .map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))
    }
}

async fn read_limited_json_response(
    response: reqwest::Response,
    limit: usize,
) -> CatgaResult<Vec<u8>> {
    let mut body = Vec::new();
    let mut chunks = response.bytes_stream();
    while let Some(chunk) = chunks.next().await {
        let chunk =
            chunk.map_err(|error| CatgaError::new(ErrorCode::Transient, error.to_string()))?;
        let next_len = body.len().checked_add(chunk.len()).ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Transient,
                "leader forwarding response body length overflowed",
            )
        })?;
        if next_len > limit {
            return Err(CatgaError::new(
                ErrorCode::Transient,
                "leader forwarding response body exceeds the configured limit",
            ));
        }
        body.try_reserve(chunk.len()).map_err(|_| {
            CatgaError::new(
                ErrorCode::Transient,
                "leader forwarding response body allocation failed",
            )
        })?;
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// HTTP implementation of [`RaftTransport`] using compact protobuf protocol frames.
///
/// Frames are POSTed to `{endpoint}`[`RAFT_MESSAGE_PATH`](crate::RAFT_MESSAGE_PATH) with an
/// optional self-asserted peer-identity header. Transient HTTP statuses (408, 425, 429, 502,
/// 503, 504) and connect/timeout failures are reported retryable so the Raft owner reports the
/// peer unreachable and keeps running; every other failure is fatal and stops the owner task.
///
/// ```
/// use std::sync::Arc;
/// use std::time::Duration;
/// use catga_axum::HttpRaftTransport;
/// use catga_cluster::RaftMember;
///
/// let transport = HttpRaftTransport::new(
///     reqwest::Client::new(),
///     vec![
///         RaftMember::new(1, "http://node-1:9000"),
///         RaftMember::new(2, "http://node-2:9000"),
///     ],
/// )
/// .with_request_timeout(Duration::from_secs(2))
/// .with_peer_identity("node-1");
///
/// // Mirror a committed membership change into the route map.
/// transport.update_member(3, Arc::from("http://node-3:9000"));
/// transport.remove_member(3);
/// ```
pub struct HttpRaftTransport {
    client: reqwest::Client,
    endpoints: std::sync::RwLock<HashMap<u64, Arc<str>>>,
    request_timeout: Option<Duration>,
    peer_identity: Option<HeaderValue>,
}

impl HttpRaftTransport {
    /// Creates a transport whose member map routes Raft IDs to endpoints.
    ///
    /// The map starts from the configured members and can later follow cluster
    /// membership changes through [`Self::update_member`] and
    /// [`Self::remove_member`].
    pub fn new<I>(client: reqwest::Client, members: I) -> Self
    where
        I: IntoIterator<Item = RaftMember>,
    {
        Self {
            client,
            endpoints: std::sync::RwLock::new(
                members
                    .into_iter()
                    .map(|member| (member.id(), Arc::from(member.endpoint())))
                    .collect(),
            ),
            request_timeout: None,
            peer_identity: None,
        }
    }

    /// Adds or replaces the route for one member.
    ///
    /// `HttpRaftTransport` deliberately does not subscribe to membership
    /// changes itself: the application observes the committed voter set (for
    /// example through
    /// [`ClusterCoordinator::member_endpoints`](catga_cluster::ClusterCoordinator::member_endpoints)
    /// after `add_voter`/`remove_voter`) and mirrors it here. A newly added
    /// voter must be registered before the leader can replicate to it, so call
    /// this as soon as the change is observable.
    pub fn update_member(&self, id: u64, endpoint: Arc<str>) {
        self.endpoints
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id, endpoint);
    }

    /// Drops the route for one member, if it was registered.
    ///
    /// Sends to an unknown member fail as fatal transport errors, so only
    /// remove members whose removal has been committed by the cluster.
    pub fn remove_member(&self, id: u64) {
        self.endpoints
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&id);
    }

    /// Bounds every outbound Raft frame with `timeout`.
    ///
    /// The Raft owner task awaits each send, so a peer that accepts TCP but never
    /// responds would otherwise stall the whole Raft loop indefinitely. With a timeout
    /// the send instead fails as retryable backpressure and the peer is reported
    /// unreachable until it recovers.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    /// Attaches the static peer-identity header expected by
    /// [`raft_peer_identity_middleware`](crate::raft_peer_identity_middleware).
    ///
    /// A self-asserted identity is only safe on trusted networks or demos; production
    /// deployments must authenticate peers at the transport layer (for example mTLS) and
    /// derive the identity there. Values that are not valid HTTP header content are
    /// dropped and no header is sent.
    pub fn with_peer_identity(mut self, identity: impl Into<String>) -> Self {
        let identity = identity.into();
        let parsed = HeaderValue::from_str(&identity).ok();
        debug_assert!(
            parsed.is_some(),
            "raft peer identity must be valid HTTP header content, got {identity:?}"
        );
        self.peer_identity = parsed;
        self
    }
}

#[async_trait]
impl RaftTransport for HttpRaftTransport {
    async fn send(&self, message: RaftMessage) -> RaftTransportResult {
        let endpoint = self
            .endpoints
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&message.to)
            .cloned()
            .ok_or_else(|| {
                RaftTransportError::fatal(io::Error::other(format!(
                    "unknown Raft peer {}",
                    message.to
                )))
            })?;
        let body = message
            .write_to_bytes()
            .map_err(RaftTransportError::fatal)?;
        let mut request = self
            .client
            .post(format!(
                "{}{RAFT_MESSAGE_PATH}",
                endpoint.trim_end_matches('/')
            ))
            .header(reqwest::header::CONTENT_TYPE, "application/x-protobuf");
        if let Some(identity) = &self.peer_identity {
            request = request.header(RAFT_PEER_IDENTITY_HEADER, identity);
        }
        let send = request.body(body).send();
        let response = match self.request_timeout {
            Some(timeout) => match tokio::time::timeout(timeout, send).await {
                Ok(result) => result.map_err(classify_raft_http_client_error)?,
                Err(_) => {
                    return Err(RaftTransportError::retryable(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "Raft peer send timed out",
                    )));
                }
            },
            None => send.await.map_err(classify_raft_http_client_error)?,
        };
        if response.status().is_success() {
            Ok(())
        } else if retryable_raft_http_status(response.status()) {
            Err(RaftTransportError::retryable(io::Error::other(format!(
                "Raft peer returned temporary HTTP {}",
                response.status()
            ))))
        } else {
            Err(RaftTransportError::fatal(io::Error::other(format!(
                "Raft peer returned HTTP {}",
                response.status()
            ))))
        }
    }
}

fn retryable_raft_http_status(status: reqwest::StatusCode) -> bool {
    matches!(
        status,
        reqwest::StatusCode::REQUEST_TIMEOUT
            | reqwest::StatusCode::TOO_EARLY
            | reqwest::StatusCode::TOO_MANY_REQUESTS
            | reqwest::StatusCode::BAD_GATEWAY
            | reqwest::StatusCode::SERVICE_UNAVAILABLE
            | reqwest::StatusCode::GATEWAY_TIMEOUT
    )
}

fn classify_raft_http_client_error(error: reqwest::Error) -> RaftTransportError {
    if error.is_timeout() || error.is_connect() {
        RaftTransportError::retryable(error)
    } else {
        RaftTransportError::fatal(error)
    }
}
