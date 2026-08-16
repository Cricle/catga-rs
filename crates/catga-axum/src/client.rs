//! Outgoing HTTP clients for cluster forwarding.
//!
//! These types propagate Catga correlation and W3C trace context headers on outgoing requests.
//! They own no background work and share a caller-supplied reusable [`reqwest::Client`].

use std::{num::NonZeroUsize, sync::Arc};

use catga_core::ClusterForwarder;
use catga_core::{CatgaError, CatgaResult, ErrorCode, Request};
use futures::StreamExt;
use http::HeaderMap;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::{propagate_correlation_header, propagate_trace_context_headers};

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
/// use catga_core::ClusterForwarder;
/// use catga_core::{CatgaResult, Message, Request};
///
/// #[derive(serde::Serialize)]
/// struct GetBalance;
/// impl Message for GetBalance {}
/// impl Request for GetBalance { type Response = u64; }
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
    /// The resulting URL is `{leader}{prefix}/{request_type}`.
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

#[async_trait::async_trait]
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
