#![forbid(unsafe_code)]
//! Axum adapters for Catga's framework-independent result types.

mod validation;

use std::{
    collections::HashMap,
    future::Future,
    io,
    num::NonZeroUsize,
    panic::AssertUnwindSafe,
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
};

use async_trait::async_trait;
use axum::{
    Json, Router,
    body::Bytes,
    extract::Request as AxumRequest,
    http::{HeaderMap, HeaderValue, Method, StatusCode, header::LOCATION},
    middleware::Next,
    response::{IntoResponse, Response},
    routing::{MethodFilter, on, post},
};
use catga_cluster::{
    ClusterForwarder, RaftMember, RaftMessage, RaftTransport, RaftTransportResult,
};
use catga_core::{
    CatgaError, CatgaResult, Envelope, ErrorCode, Event, Mediator, MessageMetadata, Request,
    TraceContext, current_correlation_id, current_correlation_value, current_transport_context,
    scope_transport_context,
};
use futures::{FutureExt, StreamExt};
use protobuf::Message as ProtobufMessage;
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::mpsc;

/// Re-exported so [`axum_routes!`] expands against the same Axum version as this crate.
pub use axum;

pub use validation::{
    EndpointValidation, validate_max_length, validate_min_count, validate_min_length,
    validate_not_empty, validate_positive, validate_range, validate_required,
};

/// Header used to propagate request correlation identifiers.
pub const CORRELATION_ID_HEADER: &str = "x-correlation-id";

/// Adds the current task's correlation identifier to an outgoing HTTP header map.
///
/// A caller-supplied correlation header is preserved, allowing an explicit
/// downstream boundary to take precedence. Otherwise, an opaque scoped value
/// or correlation header from the current transport context takes precedence
/// over a numeric correlation identifier. When no correlation is scoped, this
/// function leaves `headers` unchanged. It is transport-neutral and can be
/// used before building requests for any Rust HTTP client.
pub fn propagate_correlation_header(headers: &mut HeaderMap) {
    if headers.contains_key(CORRELATION_ID_HEADER) {
        return;
    }
    if let Some(correlation_value) = current_correlation_value()
        && let Ok(value) = HeaderValue::from_str(&correlation_value)
    {
        headers.insert(CORRELATION_ID_HEADER, value);
        return;
    }
    if let Some(context) = current_transport_context()
        && let Some(context_headers) = context.headers()
        && let Some(correlation_value) = context_headers.get(CORRELATION_ID_HEADER)
        && let Ok(value) = HeaderValue::from_str(correlation_value)
    {
        headers.insert(CORRELATION_ID_HEADER, value);
        return;
    }
    let Some(correlation_id) = current_correlation_id() else {
        return;
    };
    let Ok(value) = correlation_id.to_string().parse::<HeaderValue>() else {
        return;
    };
    headers.insert(CORRELATION_ID_HEADER, value);
}

/// Adds the current delivery's validated W3C trace context to outgoing HTTP headers.
///
/// Explicit HTTP trace headers always win as one pair: when either `traceparent` or `tracestate`
/// is already present, this function leaves both unchanged. Invalid inbound envelope values and
/// values rejected by HTTP header validation are ignored without affecting the caller's request.
pub fn propagate_trace_context_headers(headers: &mut HeaderMap) {
    if headers.contains_key(catga_core::TRACEPARENT_HEADER)
        || headers.contains_key(catga_core::TRACESTATE_HEADER)
    {
        return;
    }
    let Some(context) = current_transport_context().and_then(|context| {
        context
            .headers()
            .and_then(TraceContext::from_envelope_headers)
    }) else {
        return;
    };
    let Ok(traceparent) = HeaderValue::from_str(context.traceparent()) else {
        return;
    };
    let tracestate = context.tracestate().map(HeaderValue::from_str).transpose();
    let Ok(tracestate) = tracestate else {
        return;
    };
    headers.insert(catga_core::TRACEPARENT_HEADER, traceparent);
    if let Some(tracestate) = tracestate {
        headers.insert(catga_core::TRACESTATE_HEADER, tracestate);
    }
}

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

/// HTTP endpoint used to receive raw protobuf Raft protocol messages.
pub const RAFT_MESSAGE_PATH: &str = "/api/catga/raft";

static NEXT_CORRELATION_ID: AtomicU64 = AtomicU64::new(1);

const COMMAND_RESPONSES: &[StatusCode] = &[
    StatusCode::OK,
    StatusCode::UNPROCESSABLE_ENTITY,
    StatusCode::NOT_FOUND,
    StatusCode::CONFLICT,
];
const QUERY_RESPONSES: &[StatusCode] = &[StatusCode::OK, StatusCode::NOT_FOUND];
const EVENT_RESPONSES: &[StatusCode] = &[StatusCode::NO_CONTENT];

/// Categorizes a Catga HTTP endpoint for documentation consumers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EndpointKind {
    /// A state-changing mediator request.
    Command,
    /// A read-oriented mediator request.
    Query,
    /// A mediator event publication.
    Event,
}

/// HTTP verbs supported by Catga's explicit typed endpoint registration.
///
/// The variants mirror the upstream endpoint attribute while keeping route registration fully
/// compile-time and explicit in Rust source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EndpointMethod {
    /// A read-oriented HTTP GET endpoint.
    Get,
    /// An HTTP POST endpoint.
    Post,
    /// An HTTP PUT endpoint.
    Put,
    /// An HTTP PATCH endpoint.
    Patch,
    /// An HTTP DELETE endpoint.
    Delete,
}

impl EndpointMethod {
    const fn filter(self) -> MethodFilter {
        match self {
            Self::Get => MethodFilter::GET,
            Self::Post => MethodFilter::POST,
            Self::Put => MethodFilter::PUT,
            Self::Patch => MethodFilter::PATCH,
            Self::Delete => MethodFilter::DELETE,
        }
    }

    /// Returns the corresponding HTTP method for metadata consumers.
    pub fn as_http_method(self) -> Method {
        match self {
            Self::Get => Method::GET,
            Self::Post => Method::POST,
            Self::Put => Method::PUT,
            Self::Patch => Method::PATCH,
            Self::Delete => Method::DELETE,
        }
    }
}

impl EndpointKind {
    /// Returns the stable documentation tag for this endpoint category.
    pub const fn tag(self) -> &'static str {
        match self {
            Self::Command => "Commands",
            Self::Query => "Queries",
            Self::Event => "Events",
        }
    }

    const fn responses(self) -> &'static [StatusCode] {
        match self {
            Self::Command => COMMAND_RESPONSES,
            Self::Query => QUERY_RESPONSES,
            Self::Event => EVENT_RESPONSES,
        }
    }
}

/// Static metadata for one Catga endpoint, ready for an OpenAPI or Swagger adapter.
///
/// The type intentionally does not select an OpenAPI implementation. Applications can feed the
/// returned values into `utoipa`, `aide`, or an in-house generator while retaining an explicit,
/// compile-time route list and avoiding runtime reflection. All stored text is `&'static str`, so
/// catalog construction does not allocate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EndpointMetadata {
    kind: EndpointKind,
    method: EndpointMethod,
    path: &'static str,
    operation_id: &'static str,
    description: Option<&'static str>,
}

impl EndpointMetadata {
    /// Creates metadata for a command endpoint that accepts `M` as JSON over HTTP POST.
    pub fn command<M: Request>(path: &'static str) -> Self {
        Self::command_with_method::<M>(EndpointMethod::Post, path)
    }

    /// Creates metadata for a command endpoint that accepts `M` as JSON over `method`.
    pub fn command_with_method<M: Request>(method: EndpointMethod, path: &'static str) -> Self {
        Self::new(EndpointKind::Command, method, path, short_type_name::<M>())
    }

    /// Creates metadata for a query endpoint that accepts `M` as JSON over HTTP POST.
    ///
    /// Catga uses the same typed mediator route implementation for commands and queries; the
    /// distinction is documentation-only and controls its tag and documented response set.
    pub fn query<M: Request>(path: &'static str) -> Self {
        Self::query_with_method::<M>(EndpointMethod::Post, path)
    }

    /// Creates metadata for a query endpoint that accepts `M` as JSON over `method`.
    pub fn query_with_method<M: Request>(method: EndpointMethod, path: &'static str) -> Self {
        Self::new(EndpointKind::Query, method, path, short_type_name::<M>())
    }

    /// Creates metadata for an event publication endpoint that accepts `E` as JSON over HTTP POST.
    pub fn event<E: Event>(path: &'static str) -> Self {
        Self::event_with_method::<E>(EndpointMethod::Post, path)
    }

    /// Creates metadata for an event endpoint that accepts `E` as JSON over `method`.
    pub fn event_with_method<E: Event>(method: EndpointMethod, path: &'static str) -> Self {
        Self::new(EndpointKind::Event, method, path, short_type_name::<E>())
    }

    const fn new(
        kind: EndpointKind,
        method: EndpointMethod,
        path: &'static str,
        operation_id: &'static str,
    ) -> Self {
        Self {
            kind,
            method,
            path,
            operation_id,
            description: None,
        }
    }

    /// Replaces the generated operation identifier with an explicit stable identifier.
    pub const fn with_operation_id(mut self, operation_id: &'static str) -> Self {
        self.operation_id = operation_id;
        self
    }

    /// Adds a human-readable operation description without allocating a catalog string.
    pub const fn with_description(mut self, description: &'static str) -> Self {
        self.description = Some(description);
        self
    }

    /// Returns whether this endpoint documents a command, query, or event publication.
    pub const fn kind(self) -> EndpointKind {
        self.kind
    }

    /// Returns the HTTP method used by this endpoint.
    pub fn method(self) -> Method {
        self.method.as_http_method()
    }

    /// Returns the statically registered endpoint path.
    pub const fn path(self) -> &'static str {
        self.path
    }

    /// Returns the stable OpenAPI operation identifier.
    pub const fn operation_id(self) -> &'static str {
        self.operation_id
    }

    /// Returns the optional human-readable endpoint description.
    pub const fn description(self) -> Option<&'static str> {
        self.description
    }

    /// Returns the standard tag used by Catga's generated OpenAPI metadata.
    pub const fn tag(self) -> &'static str {
        self.kind.tag()
    }

    /// Returns successful and common error response status codes for this endpoint kind.
    pub const fn response_statuses(self) -> &'static [StatusCode] {
        self.kind.responses()
    }
}

fn short_type_name<T>() -> &'static str {
    std::any::type_name::<T>()
        .rsplit("::")
        .next()
        .unwrap_or("catga-endpoint")
}

/// Maps an endpoint-method token to [`EndpointMethod`] for [`catga_routes!`] and
/// [`catga_endpoint_metadata!`].
///
/// This macro is public only because exported macros resolve through `$crate`; applications use
/// the higher-level route and metadata macros instead.
#[doc(hidden)]
#[macro_export]
macro_rules! __catga_endpoint_method {
    () => {
        $crate::EndpointMethod::Post
    };
    (get) => {
        $crate::EndpointMethod::Get
    };
    (post) => {
        $crate::EndpointMethod::Post
    };
    (put) => {
        $crate::EndpointMethod::Put
    };
    (patch) => {
        $crate::EndpointMethod::Patch
    };
    (delete) => {
        $crate::EndpointMethod::Delete
    };
    (GET) => {
        $crate::EndpointMethod::Get
    };
    (POST) => {
        $crate::EndpointMethod::Post
    };
    (PUT) => {
        $crate::EndpointMethod::Put
    };
    (PATCH) => {
        $crate::EndpointMethod::Patch
    };
    (DELETE) => {
        $crate::EndpointMethod::Delete
    };
}

/// Builds an Axum router from explicit, compile-time typed Catga request and event routes.
///
/// The macro is the Rust equivalent of endpoint source generation: the listed message types
/// expand directly to [`mediator_route`] and [`event_route`] calls, with no reflection, service
/// lookup, or dynamic route discovery. At least one request or event route is required.
///
/// ```ignore
/// let routes = catga_axum::catga_routes! {
///     mediator = mediator;
///     requests { @post "/orders" => CreateOrder }
///     events { @post "/orders/created" => OrderCreated }
/// }?;
/// ```
#[macro_export]
macro_rules! catga_routes {
    (
        mediator = $mediator:expr;
        requests {
            $(@$first_request_method:ident)? $first_request_path:expr => $first_request:ty
            $(, $(@$request_method:ident)? $request_path:expr => $request:ty)*
            $(,)?
        }
        events {
            $( $(@$event_method:ident)? $event_path:expr => $event:ty ),* $(,)?
        }
    ) => {{
        (|| -> ::catga_core::CatgaResult<_> {
            let mediator = $mediator;
            let mut router = $crate::mediator_route_with_method::<$first_request>(
                $crate::__catga_endpoint_method!($($first_request_method)?),
                $first_request_path,
                ::std::sync::Arc::clone(&mediator),
            )?;
            $(
                router = router.merge($crate::mediator_route_with_method::<$request>(
                    $crate::__catga_endpoint_method!($($request_method)?),
                    $request_path,
                    ::std::sync::Arc::clone(&mediator),
                )?);
            )*
            $(
                router = router.merge($crate::event_route_with_method::<$event>(
                    $crate::__catga_endpoint_method!($($event_method)?),
                    $event_path,
                    ::std::sync::Arc::clone(&mediator),
                )?);
            )*
            Ok(router)
        })()
    }};
    (
        mediator = $mediator:expr;
        requests {}
        events {
            $(@$first_event_method:ident)? $first_event_path:expr => $first_event:ty
            $(, $(@$event_method:ident)? $event_path:expr => $event:ty)*
            $(,)?
        }
    ) => {{
        (|| -> ::catga_core::CatgaResult<_> {
            let mediator = $mediator;
            let mut router = $crate::event_route_with_method::<$first_event>(
                $crate::__catga_endpoint_method!($($first_event_method)?),
                $first_event_path,
                ::std::sync::Arc::clone(&mediator),
            )?;
            $(
                router = router.merge($crate::event_route_with_method::<$event>(
                    $crate::__catga_endpoint_method!($($event_method)?),
                    $event_path,
                    ::std::sync::Arc::clone(&mediator),
                )?);
            )*
            Ok(router)
        })()
    }};
}

/// Builds routes for native Axum handlers from an existing [`axum::Router`] expression.
///
/// This is separate from [`catga_routes!`], which registers Catga mediator request and event
/// handlers. `axum_routes!` expands each entry directly to Axum's corresponding routing method;
/// it neither serializes through Catga nor stores handlers in a runtime registry. Consequently,
/// handlers may use any extractor and response type accepted by Axum, and invalid handler,
/// extractor, state, or `Send` bounds are reported by the usual Axum compiler diagnostics.
///
/// The first expression supplies the router to extend. For handlers that use `State<T>`, use
/// `Router::<T>::new()` as the base and call `.with_state(state)` on the expanded router. Each
/// following entry is an explicit uppercase HTTP method, a route path expression, and a handler
/// expression. Supported methods are `GET`, `POST`, `PUT`, `PATCH`, and `DELETE`.
///
/// ```no_run
/// use axum::{
///     Router,
///     extract::{Path, State},
/// };
///
/// #[derive(Clone)]
/// struct AppState;
///
/// async fn show_user(State(_state): State<AppState>, Path(id): Path<u64>) -> String {
///     id.to_string()
/// }
///
/// let app: Router = catga_axum::axum_routes! {
///     Router::<AppState>::new();
///     GET "/users/{id}" => show_user,
///     POST "/users" => || async { axum::http::StatusCode::CREATED },
/// }
/// .with_state(AppState);
/// # let _ = app;
/// ```
///
/// A closure is a handler expression just like a function item. The expanded router is returned
/// directly, so route storage stays proportional to the number of declared routes and the macro
/// creates no background work.
#[macro_export]
macro_rules! axum_routes {
    (
        $router:expr;
        $( $method:ident $path:expr => $handler:expr ),+ $(,)?
    ) => {{
        let mut router = $router;
        $(
            router = $crate::__catga_axum_route!($method, router, $path, $handler);
        )+
        router
    }};
}

/// Expands one [`axum_routes!`] entry to Axum's native routing API.
///
/// This macro is public only because exported macros resolve through `$crate`; applications use
/// [`axum_routes!`] instead.
#[doc(hidden)]
#[macro_export]
macro_rules! __catga_axum_route {
    (GET, $router:ident, $path:expr, $handler:expr) => {
        $router.route($path, $crate::axum::routing::get($handler))
    };
    (POST, $router:ident, $path:expr, $handler:expr) => {
        $router.route($path, $crate::axum::routing::post($handler))
    };
    (PUT, $router:ident, $path:expr, $handler:expr) => {
        $router.route($path, $crate::axum::routing::put($handler))
    };
    (PATCH, $router:ident, $path:expr, $handler:expr) => {
        $router.route($path, $crate::axum::routing::patch($handler))
    };
    (DELETE, $router:ident, $path:expr, $handler:expr) => {
        $router.route($path, $crate::axum::routing::delete($handler))
    };
}

/// Builds a static OpenAPI/Swagger metadata catalog from explicit Catga message types.
///
/// The catalog mirrors [`catga_routes!`] without constructing a router, allowing an application
/// to supply one list to any OpenAPI implementation it chooses. Every group may be empty.
///
/// ```
/// # use catga_axum::catga_endpoint_metadata;
/// # use catga_core::{Event, Message, Request};
/// # struct CreateOrder;
/// # impl Message for CreateOrder {}
/// # impl Request for CreateOrder { type Response = (); }
/// # #[derive(Clone)] struct OrderCreated;
/// # impl Message for OrderCreated {}
/// # impl Event for OrderCreated {}
/// let catalog = catga_endpoint_metadata! {
///     commands { "/orders" => CreateOrder }
///     queries {}
///     events { "/orders/created" => OrderCreated }
/// };
/// assert_eq!(catalog.len(), 2);
/// ```
#[macro_export]
macro_rules! catga_endpoint_metadata {
    (
        commands {}
        queries {}
        events {}
    ) => {
        [] as [$crate::EndpointMetadata; 0]
    };
    (
        commands { $( $(@$command_method:ident)? $command_path:expr => $command:ty ),* $(,)? }
        queries { $( $(@$query_method:ident)? $query_path:expr => $query:ty ),* $(,)? }
        events { $( $(@$event_method:ident)? $event_path:expr => $event:ty ),* $(,)? }
    ) => {
        [
            $( $crate::EndpointMetadata::command_with_method::<$command>($crate::__catga_endpoint_method!($($command_method)?), $command_path), )*
            $( $crate::EndpointMetadata::query_with_method::<$query>($crate::__catga_endpoint_method!($($query_method)?), $query_path), )*
            $( $crate::EndpointMetadata::event_with_method::<$event>($crate::__catga_endpoint_method!($($event_method)?), $event_path), )*
        ]
    };
}

/// HTTP implementation of [`ClusterForwarder`] for Serde request and response types.
pub struct HttpClusterForwarder {
    client: reqwest::Client,
    response_limit: usize,
}

/// Default maximum JSON response body accepted from a cluster leader.
///
/// A one-mebibyte limit bounds memory retained while decoding a successful forwarded response.
pub const DEFAULT_HTTP_CLUSTER_FORWARD_RESPONSE_LIMIT_BYTES: usize = 1024 * 1024;

impl HttpClusterForwarder {
    /// Creates a forwarder using the supplied reusable HTTP client and default response limit.
    pub const fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            response_limit: DEFAULT_HTTP_CLUSTER_FORWARD_RESPONSE_LIMIT_BYTES,
        }
    }

    /// Creates a forwarder with a strict nonzero JSON response body limit in bytes.
    ///
    /// The limit is enforced while streaming the body, so a peer cannot bypass it by omitting or
    /// lying about `Content-Length`. Use [`Self::new`] to retain the default one-mebibyte limit.
    pub const fn with_response_limit(
        client: reqwest::Client,
        response_limit: NonZeroUsize,
    ) -> Self {
        Self {
            client,
            response_limit: response_limit.get(),
        }
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
///
/// Valid inbound W3C trace context is scoped through the mediator request and any nested
/// publication, because this route uses the same typed mediator router as [`mediator_route`].
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
    mediator_router::<M>(EndpointMethod::Post, &path, mediator)
}

/// Builds one typed JSON endpoint that dispatches its request through a mediator.
///
/// Route registration is explicit and static, keeping the hot request path free of reflection,
/// service lookup, and route-table locks. Valid inbound W3C trace context remains scoped through
/// the complete mediator request, including nested publication.
pub fn mediator_route<M>(path: &str, mediator: Arc<Mediator>) -> CatgaResult<Router>
where
    M: Request + DeserializeOwned,
    M::Response: Serialize,
{
    mediator_route_with_method::<M>(EndpointMethod::Post, path, mediator)
}

/// Builds one typed JSON endpoint that dispatches its request through a mediator over `method`.
///
/// Registration remains explicit and static, so using a non-POST verb does not introduce route
/// discovery, reflection, or a runtime route table.
pub fn mediator_route_with_method<M>(
    method: EndpointMethod,
    path: &str,
    mediator: Arc<Mediator>,
) -> CatgaResult<Router>
where
    M: Request + DeserializeOwned,
    M::Response: Serialize,
{
    if !path.starts_with('/') || path == "/" {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "mediator route path must start with '/' and name an endpoint",
        ));
    }
    Ok(mediator_router::<M>(method, path, mediator))
}

/// Builds one typed JSON endpoint that publishes an event through a mediator.
///
/// Valid inbound W3C trace context remains scoped through the complete event publication.
pub fn event_route<E>(path: &str, mediator: Arc<Mediator>) -> CatgaResult<Router>
where
    E: Event + DeserializeOwned,
{
    event_route_with_method::<E>(EndpointMethod::Post, path, mediator)
}

/// Builds one typed JSON endpoint that publishes an event through a mediator over `method`.
pub fn event_route_with_method<E>(
    method: EndpointMethod,
    path: &str,
    mediator: Arc<Mediator>,
) -> CatgaResult<Router>
where
    E: Event + DeserializeOwned,
{
    if !path.starts_with('/') || path == "/" {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "event route path must start with '/' and name an endpoint",
        ));
    }
    Ok(Router::new().route(
        path,
        on(
            method.filter(),
            move |headers: HeaderMap, Json(event): Json<E>| {
                let mediator = Arc::clone(&mediator);
                async move {
                    scope_inbound_trace_context(&headers, async move {
                        mediator
                            .publish(event)
                            .await
                            .map(|()| StatusCode::NO_CONTENT)
                            .map_err(CatgaHttpError::from)
                    })
                    .await
                }
            },
        ),
    ))
}

fn mediator_router<M>(method: EndpointMethod, path: &str, mediator: Arc<Mediator>) -> Router
where
    M: Request + DeserializeOwned,
    M::Response: Serialize,
{
    Router::new().route(
        path,
        on(
            method.filter(),
            move |headers: HeaderMap, Json(message): Json<M>| {
                let mediator = Arc::clone(&mediator);
                async move {
                    scope_inbound_trace_context(&headers, async move {
                        mediator
                            .send(message)
                            .await
                            .map(Json)
                            .map_err(CatgaHttpError::from)
                    })
                    .await
                }
            },
        ),
    )
}

/// Scopes a validated HTTP W3C context using Catga's existing transport context API.
///
/// The minimal envelope is local to the HTTP boundary; it carries no payload and exists only to
/// retain the validated propagation headers while the supplied future runs. An invalid parent
/// leaves the future unscoped. A malformed state is already discarded by [`TraceContext::parse`]
/// while retaining a valid parent.
async fn scope_inbound_trace_context<T>(headers: &HeaderMap, future: impl Future<Output = T>) -> T {
    let Some(traceparent) = headers
        .get(catga_core::TRACEPARENT_HEADER)
        .and_then(|value| value.to_str().ok())
    else {
        return future.await;
    };
    let tracestate = headers
        .get(catga_core::TRACESTATE_HEADER)
        .and_then(|value| value.to_str().ok());
    let Some(context) = TraceContext::parse(traceparent, tracestate) else {
        return future.await;
    };
    let Ok(headers) = context.inject_into_envelope_headers(None) else {
        return future.await;
    };
    let envelope = Envelope::new(
        0,
        "catga.http.inbound",
        Vec::new(),
        MessageMetadata::new(0, None),
    )
    .with_headers(headers);
    scope_transport_context(&envelope, future).await
}

/// Reads a numeric correlation identifier or allocates a monotonic process-local fallback.
pub fn correlation_id(headers: &axum::http::HeaderMap) -> u64 {
    headers
        .get(CORRELATION_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| NEXT_CORRELATION_ID.fetch_add(1, Ordering::Relaxed))
}

/// Scopes a request correlation value through the downstream future and echoes it in the response.
///
/// Valid, nonempty inbound correlation headers remain opaque: they are neither parsed nor
/// normalized before being made available to downstream HTTP calls and echoed back to the client.
/// The numeric correlation scope remains populated with an existing or generated ID for
/// compatibility with typed transport code.
pub async fn correlation_middleware(request: AxumRequest, next: Next) -> Response {
    let correlation_id = correlation_id(request.headers());
    let correlation_header = request
        .headers()
        .get(CORRELATION_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(|value| (Arc::<str>::from(value), HeaderValue::from_str(value).ok()));
    let (correlation_value, response_header) = match correlation_header {
        Some((value, header)) => (value, header),
        None => {
            let value: Arc<str> = correlation_id.to_string().into();
            let header = HeaderValue::from_str(&value).ok();
            (value, header)
        }
    };
    let mut response = catga_core::scope_correlation_value(
        correlation_value,
        catga_core::scope_correlation_id(correlation_id, next.run(request)),
    )
    .await;
    if let Some(header_value) = response_header {
        response
            .headers_mut()
            .insert(CORRELATION_ID_HEADER, header_value);
    }
    response
}

/// Converts an endpoint-handler unwind into Catga's stable internal-error response.
///
/// Install this opt-in boundary with
/// `axum::middleware::from_fn(endpoint_panic_middleware)`. Successful responses
/// are forwarded unchanged. A caught unwind becomes the same compact JSON
/// response produced by [`CatgaHttpError`] for [`ErrorCode::Internal`], without
/// exposing the panic payload to an HTTP client.
///
/// This middleware does not catch abort-mode panics and cannot repair a
/// response after its headers or body have been sent. Normal handlers should
/// continue returning [`CatgaResult`] rather than using panics for expected
/// failures.
pub async fn endpoint_panic_middleware(request: AxumRequest, next: Next) -> Response {
    match AssertUnwindSafe(next.run(request)).catch_unwind().await {
        Ok(response) => response,
        Err(_) => CatgaHttpError::from(CatgaError::new(
            ErrorCode::Internal,
            "endpoint handler panicked",
        ))
        .into_response(),
    }
}

/// An Axum response wrapper for a [`CatgaError`].
pub struct CatgaHttpError(CatgaError);

impl From<CatgaError> for CatgaHttpError {
    fn from(error: CatgaError) -> Self {
        Self(error)
    }
}

/// Converts a [`CatgaResult`] into an owned Axum HTTP response.
///
/// This trait is implemented by `CatgaResult<T>` when `T` implements
/// [`Serialize`]. It preserves Catga's error-to-HTTP mapping while allowing a
/// handler to select its successful response status without duplicating error
/// branching at every route.
pub trait IntoCatgaHttpResponse {
    /// Serializes a successful value as JSON with `success_status`.
    ///
    /// A [`StatusCode::NO_CONTENT`] success deliberately has no response body.
    /// On failure, this delegates to [`CatgaHttpError`], ignoring
    /// `success_status` and preserving the error's mapped status and compact
    /// JSON body.
    fn into_catga_response(self, success_status: StatusCode) -> Response;

    /// Serializes a successful value into a `201 Created` JSON response.
    ///
    /// `location` becomes the HTTP `Location` header and must therefore use
    /// valid header-value bytes, such as a percent-encoded URI path. An invalid
    /// value becomes Catga's structured internal-error response rather than a
    /// panic. Failures in the original result continue to use
    /// [`CatgaHttpError`] and do not emit a `Location` header.
    fn into_catga_created(self, location: &str) -> Response;
}

impl<T> IntoCatgaHttpResponse for CatgaResult<T>
where
    T: Serialize,
{
    fn into_catga_response(self, success_status: StatusCode) -> Response {
        match self {
            Ok(_) if success_status == StatusCode::NO_CONTENT => success_status.into_response(),
            Ok(value) => (success_status, Json(value)).into_response(),
            Err(error) => CatgaHttpError::from(error).into_response(),
        }
    }

    fn into_catga_created(self, location: &str) -> Response {
        match self {
            Ok(value) => match HeaderValue::from_str(location) {
                Ok(location) => {
                    (StatusCode::CREATED, [(LOCATION, location)], Json(value)).into_response()
                }
                Err(_) => CatgaHttpError::from(CatgaError::new(
                    ErrorCode::Internal,
                    "invalid Location header",
                ))
                .into_response(),
            },
            Err(error) => CatgaHttpError::from(error).into_response(),
        }
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
        ErrorCode::Unauthorized => StatusCode::UNAUTHORIZED,
        ErrorCode::Forbidden => StatusCode::FORBIDDEN,
        ErrorCode::Cancelled | ErrorCode::Timeout => StatusCode::REQUEST_TIMEOUT,
        ErrorCode::Unsupported => StatusCode::NOT_IMPLEMENTED,
        ErrorCode::Transient | ErrorCode::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn error_code_name(code: ErrorCode) -> &'static str {
    match code {
        ErrorCode::Validation => "validation",
        ErrorCode::NotFound => "not_found",
        ErrorCode::Conflict => "conflict",
        ErrorCode::Unauthorized => "unauthorized",
        ErrorCode::Forbidden => "forbidden",
        ErrorCode::Cancelled => "cancelled",
        ErrorCode::Timeout => "timeout",
        ErrorCode::Unsupported => "unsupported",
        ErrorCode::Transient => "transient",
        ErrorCode::Unavailable => "unavailable",
        ErrorCode::Internal => "internal",
    }
}
