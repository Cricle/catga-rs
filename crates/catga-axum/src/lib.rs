#![forbid(unsafe_code)]
//! Axum adapters for Catga's framework-independent result types.
//!
//! This crate provides composable primitives for integrating Catga with Axum:
//!
//! - [`MediatorState`] — a standard Axum extractor for the Catga mediator, combinable with
//!   `Path`, `Query`, `State`, and any other extractor.
//! - [`CorrelationLayer`] / [`TraceContextLayer`] — opt-in tower layers for context propagation.
//! - `impl IntoResponse for CatgaError` — any handler can return `CatgaResult<T>` directly.
//!
//! For rapid prototyping, the opinionated [`catga_routes!`] and [`catga_application!`] macros
//! remain available as convenience shortcuts. They are not required for integration.
//!
//! # Boundary assumptions
//!
//! Callers retain ownership of server lifecycle, request-size limits other than the bounded Raft
//! ingress, and authentication. For outgoing requests, [`CorrelationHttpClient`] preserves
//! caller-provided correlation and trace headers; ambient Catga context fills in only missing
//! values. Treat inbound correlation headers as untrusted until application middleware validates
//! or replaces them according to the deployment's trust boundary.

mod client;
mod compat;
mod extract;
mod layer;
mod validation;

#[cfg(test)]
mod tests;

use std::{
    panic::AssertUnwindSafe,
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
};

use axum::{
    Json, Router,
    extract::Request as AxumRequest,
    middleware::Next,
    response::{IntoResponse, Response},
    routing::MethodFilter,
};
use catga_core::{
    CatgaError, CatgaResult, ErrorCode, Event, Mediator, Request, TraceContext,
    current_correlation_id, current_correlation_value, current_transport_context,
    scope_correlation_id, scope_correlation_value,
};
use futures::FutureExt;
use http::{HeaderMap, HeaderValue, Method, StatusCode, header::LOCATION};
use serde::Serialize;

// ---------------------------------------------------------------------------
// Re-exports: new composable primitives
// ---------------------------------------------------------------------------

pub use client::{
    CorrelationHttpClient, DEFAULT_FORWARD_PATH_PREFIX,
    DEFAULT_HTTP_CLUSTER_FORWARD_RESPONSE_LIMIT_BYTES, HttpClusterForwarder, HttpRaftTransport,
};
pub use compat::{
    event_route, event_route_with_method, leader_forward_route, leader_forward_route_at,
    mediator_route, mediator_route_with_method, raft_message_route,
};
pub use extract::MediatorState;
pub use layer::{CorrelationLayer, CorrelationService, TraceContextLayer, TraceContextService};
pub use validation::{
    EndpointValidation, validate_max_length, validate_min_count, validate_min_length,
    validate_not_empty, validate_positive, validate_range, validate_required,
};

/// Re-exported so [`axum_routes!`] expands against the same Axum version as this crate.
pub use axum;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Header used to propagate request correlation identifiers.
///
/// Re-exported from [`catga_core::CORRELATION_ID_HEADER`] for backward compatibility.
pub use catga_core::CORRELATION_ID_HEADER;

/// Largest protobuf frame accepted by the Raft HTTP ingress route.
///
/// This matches the native Raft node `max_size_per_msg` setting. Deployments carrying a larger
/// snapshot must use the snapshot transport rather than bypassing this bounded route.
pub const MAX_RAFT_MESSAGE_BYTES: usize = 1024 * 1024;

/// HTTP endpoint used to receive raw protobuf Raft protocol messages.
pub const RAFT_MESSAGE_PATH: &str = "/api/catga/raft";

// ---------------------------------------------------------------------------
// CatgaError → HTTP response (zero-cost error mapping)
// ---------------------------------------------------------------------------

/// A convenience result alias for Axum handlers that return Catga errors.
///
/// Because Rust's orphan rule prevents `impl IntoResponse for CatgaError` (both are
/// foreign types), this alias pairs any successful `Serialize` value with the Catga HTTP
/// error newtype. Handlers can use the [`IntoCatgaHttpResponse`] trait or simply map:
///
/// ```no_run
/// # use catga_axum::CatgaHttpResult;
/// # use axum::Json;
/// #[derive(serde::Serialize)]
/// # struct Order;
/// async fn create_order() -> CatgaHttpResult<Json<Order>> {
///     // ... mediator.send(cmd).await.map(Json).map_err(Into::into)
///     # todo!()
/// }
/// ```
pub type CatgaHttpResult<T> = Result<T, CatgaHttpError>;

// ---------------------------------------------------------------------------
// CatgaHttpError (explicit newtype, retained for backward compatibility)
// ---------------------------------------------------------------------------

/// An Axum response wrapper for a [`CatgaError`].
///
/// This newtype bridges Catga's error type to Axum's `IntoResponse` trait. Use
/// [`CatgaHttpResult`] as the return type for handlers, or convert with `.map_err(Into::into)`.
pub struct CatgaHttpError(CatgaError);

impl From<CatgaError> for CatgaHttpError {
    fn from(error: CatgaError) -> Self {
        Self(error)
    }
}

impl IntoResponse for CatgaHttpError {
    fn into_response(self) -> Response {
        let body = ErrorBody {
            code: self.0.code().as_stable_str(),
            message: self.0.message(),
        };
        (status_code(self.0.code()), Json(body)).into_response()
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
    /// On failure, this delegates to [`CatgaError`]'s `IntoResponse`, ignoring
    /// `success_status` and preserving the error's mapped status and compact
    /// JSON body.
    fn into_catga_response(self, success_status: StatusCode) -> Response;

    /// Serializes a successful value into a `201 Created` JSON response.
    ///
    /// `location` becomes the HTTP `Location` header and must therefore use
    /// valid header-value bytes, such as a percent-encoded URI path. An invalid
    /// value becomes Catga's structured internal-error response rather than a
    /// panic. Failures in the original result continue to use
    /// [`CatgaError`] and do not emit a `Location` header.
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

// ---------------------------------------------------------------------------
// Error mapping internals
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct ErrorBody<'a> {
    code: &'static str,
    message: &'a str,
}

fn status_code(code: ErrorCode) -> StatusCode {
    StatusCode::from_u16(code.http_status_u16()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
}

// ---------------------------------------------------------------------------
// Endpoint metadata (OpenAPI bridge)
// ---------------------------------------------------------------------------

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
    pub(crate) const fn filter(self) -> MethodFilter {
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

// ---------------------------------------------------------------------------
// CatgaApplication (retained for backward compatibility)
// ---------------------------------------------------------------------------

/// An immutable, explicitly composed typed HTTP application.
///
/// Construct this with [`catga_application!`] when an application has one mediator and a static
/// set of typed Axum routes. The type owns no runtime, background task, storage connection, or
/// configuration discovery; callers retain those deployment decisions and can clone the router
/// for the server they own.
#[derive(Clone)]
pub struct CatgaApplication {
    mediator: Arc<Mediator>,
    router: Router,
}

impl CatgaApplication {
    /// Combines an already constructed mediator and static Axum router.
    #[must_use]
    pub const fn new(mediator: Arc<Mediator>, router: Router) -> Self {
        Self { mediator, router }
    }

    /// Returns the application mediator for explicit in-process dispatch.
    #[must_use]
    pub fn mediator(&self) -> Arc<Mediator> {
        Arc::clone(&self.mediator)
    }

    /// Returns a clone of the application router for an application-owned Axum server.
    pub fn router(&self) -> Router {
        self.router.clone()
    }
}

// ---------------------------------------------------------------------------
// Correlation and trace propagation helpers
// ---------------------------------------------------------------------------

/// Adds the current task's correlation identifier to an outgoing HTTP header map.
///
/// A caller-supplied correlation header is preserved, allowing an explicit
/// downstream boundary to take precedence.
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
/// is already present, this function leaves both unchanged.
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

static NEXT_CORRELATION_ID: AtomicU64 = AtomicU64::new(1);

/// Reads a numeric correlation identifier or allocates a monotonic process-local fallback.
pub fn correlation_id(headers: &HeaderMap) -> u64 {
    headers
        .get(CORRELATION_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| NEXT_CORRELATION_ID.fetch_add(1, Ordering::Relaxed))
}

/// Scopes a request correlation value through the downstream future and echoes it in the response.
///
/// Prefer [`CorrelationLayer`] for new code. This function-based middleware is retained for
/// backward compatibility.
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
    let mut response = scope_correlation_value(
        correlation_value,
        scope_correlation_id(correlation_id, next.run(request)),
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
/// `axum::middleware::from_fn(endpoint_panic_middleware)`.
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

// ---------------------------------------------------------------------------
// Macros
// ---------------------------------------------------------------------------

/// Maps an endpoint-method token to [`EndpointMethod`] for [`catga_routes!`] and
/// [`catga_endpoint_metadata!`].
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
/// This is an opinionated shortcut. For full flexibility, use standard Axum handlers with
/// [`MediatorState`] extraction.
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

/// Composes handler registration, mediator binding, and typed Axum routes during startup.
///
/// This is an opinionated shortcut. For full flexibility, construct your own router with
/// [`MediatorState`] and standard Axum routing.
#[macro_export]
macro_rules! catga_application {
    (
        handlers { $($handler:tt)* }
        routes { $($route:tt)* }
        $(,)?
    ) => {{
        (|| -> ::catga_core::CatgaResult<$crate::CatgaApplication> {
            let registry = ::catga_core::catga_handlers! { $($handler)* }?;
            let mediator = ::std::sync::Arc::new(::catga_core::Mediator::new(registry));
            let router = $crate::catga_routes! {
                mediator = ::std::sync::Arc::clone(&mediator);
                $($route)*
            }?;
            Ok($crate::CatgaApplication::new(mediator, router))
        })()
    }};
    (
        mediator_handle = $mediator_handle:expr;
        handlers { $($handler:tt)* }
        routes { $($route:tt)* }
        $(,)?
    ) => {{
        (|| -> ::catga_core::CatgaResult<$crate::CatgaApplication> {
            let registry = ::catga_core::catga_handlers! { $($handler)* }?;
            let mediator = ::std::sync::Arc::new(::catga_core::Mediator::new(registry));
            let router = $crate::catga_routes! {
                mediator = ::std::sync::Arc::clone(&mediator);
                $($route)*
            }?;
            ($mediator_handle).bind(::std::sync::Arc::clone(&mediator))?;
            Ok($crate::CatgaApplication::new(mediator, router))
        })()
    }};
}

/// Builds routes for native Axum handlers from an existing [`axum::Router`] expression.
///
/// This is separate from [`catga_routes!`], which registers Catga mediator request and event
/// handlers. `axum_routes!` expands each entry directly to Axum's corresponding routing method.
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
