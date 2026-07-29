//! Composable tower layers for Catga HTTP context propagation.
//!
//! Each layer is a standard [`tower_layer::Layer`] that can be applied to any Axum router
//! with `.layer(...)`. They are opt-in and independent: apply only what your deployment needs.
//!
//! - [`CorrelationLayer`]: reads or generates a correlation identifier, scopes it through the
//!   request, and echoes it in the response.
//! - [`TraceContextLayer`]: validates inbound W3C `traceparent`/`tracestate` headers and scopes
//!   them through the request without allocating a full [`catga_core::Envelope`].

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll},
};

use catga_core::{
    TraceContext, scope_correlation_id, scope_correlation_value, scope_transport_context_value,
};
use http::{HeaderValue, Request, Response};
use tower_layer::Layer;
use tower_service::Service;

use crate::CORRELATION_ID_HEADER;

static NEXT_LAYER_CORRELATION_ID: AtomicU64 = AtomicU64::new(1);

// ---------------------------------------------------------------------------
// CorrelationLayer
// ---------------------------------------------------------------------------

/// A tower layer that scopes a request correlation identifier and echoes it in the response.
///
/// Valid, nonempty inbound `x-correlation-id` headers are preserved opaquely. When no header
/// is present, a monotonic process-local identifier is generated. The numeric scope remains
/// populated for compatibility with typed transport code.
///
/// ```no_run
/// # use axum::Router;
/// # use catga_axum::CorrelationLayer;
/// # let app: Router<()> = Router::new()
///     .layer(CorrelationLayer::new());
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct CorrelationLayer;

impl CorrelationLayer {
    /// Creates a new correlation layer.
    pub const fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for CorrelationLayer {
    type Service = CorrelationService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        CorrelationService { inner }
    }
}

/// The service produced by [`CorrelationLayer`].
#[derive(Clone, Debug)]
pub struct CorrelationService<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for CorrelationService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        // Single header read: resolve both the numeric ID and the opaque value from
        // one `to_str()` call, avoiding a redundant second header lookup.
        let header_str = request
            .headers()
            .get(CORRELATION_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .filter(|s| !s.is_empty());

        let correlation_id = header_str
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| NEXT_LAYER_CORRELATION_ID.fetch_add(1, Ordering::Relaxed));

        let (correlation_value, response_header) = match header_str {
            Some(s) => {
                // Reuse the already-validated HeaderValue for the response echo;
                // only the Arc<str> for task-local scoping is allocated.
                let response_header = HeaderValue::from_str(s).ok();
                (Arc::<str>::from(s), response_header)
            }
            None => {
                let value: Arc<str> = correlation_id.to_string().into();
                let header = HeaderValue::from_str(&value).ok();
                (value, header)
            }
        };

        let mut inner = self.inner.clone();
        Box::pin(async move {
            let mut response = scope_correlation_value(
                correlation_value,
                scope_correlation_id(correlation_id, inner.call(request)),
            )
            .await?;
            if let Some(header_value) = response_header {
                response
                    .headers_mut()
                    .insert(CORRELATION_ID_HEADER, header_value);
            }
            Ok(response)
        })
    }
}

// ---------------------------------------------------------------------------
// TraceContextLayer
// ---------------------------------------------------------------------------

/// A tower layer that validates and scopes inbound W3C trace context headers.
///
/// Unlike the legacy `scope_inbound_trace_context` helper, this layer avoids allocating a
/// full [`catga_core::Envelope`] per request. It constructs only the minimal
/// [`catga_core::EnvelopeHeaders`] (an `Arc` slice of at most two entries) and scopes a
/// [`catga_core::TransportContext`] directly.
///
/// Invalid or missing `traceparent` headers leave the request unscoped. An invalid
/// `tracestate` is discarded while a valid parent is retained.
///
/// ```no_run
/// # use axum::Router;
/// # use catga_axum::TraceContextLayer;
/// # let app: Router<()> = Router::new()
///     .layer(TraceContextLayer::new());
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct TraceContextLayer;

impl TraceContextLayer {
    /// Creates a new trace context layer.
    pub const fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for TraceContextLayer {
    type Service = TraceContextService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TraceContextService { inner }
    }
}

/// The service produced by [`TraceContextLayer`].
#[derive(Clone, Debug)]
pub struct TraceContextService<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for TraceContextService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        // Parse synchronously on the stack; TraceContext::to_transport_context() builds
        // EnvelopeHeaders directly from a fixed-size array (no intermediate Vec).
        let context = request
            .headers()
            .get(catga_core::TRACEPARENT_HEADER)
            .and_then(|value| value.to_str().ok())
            .and_then(|traceparent| {
                let tracestate = request
                    .headers()
                    .get(catga_core::TRACESTATE_HEADER)
                    .and_then(|value| value.to_str().ok());
                let context = TraceContext::parse(traceparent, tracestate)?;
                context.to_transport_context().ok()
            });

        let mut inner = self.inner.clone();
        Box::pin(async move {
            match context {
                Some(context) => scope_transport_context_value(context, inner.call(request)).await,
                None => inner.call(request).await,
            }
        })
    }
}
