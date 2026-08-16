//! Opinionated route builders and macros for rapid Catga endpoint registration.
//!
//! These are convenience shortcuts that expand to standard Axum routes internally. They are
//! **not** the only way to integrate Catga with Axum—prefer [`crate::MediatorState`] and
//! [`crate::CorrelationLayer`]/[`crate::TraceContextLayer`] for full flexibility with existing
//! Axum applications.

use std::{future::Future, sync::Arc};

use axum::{Json, Router, routing::on};
use catga_core::{
    CatgaError, CatgaResult, Envelope, ErrorCode, Event, Mediator, MessageMetadata, Request,
    TraceContext, scope_transport_context,
};
use http::{HeaderMap, StatusCode};
use serde::{Serialize, de::DeserializeOwned};

use crate::{CatgaHttpError, EndpointMethod};

/// Builds the leader-side forwarding route for one explicitly registered request type.
///
/// The route path defaults to `/api/catga/forward/{RequestType}`. Use
/// [`leader_forward_route_at`] to supply a custom path.
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
    let path = format!(
        "{}/{request_type}",
        crate::client::DEFAULT_FORWARD_PATH_PREFIX
    );
    mediator_router::<M>(EndpointMethod::Post, &path, mediator)
}

/// Builds the leader-side forwarding route at a caller-specified path.
///
/// This gives deployments full control over the forwarding endpoint location rather
/// than assuming a fixed URL pattern.
pub fn leader_forward_route_at<M>(path: &str, mediator: Arc<Mediator>) -> Router
where
    M: Request + DeserializeOwned,
    M::Response: Serialize,
{
    mediator_router::<M>(EndpointMethod::Post, path, mediator)
}

/// Builds one typed JSON endpoint that dispatches its request through a mediator.
///
/// Route registration is explicit and static, keeping the hot request path free of reflection,
/// service lookup, and route-table locks. Valid inbound W3C trace context remains scoped through
/// the complete mediator request, including nested publication.
///
/// For full flexibility with extractors, paths, and methods, prefer a standard Axum handler
/// with [`crate::MediatorState`].
///
/// ```
/// use std::sync::Arc;
/// use catga_axum::mediator_route;
/// use catga_core::{CatgaResult, Mediator, Message, Registry, Request, request_handler};
///
/// #[derive(serde::Serialize, serde::Deserialize)]
/// struct GetBalance;
/// impl Message for GetBalance {}
/// impl Request for GetBalance { type Response = u64; }
///
/// # fn run() -> CatgaResult<()> {
/// let mut registry = Registry::new();
/// registry.register_request::<GetBalance, _>(request_handler(|_: GetBalance| async {
///     Ok(42_u64)
/// }))?;
/// let mediator = Arc::new(Mediator::new(registry));
/// let router = mediator_route::<GetBalance>("/api/balance", mediator)?;
///
/// // Paths must be absolute; a relative path is rejected at registration time.
/// let mediator = Arc::new(Mediator::new(Registry::new()));
/// assert!(mediator_route::<GetBalance>("api/balance", mediator).is_err());
/// # drop(router);
/// # Ok(())
/// # }
/// # run().expect("route example");
/// ```
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
///
/// ```
/// use std::sync::Arc;
/// use catga_axum::event_route;
/// use catga_core::{CatgaResult, Event, Mediator, Message, Registry};
///
/// #[derive(Clone, serde::Serialize, serde::Deserialize)]
/// struct BalanceChanged;
/// impl Message for BalanceChanged {}
/// impl Event for BalanceChanged {}
///
/// # fn run() -> CatgaResult<()> {
/// let mediator = Arc::new(Mediator::new(Registry::new()));
/// let router = event_route::<BalanceChanged>("/api/balance-changed", mediator)?;
///
/// let mediator = Arc::new(Mediator::new(Registry::new()));
/// assert!(event_route::<BalanceChanged>("api/balance-changed", mediator).is_err());
/// # drop(router);
/// # Ok(())
/// # }
/// # run().expect("route example");
/// ```
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

pub(crate) fn mediator_router<M>(
    method: EndpointMethod,
    path: &str,
    mediator: Arc<Mediator>,
) -> Router
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
pub(crate) async fn scope_inbound_trace_context<T>(
    headers: &HeaderMap,
    future: impl Future<Output = T>,
) -> T {
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
