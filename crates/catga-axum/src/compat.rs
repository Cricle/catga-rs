//! Opinionated route builders and macros for rapid Catga endpoint registration.
//!
//! These are convenience shortcuts that expand to standard Axum routes internally. They are
//! **not** the only way to integrate Catga with Axum—prefer [`crate::MediatorState`] and
//! [`crate::CorrelationLayer`]/[`crate::TraceContextLayer`] for full flexibility with existing
//! Axum applications.

use std::{future::Future, sync::Arc};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Extension},
    routing::{on, post},
};
use catga_cluster::{RaftInboundPolicy, RaftInboundRejection, RaftMessage, RaftPeerIdentity};
use catga_core::{
    CatgaError, CatgaResult, Envelope, ErrorCode, Event, Mediator, MessageMetadata, Request,
    TraceContext, scope_transport_context,
};
use http::{HeaderMap, StatusCode, header::CONTENT_TYPE};
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::mpsc;

use crate::{CatgaHttpError, EndpointMethod, MAX_RAFT_MESSAGE_BYTES};

/// Builds the server route that decodes protobuf Raft frames into a runtime inbox.
///
/// The surrounding transport-authentication layer must insert a verified [`RaftPeerIdentity`]
/// extension (for example, from an mTLS SAN or a signed-frame key ID). Never derive this value
/// from a request header or the untrusted protobuf payload. `policy` then binds that identity to
/// the message sender and the local node before the frame reaches Raft.
///
/// The route accepts only bounded `application/x-protobuf` bodies. It returns `401` for a missing
/// authenticated identity, `403` for an untrusted sender or target, `429` for a full inbox, and
/// `503` when the runtime has stopped. A full inbox therefore creates bounded peer backpressure
/// rather than unbounded request tasks.
pub fn raft_message_route<P>(inbox: mpsc::Sender<RaftMessage>, policy: P) -> Router
where
    P: RaftInboundPolicy + 'static,
{
    let policy = Arc::new(policy);
    Router::new()
        .route(
            crate::RAFT_MESSAGE_PATH,
            post(
                move |headers: HeaderMap,
                      peer: Option<Extension<RaftPeerIdentity>>,
                      body: Bytes| {
                    let inbox = inbox.clone();
                    let policy = Arc::clone(&policy);
                    async move {
                        if !is_protobuf_content_type(&headers) {
                            return StatusCode::UNSUPPORTED_MEDIA_TYPE;
                        }
                        let message = match RaftMessage::parse_from_bytes(&body) {
                            Ok(message) => message,
                            Err(_) => return StatusCode::BAD_REQUEST,
                        };
                        match policy.authorize(peer.as_ref().map(|peer| &peer.0), &message) {
                            Ok(()) => {}
                            Err(RaftInboundRejection::Unauthenticated) => {
                                return StatusCode::UNAUTHORIZED;
                            }
                            Err(RaftInboundRejection::Forbidden) => return StatusCode::FORBIDDEN,
                        }
                        match inbox.try_send(message) {
                            Ok(()) => StatusCode::NO_CONTENT,
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                StatusCode::TOO_MANY_REQUESTS
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                StatusCode::SERVICE_UNAVAILABLE
                            }
                        }
                    }
                },
            ),
        )
        .layer(DefaultBodyLimit::max(MAX_RAFT_MESSAGE_BYTES))
}

fn is_protobuf_content_type(headers: &HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| {
            media_type
                .trim()
                .eq_ignore_ascii_case("application/x-protobuf")
        })
}

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

use protobuf::Message as ProtobufMessage;
