use std::sync::Arc;

use crate::{Envelope, EnvelopeHeaders, MessageMetadata, MessagePriority};

/// Standard header name used to propagate request correlation identifiers across HTTP boundaries.
///
/// This constant is framework-agnostic; any HTTP adapter can reference it without depending
/// on a specific web framework's header type.
pub const CORRELATION_ID_HEADER: &str = "x-correlation-id";

tokio::task_local! {
    static CORRELATION_ID: u64;
}

tokio::task_local! {
    static CORRELATION_VALUE: Arc<str>;
}

tokio::task_local! {
    static TRANSPORT_CONTEXT: TransportContext;
}

/// Immutable inbound transport data available while a delivery is handled.
///
/// The context retains only the correlation identity, priority, and optional
/// shared envelope headers. Cloning it copies the priority and clones at most
/// an [`EnvelopeHeaders`] `Arc`, never an envelope payload or individual
/// header strings.
///
/// ```
/// use catga_core::{EnvelopeHeaders, TransportContext, MessagePriority};
///
/// let headers = EnvelopeHeaders::try_new([("x-tenant", "acme")]).expect("valid");
/// let context = TransportContext::from_headers(headers);
/// assert!(context.correlation_id().is_none());
/// assert_eq!(context.priority(), MessagePriority::Normal);
/// assert_eq!(context.headers().and_then(|h| h.get("x-tenant")), Some("acme"));
/// ```
#[derive(Clone, Debug)]
pub struct TransportContext {
    correlation_id: Option<u64>,
    priority: MessagePriority,
    headers: Option<EnvelopeHeaders>,
}

impl TransportContext {
    pub(crate) fn from_envelope(envelope: &Envelope) -> Self {
        Self {
            correlation_id: envelope.metadata().correlation_id(),
            priority: envelope.metadata().priority(),
            headers: envelope.shared_headers(),
        }
    }

    /// Creates a transport context from validated envelope headers without a full envelope.
    ///
    /// This is useful at HTTP boundaries where only propagation headers are available and
    /// allocating a complete [`Envelope`] would add unnecessary overhead. The context carries
    /// no correlation ID or priority unless explicitly supplied.
    pub fn from_headers(headers: EnvelopeHeaders) -> Self {
        Self {
            correlation_id: None,
            priority: MessagePriority::Normal,
            headers: Some(headers),
        }
    }

    /// Returns the inbound correlation identifier, when the envelope carried one.
    pub const fn correlation_id(&self) -> Option<u64> {
        self.correlation_id
    }

    /// Returns the priority carried by the inbound envelope.
    pub const fn priority(&self) -> MessagePriority {
        self.priority
    }

    /// Returns the immutable inbound headers, when the envelope carried any.
    pub fn headers(&self) -> Option<&EnvelopeHeaders> {
        self.headers.as_ref()
    }
}

/// Supplies metadata for requests that participate in ambient correlation propagation.
pub trait Correlated {
    /// Returns the message metadata that carries this request's correlation identity.
    fn metadata(&self) -> MessageMetadata;
}

/// Returns the correlation identifier scoped to the current asynchronous task chain.
pub fn current_correlation_id() -> Option<u64> {
    CORRELATION_ID.try_with(|id| *id).ok()
}

/// Returns the opaque correlation value scoped to the current asynchronous task chain.
///
/// Unlike [`current_correlation_id`], this value is not parsed or normalized. HTTP adapters use
/// it to preserve client-provided correlation headers that are valid text but not numeric IDs.
pub fn current_correlation_value() -> Option<Arc<str>> {
    CORRELATION_VALUE.try_with(Clone::clone).ok()
}

/// Returns the transport data scoped to the current asynchronous delivery handler.
///
/// The returned value shares envelope headers through an `Arc`; it does not
/// clone payload bytes or allocate a metadata map.
pub fn current_transport_context() -> Option<TransportContext> {
    TRANSPORT_CONTEXT.try_with(Clone::clone).ok()
}

/// Runs a future with `correlation_id` available to the current asynchronous task chain.
pub async fn scope_correlation_id<T>(correlation_id: u64, future: impl Future<Output = T>) -> T {
    CORRELATION_ID.scope(correlation_id, future).await
}

/// Runs a future with an opaque correlation value available to the current asynchronous task chain.
///
/// This scope is independent from [`scope_correlation_id`] so transports that require numeric
/// identifiers can keep using their existing API while HTTP boundaries retain an incoming value
/// exactly as supplied.
pub async fn scope_correlation_value<T>(
    correlation_value: Arc<str>,
    future: impl Future<Output = T>,
) -> T {
    CORRELATION_VALUE.scope(correlation_value, future).await
}

/// Runs `future` with one received envelope's correlation, priority, and headers available.
///
/// This is the Rust ownership-safe counterpart to the source transport callback
/// scope. Callers retain ownership of the delivery and choose its acknowledgement
/// timing; nested typed publication can inherit this immutable context without a
/// background task, payload copy, or mutable global dictionary.
pub async fn scope_transport_context<T>(envelope: &Envelope, future: impl Future<Output = T>) -> T {
    scope_transport_context_value(TransportContext::from_envelope(envelope), future).await
}

/// Runs `future` with an explicit transport context scoped to the current task chain.
///
/// This avoids allocating a full [`Envelope`] when only propagation headers are available,
/// such as at an HTTP boundary. The context is shared through an `Arc` slice; cloning it
/// does not copy header strings.
pub async fn scope_transport_context_value<T>(
    context: TransportContext,
    future: impl Future<Output = T>,
) -> T {
    let correlation_id = context.correlation_id();
    TRANSPORT_CONTEXT
        .scope(context, async move {
            if let Some(correlation_id) = correlation_id {
                scope_correlation_id(correlation_id, future).await
            } else {
                future.await
            }
        })
        .await
}
