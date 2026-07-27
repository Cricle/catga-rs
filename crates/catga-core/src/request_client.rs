use std::{sync::Arc, time::Duration};

use async_trait::async_trait;

use crate::{CatgaError, CatgaResult, Envelope, ErrorCode, Message, Request};

/// A format-neutral request accepted by a typed remote client.
///
/// This blanket trait keeps remote-call bounds in one place without coupling application message
/// types to a particular serialization format.
pub trait RemoteRequest: Message + Request {}

impl<T> RemoteRequest for T where T: Message + Request {}

/// Sends one typed request through an already-configured remote endpoint.
///
/// The client owns destination binding, reply routing, timeout policy, and
/// serialization details. This keeps flow code independent of NATS, Redis,
/// RobustMQ, or any shared request-reply registry.
#[async_trait]
pub trait RequestClient<M>: Send + Sync
where
    M: RemoteRequest,
{
    /// Sends `request` using the client's configured default timeout.
    async fn request(&self, request: &M) -> CatgaResult<M::Response>;
}

/// A backend-native request/reply transport.
///
/// Implementations own their reply routing. This deliberately prevents a generic client from
/// imposing a shared pending-reply map or lock on transports with native inboxes or channels.
#[async_trait]
pub trait RequestTransport: Send + Sync {
    /// Sends one envelope to `destination` and returns its correlated reply before `timeout`.
    async fn request(
        &self,
        destination: &str,
        request: Envelope,
        timeout: Duration,
    ) -> CatgaResult<Envelope>;
}

/// A small destination-bound client for envelope-level cross-service request/reply.
pub struct EnvelopeRequestClient<T: ?Sized> {
    transport: Arc<T>,
    destination: Box<str>,
    default_timeout: Duration,
}

impl<T: ?Sized> Clone for EnvelopeRequestClient<T> {
    fn clone(&self) -> Self {
        Self {
            transport: Arc::clone(&self.transport),
            destination: self.destination.clone(),
            default_timeout: self.default_timeout,
        }
    }
}

impl<T> EnvelopeRequestClient<T>
where
    T: RequestTransport + ?Sized,
{
    /// Creates a client with one destination and default timeout.
    pub fn new(
        transport: Arc<T>,
        destination: impl Into<Box<str>>,
        default_timeout: Duration,
    ) -> CatgaResult<Self> {
        let destination = destination.into();
        if destination.is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "request destination must not be empty",
            ));
        }
        if default_timeout.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "request timeout must be greater than zero",
            ));
        }
        Ok(Self {
            transport,
            destination,
            default_timeout,
        })
    }

    /// Sends a request using the configured timeout.
    pub async fn request(&self, request: Envelope) -> CatgaResult<Envelope> {
        self.request_with_timeout(request, self.default_timeout)
            .await
    }

    /// Sends a request using an explicit timeout.
    pub async fn request_with_timeout(
        &self,
        request: Envelope,
        timeout: Duration,
    ) -> CatgaResult<Envelope> {
        if timeout.is_zero() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "request timeout must be greater than zero",
            ));
        }
        self.transport
            .request(&self.destination, request, timeout)
            .await
    }

    /// Returns the bound destination.
    pub fn destination(&self) -> &str {
        &self.destination
    }

    /// Returns the configured timeout without allocating or locking.
    pub const fn default_timeout(&self) -> Duration {
        self.default_timeout
    }
}
