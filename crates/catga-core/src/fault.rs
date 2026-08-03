//! Structured failure events for request-processing faults.

use std::time::SystemTime;

use crate::{CatgaError, Event, Message, MessageTypeId, current_correlation_id};

/// An event describing one request failure.
///
/// ```
/// use catga_core::{CatgaError, ErrorCode, Fault};
///
/// let fault = Fault::new("order-42", CatgaError::new(ErrorCode::HandlerFailed, "timeout"));
/// assert_eq!(*fault.message(), "order-42");
/// assert_eq!(fault.error().code(), ErrorCode::HandlerFailed);
/// assert!(!fault.host().is_empty());
/// ```
#[derive(Clone)]
pub struct Fault<M> {
    message: M,
    error: CatgaError,
    correlation_id: Option<u64>,
    occurred_at: SystemTime,
    host: Box<str>,
}

impl<M> Fault<M> {
    /// Captures a failed message, its structured error, and the ambient correlation context.
    pub fn new(message: M, error: CatgaError) -> Self {
        Self {
            message,
            error,
            correlation_id: current_correlation_id(),
            occurred_at: SystemTime::now(),
            host: std::env::var("HOSTNAME")
                .unwrap_or_else(|_| "unknown".to_owned())
                .into_boxed_str(),
        }
    }

    /// Returns the original message that failed.
    pub const fn message(&self) -> &M {
        &self.message
    }

    /// Returns the structured failure from request processing.
    pub const fn error(&self) -> &CatgaError {
        &self.error
    }

    /// Returns the captured distributed correlation identifier, when available.
    pub const fn correlation_id(&self) -> Option<u64> {
        self.correlation_id
    }

    /// Returns when the failure was captured.
    pub const fn occurred_at(&self) -> SystemTime {
        self.occurred_at
    }

    /// Returns the host name captured for fault diagnostics.
    pub fn host(&self) -> &str {
        &self.host
    }
}

impl<M> Message for Fault<M> where M: Message + Clone {}

impl<M> Event for Fault<M>
where
    M: Message + Clone,
{
    type TypeId = FaultEventTypeId;
}

pub struct FaultEventTypeId;

impl MessageTypeId for FaultEventTypeId {
    const NAME: &'static str = "Fault";
}
