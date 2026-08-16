//! Error types, error codes, and result aliases for the Catga framework.
//!
//! All framework operations return [`CatgaResult<T>`] which is an alias for
//! `Result<T, CatgaError>`. The [`CatgaError`] type carries a stable [`ErrorCode`]
//! classification that callers use for control-flow decisions.
//!
//! # Error Code Categories
//!
//! [`ErrorCode`] partitions failures into actionable categories:
//! - **Validation** — Input rejected before reaching a handler
//! - **HandlerFailed** — Handler returned an error
//! - **HandlerNotFound** — No handler registered for the message type
//! - **PipelineFailed** — A pipeline behavior reported failure
//! - **PersistenceFailed** — Storage operation failed
//! - **LockFailed** — Distributed lock acquisition failed
//! - **TransportFailed** — Network transport operation failed
//! - **SerializationFailed** — Codec encode/decode failed
//! - **Conflict** — Optimistic concurrency conflict detected
//! - **NotFound** — Requested resource does not exist
//! - **Unauthorized/Forbidden** — Security check failed
//! - **Flow*** — Saga/flow orchestration failures
//!
//! # Stability
//!
//! Error codes are stable identifiers. [`ErrorCode::as_stable_str`] emits a
//! lowercase snake_case name suitable for logging and wire protocols.

use std::fmt;

use serde::{Deserialize, Serialize, de};

use crate::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    MemoryPackWriter, MemoryPackable,
};

/// Maximum UTF-8 byte length retained for optional error details.
pub const MAX_ERROR_DETAILS_BYTES: usize = 1024;

/// Categories used to classify framework failures.
///
/// New variants may be added in future releases, so this enum is marked
/// [`#[non_exhaustive]`][non_exhaustive]: code outside this crate must match it with a
/// wildcard arm and map unrecognized categories to a conservative default (typically
/// [`ErrorCode::Internal`] behavior such as HTTP 500).
///
/// ```
/// use catga_core::ErrorCode;
///
/// assert_eq!(ErrorCode::Validation.as_stable_str(), "validation");
/// assert_eq!(ErrorCode::from_stable_str("conflict"), Some(ErrorCode::Conflict));
/// assert!(ErrorCode::Transient.is_retryable());
/// assert!(!ErrorCode::Validation.is_retryable());
/// ```
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum ErrorCode {
    /// Input does not meet the handler's validation rules.
    Validation,
    /// A request handler reported a framework-classified failure.
    HandlerFailed,
    /// No request or command handler is registered for the message type.
    HandlerNotFound,
    /// A mediator pipeline behavior reported a framework-classified failure.
    PipelineFailed,
    /// A persistence operation failed without a safe generic retry guarantee.
    PersistenceFailed,
    /// A distributed lock operation failed without a safe generic retry guarantee.
    LockFailed,
    /// Transport communication failed and is normally safe to retry.
    TransportFailed,
    /// Serialization or deserialization failed for a message contract.
    SerializationFailed,
    /// A requested resource does not exist.
    NotFound,
    /// An operation conflicts with persisted state.
    Conflict,
    /// Authentication is required before the operation may proceed.
    Unauthorized,
    /// The authenticated identity is not permitted to perform the operation.
    Forbidden,
    /// Work was cancelled before completion.
    Cancelled,
    /// Work exceeded its configured deadline.
    Timeout,
    /// A durable flow failed its business or orchestration operation.
    FlowFailed,
    /// A durable flow was cancelled; unlike transport cancellation this is a terminal flow state.
    FlowCancelled,
    /// A durable flow exceeded its configured deadline and may be retried by its owner.
    FlowTimeout,
    /// A durable flow is compensating after a failed operation.
    FlowCompensating,
    /// No configured component supports the requested operation.
    Unsupported,
    /// The operation may succeed when retried.
    Transient,
    /// The component is intentionally not accepting work or cannot currently serve it.
    Unavailable,
    /// An unexpected framework failure occurred.
    Internal,
}

impl ErrorCode {
    /// Returns the stable wire name for this failure category.
    pub const fn as_stable_str(self) -> &'static str {
        match self {
            Self::Validation => "validation",
            Self::HandlerFailed => "handler_failed",
            Self::HandlerNotFound => "handler_not_found",
            Self::PipelineFailed => "pipeline_failed",
            Self::PersistenceFailed => "persistence_failed",
            Self::LockFailed => "lock_failed",
            Self::TransportFailed => "transport_failed",
            Self::SerializationFailed => "serialization_failed",
            Self::NotFound => "not_found",
            Self::Conflict => "conflict",
            Self::Unauthorized => "unauthorized",
            Self::Forbidden => "forbidden",
            Self::Cancelled => "cancelled",
            Self::Timeout => "timeout",
            Self::FlowFailed => "flow_failed",
            Self::FlowCancelled => "flow_cancelled",
            Self::FlowTimeout => "flow_timeout",
            Self::FlowCompensating => "flow_compensating",
            Self::Unsupported => "unsupported",
            Self::Transient => "transient",
            Self::Unavailable => "unavailable",
            Self::Internal => "internal",
        }
    }

    /// Parses an error category emitted by [`Self::as_stable_str`].
    ///
    /// This accepts Catga's stable Rust names and all upstream C# names. Upstream names are
    /// accepted only at the boundary; [`Self::as_stable_str`] always emits the typed, stable
    /// Rust name.
    pub fn from_stable_str(value: &str) -> Option<Self> {
        match value {
            "validation" => Some(Self::Validation),
            "handler_failed" => Some(Self::HandlerFailed),
            "handler_not_found" => Some(Self::HandlerNotFound),
            "pipeline_failed" => Some(Self::PipelineFailed),
            "persistence_failed" => Some(Self::PersistenceFailed),
            "lock_failed" => Some(Self::LockFailed),
            "transport_failed" => Some(Self::TransportFailed),
            "serialization_failed" => Some(Self::SerializationFailed),
            "not_found" => Some(Self::NotFound),
            "conflict" => Some(Self::Conflict),
            "unauthorized" => Some(Self::Unauthorized),
            "forbidden" => Some(Self::Forbidden),
            "cancelled" => Some(Self::Cancelled),
            "timeout" => Some(Self::Timeout),
            "flow_failed" => Some(Self::FlowFailed),
            "flow_cancelled" => Some(Self::FlowCancelled),
            "flow_timeout" => Some(Self::FlowTimeout),
            "flow_compensating" => Some(Self::FlowCompensating),
            "unsupported" => Some(Self::Unsupported),
            "transient" => Some(Self::Transient),
            "unavailable" => Some(Self::Unavailable),
            "internal" => Some(Self::Internal),
            "VALIDATION_FAILED" => Some(Self::Validation),
            "HANDLER_FAILED" => Some(Self::HandlerFailed),
            "HANDLER_NOT_FOUND" => Some(Self::HandlerNotFound),
            "PIPELINE_FAILED" => Some(Self::PipelineFailed),
            "PERSISTENCE_FAILED" => Some(Self::PersistenceFailed),
            "LOCK_FAILED" => Some(Self::LockFailed),
            "TRANSPORT_FAILED" => Some(Self::TransportFailed),
            "SERIALIZATION_FAILED" => Some(Self::SerializationFailed),
            "TIMEOUT" => Some(Self::Timeout),
            "CANCELLED" => Some(Self::Cancelled),
            "INTERNAL_ERROR" => Some(Self::Internal),
            "NOT_FOUND" => Some(Self::NotFound),
            "CONFLICT" => Some(Self::Conflict),
            "UNAUTHORIZED" => Some(Self::Unauthorized),
            "FORBIDDEN" => Some(Self::Forbidden),
            "FLOW_FAILED" => Some(Self::FlowFailed),
            "FLOW_CANCELLED" => Some(Self::FlowCancelled),
            "FLOW_TIMEOUT" => Some(Self::FlowTimeout),
            "FLOW_COMPENSATING" => Some(Self::FlowCompensating),
            _ => None,
        }
    }

    /// Returns whether this category is normally safe to retry.
    ///
    /// Only failures that are transient by contract are retryable automatically. Persistence and
    /// lock failures remain non-retryable because callers cannot infer idempotency or ownership.
    pub const fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::TransportFailed
                | Self::Timeout
                | Self::FlowTimeout
                | Self::Transient
                | Self::Unavailable
        )
    }

    /// Returns the conventional HTTP status code for this error category as a `u16`.
    ///
    /// This is framework-agnostic: any HTTP adapter (Axum, Actix, Poem, etc.) converts
    /// the returned value to its own status-code type without duplicating the mapping.
    pub const fn http_status_u16(self) -> u16 {
        match self {
            Self::Validation => 422,
            Self::HandlerFailed
            | Self::PipelineFailed
            | Self::SerializationFailed
            | Self::FlowFailed
            | Self::FlowCompensating => 400,
            Self::HandlerNotFound | Self::NotFound => 404,
            Self::PersistenceFailed | Self::LockFailed | Self::TransportFailed => 503,
            Self::Conflict => 409,
            Self::Unauthorized => 401,
            Self::Forbidden => 403,
            Self::Cancelled | Self::Timeout | Self::FlowCancelled | Self::FlowTimeout => 408,
            Self::Unsupported => 501,
            Self::Transient | Self::Unavailable => 503,
            Self::Internal => 500,
        }
    }
}

/// A structured Catga failure.
///
/// A failure has a stable category, explanatory message, optional diagnostic details, and an
/// optional retryability override. Incoming details are retained only up to
/// [`MAX_ERROR_DETAILS_BYTES`] at a UTF-8 character boundary. The bounded details decoder asks
/// binary formats for a borrowed string, so transport frames do not allocate an unbounded remote
/// detail string before the limit is applied. Protocol-specific compatibility for legacy error
/// layouts belongs to the relevant codec boundary rather than this type's deserializer.
///
/// A failure may also retain an optional causal source error for diagnostic chaining through
/// [`std::error::Error::source`]. The source is process-local: it is never serialized by the
/// RPC wire format or MemoryPack codecs, [`Clone`] does not copy it, and equality compares
/// only the wire-visible fields.
///
/// ```
/// use catga_core::{CatgaError, ErrorCode};
///
/// let error = CatgaError::new(ErrorCode::Validation, "field is required")
///     .with_details("input: {}");
/// assert_eq!(error.code(), ErrorCode::Validation);
/// assert_eq!(error.message(), "field is required");
/// assert_eq!(error.details(), Some("input: {}"));
/// assert!(!error.is_retryable());
/// ```
#[derive(Debug, Serialize)]
pub struct CatgaError {
    code: ErrorCode,
    message: Box<str>,
    #[serde(default)]
    details: Option<Box<str>>,
    /// Explicit retryability assertion, kept distinct from [`ErrorCode::is_retryable`] so a
    /// trusted producer (a constructor or a wire frame) may override the category default.
    /// [`CatgaError::is_retryable`] falls back to the category default when a legacy frame
    /// omits the field (`None`).
    #[serde(default)]
    retryable: Option<bool>,
    /// Process-local causal source, exposed through [`std::error::Error::source`] and never
    /// serialized across wire boundaries.
    #[serde(skip)]
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl Clone for CatgaError {
    /// Clones the wire-visible fields. Error sources are not generically cloneable, so the
    /// clone has no [`std::error::Error::source`].
    fn clone(&self) -> Self {
        Self {
            code: self.code,
            message: self.message.clone(),
            details: self.details.clone(),
            retryable: self.retryable,
            source: None,
        }
    }
}

impl PartialEq for CatgaError {
    /// Compares only the wire-visible fields; the process-local source is diagnostic-only.
    fn eq(&self, other: &Self) -> bool {
        self.code == other.code
            && self.message == other.message
            && self.details == other.details
            && self.retryable == other.retryable
    }
}

impl Eq for CatgaError {}

#[derive(Deserialize)]
struct CatgaErrorSerdeWire {
    code: ErrorCode,
    message: Box<str>,
    #[serde(default)]
    details: Option<BoundedDetails>,
    #[serde(default)]
    retryable: Option<bool>,
}

struct BoundedDetails(Box<str>);

impl<'de> Deserialize<'de> for BoundedDetails {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_str(BoundedDetailsVisitor)
    }
}

struct BoundedDetailsVisitor;

impl<'de> de::Visitor<'de> for BoundedDetailsVisitor {
    type Value = BoundedDetails;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a UTF-8 error detail string")
    }

    fn visit_borrowed_str<E>(self, details: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(BoundedDetails(bounded_details(details)))
    }

    fn visit_str<E>(self, details: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(BoundedDetails(bounded_details(details)))
    }

    fn visit_string<E>(self, details: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(BoundedDetails(bounded_details(&details)))
    }
}

impl<'de> Deserialize<'de> for CatgaError {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let wire = CatgaErrorSerdeWire::deserialize(deserializer)?;
        Ok(Self {
            code: wire.code,
            message: wire.message,
            details: wire.details.map(|details| details.0),
            retryable: wire.retryable,
            source: None,
        })
    }
}

impl CatgaError {
    /// Creates an error with a stable category and an explanatory message.
    ///
    /// The error has no retained source; use [`Self::with_source`] to keep the causal error
    /// for diagnostic chaining.
    pub fn new(code: ErrorCode, message: impl Into<Box<str>>) -> Self {
        Self {
            code,
            message: message.into(),
            details: None,
            retryable: Some(code.is_retryable()),
            source: None,
        }
    }

    /// Creates an error with a stable category, an explanatory message, and a retained causal
    /// source error.
    ///
    /// The source is exposed through [`std::error::Error::source`] for diagnostic chaining but
    /// is process-local: the RPC wire format and MemoryPack codecs transmit only the code,
    /// message, details, and retryability.
    ///
    /// ```
    /// use catga_core::{CatgaError, ErrorCode};
    /// use std::error::Error;
    ///
    /// let io_error = std::io::Error::other("connection refused");
    /// let error = CatgaError::with_source(
    ///     ErrorCode::TransportFailed,
    ///     "broker connection failed",
    ///     io_error,
    /// );
    /// assert_eq!(error.code(), ErrorCode::TransportFailed);
    /// assert_eq!(error.message(), "broker connection failed");
    /// assert_eq!(error.source().expect("source retained").to_string(), "connection refused");
    /// ```
    pub fn with_source(
        code: ErrorCode,
        message: impl Into<Box<str>>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            source: Some(Box::new(source)),
            ..Self::new(code, message)
        }
    }

    /// Creates a retryable [`ErrorCode::Transient`] error from a displayable source error.
    ///
    /// The source's `Display` output becomes the error message. Adapters use this to
    /// classify backend failures that are safe to retry.
    ///
    /// This constructor is lossy: the argument is stringified and not retained as the
    /// [`std::error::Error::source`]. When the argument is a concrete error type, prefer
    /// [`Self::transient_from`], which retains it for diagnostic chaining.
    ///
    /// ```
    /// use catga_core::{CatgaError, ErrorCode};
    ///
    /// let error = CatgaError::transient("connection refused");
    /// assert_eq!(error.code(), ErrorCode::Transient);
    /// assert_eq!(error.message(), "connection refused");
    /// assert!(error.is_retryable());
    /// ```
    pub fn transient(error: impl fmt::Display) -> Self {
        Self::new(ErrorCode::Transient, error.to_string())
    }

    /// Creates a retryable [`ErrorCode::Transient`] error that retains the source error.
    ///
    /// The source's `Display` output becomes the message and the error itself is retained as
    /// the [`std::error::Error::source`] for diagnostic chaining. Prefer this over
    /// [`Self::transient`] whenever the argument is a concrete error type.
    ///
    /// ```
    /// use catga_core::{CatgaError, ErrorCode};
    /// use std::error::Error;
    ///
    /// let io_error = std::io::Error::other("connection refused");
    /// let error = CatgaError::transient_from(io_error);
    /// assert_eq!(error.code(), ErrorCode::Transient);
    /// assert_eq!(error.message(), "connection refused");
    /// assert!(error.is_retryable());
    /// assert!(error.source().is_some());
    /// ```
    pub fn transient_from(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        let message = error.to_string();
        Self::with_source(ErrorCode::Transient, message, error)
    }

    /// Attaches optional diagnostic details, retaining at most
    /// [`MAX_ERROR_DETAILS_BYTES`] without splitting a UTF-8 character.
    pub fn with_details(mut self, details: impl AsRef<str>) -> Self {
        self.details = Some(bounded_details(details.as_ref()));
        self
    }

    /// Returns the error category.
    pub const fn code(&self) -> ErrorCode {
        self.code
    }

    /// Returns the explanatory error text.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns optional bounded diagnostic details supplied with this error.
    pub fn details(&self) -> Option<&str> {
        self.details.as_deref()
    }

    /// Returns whether callers may retry this error.
    ///
    /// An explicit retryability assertion (from a constructor or wire frame) wins; when the
    /// assertion is absent — e.g. a legacy wire frame that omits the field — the value falls
    /// back to the category default from [`ErrorCode::is_retryable`]. Errors constructed with
    /// [`Self::new`] always carry the category-derived assertion.
    pub const fn is_retryable(&self) -> bool {
        match self.retryable {
            Some(retryable) => retryable,
            None => self.code.is_retryable(),
        }
    }
}

impl fmt::Display for CatgaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message())
    }
}

impl std::error::Error for CatgaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|source| {
            let source: &(dyn std::error::Error + 'static) = source.as_ref();
            source
        })
    }
}

/// Truncates details to [`MAX_ERROR_DETAILS_BYTES`] without splitting a UTF-8 character.
pub fn bounded_details(details: &str) -> Box<str> {
    let mut end = details.len().min(MAX_ERROR_DETAILS_BYTES);
    while end > 0 && !details.is_char_boundary(end) {
        end -= 1;
    }
    details[..end].into()
}

/// The result returned by Catga operations.
pub type CatgaResult<T> = Result<T, CatgaError>;

/// The single bounded MemoryPack wire representation of a [`CatgaError`].
///
/// Every MemoryPack boundary that persists or transmits Catga failures — RPC responses and
/// durable flow records — shares this DTO so the wire layout, code mapping, and retryability
/// validation stay defined exactly once.
#[derive(Default, MemoryPackable)]
pub(crate) struct CatgaErrorWire {
    code: u8,
    message: String,
    details: Option<String>,
    retryable: bool,
}

impl From<&CatgaError> for CatgaErrorWire {
    fn from(error: &CatgaError) -> Self {
        Self {
            code: encode_error_code(error.code()),
            message: error.message().to_owned(),
            details: error.details().map(str::to_owned),
            retryable: error.is_retryable(),
        }
    }
}

impl TryFrom<CatgaErrorWire> for CatgaError {
    type Error = MemoryPackError;

    fn try_from(wire: CatgaErrorWire) -> Result<Self, Self::Error> {
        let code = decode_error_code(wire.code)?;
        if wire.retryable != code.is_retryable() {
            return Err(MemoryPackError::DeserializationError(
                "error retryability does not match its error code".into(),
            ));
        }
        let error = CatgaError::new(code, wire.message);
        Ok(match wire.details {
            Some(details) => error.with_details(details),
            None => error,
        })
    }
}

pub(crate) fn encode_error_code(value: ErrorCode) -> u8 {
    match value {
        ErrorCode::Validation => 0,
        ErrorCode::NotFound => 1,
        ErrorCode::Conflict => 2,
        ErrorCode::Unauthorized => 3,
        ErrorCode::Forbidden => 4,
        ErrorCode::Cancelled => 5,
        ErrorCode::Timeout => 6,
        ErrorCode::Unsupported => 7,
        ErrorCode::Transient => 8,
        ErrorCode::Unavailable => 9,
        ErrorCode::Internal => 10,
        ErrorCode::HandlerFailed => 11,
        ErrorCode::HandlerNotFound => 12,
        ErrorCode::PipelineFailed => 13,
        ErrorCode::PersistenceFailed => 14,
        ErrorCode::LockFailed => 15,
        ErrorCode::TransportFailed => 16,
        ErrorCode::SerializationFailed => 17,
        ErrorCode::FlowFailed => 18,
        ErrorCode::FlowCancelled => 19,
        ErrorCode::FlowTimeout => 20,
        ErrorCode::FlowCompensating => 21,
    }
}

pub(crate) fn decode_error_code(value: u8) -> Result<ErrorCode, MemoryPackError> {
    match value {
        0 => Ok(ErrorCode::Validation),
        1 => Ok(ErrorCode::NotFound),
        2 => Ok(ErrorCode::Conflict),
        3 => Ok(ErrorCode::Unauthorized),
        4 => Ok(ErrorCode::Forbidden),
        5 => Ok(ErrorCode::Cancelled),
        6 => Ok(ErrorCode::Timeout),
        7 => Ok(ErrorCode::Unsupported),
        8 => Ok(ErrorCode::Transient),
        9 => Ok(ErrorCode::Unavailable),
        10 => Ok(ErrorCode::Internal),
        11 => Ok(ErrorCode::HandlerFailed),
        12 => Ok(ErrorCode::HandlerNotFound),
        13 => Ok(ErrorCode::PipelineFailed),
        14 => Ok(ErrorCode::PersistenceFailed),
        15 => Ok(ErrorCode::LockFailed),
        16 => Ok(ErrorCode::TransportFailed),
        17 => Ok(ErrorCode::SerializationFailed),
        18 => Ok(ErrorCode::FlowFailed),
        19 => Ok(ErrorCode::FlowCancelled),
        20 => Ok(ErrorCode::FlowTimeout),
        21 => Ok(ErrorCode::FlowCompensating),
        value => Err(MemoryPackError::DeserializationError(format!(
            "invalid Catga error code: {value}"
        ))),
    }
}
