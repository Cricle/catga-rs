use serde::{Deserialize, Serialize};

/// Categories used to classify framework failures.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ErrorCode {
    /// Input does not meet the handler's validation rules.
    Validation,
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
            Self::NotFound => "not_found",
            Self::Conflict => "conflict",
            Self::Unauthorized => "unauthorized",
            Self::Forbidden => "forbidden",
            Self::Cancelled => "cancelled",
            Self::Timeout => "timeout",
            Self::Unsupported => "unsupported",
            Self::Transient => "transient",
            Self::Unavailable => "unavailable",
            Self::Internal => "internal",
        }
    }

    /// Parses an error category emitted by [`Self::as_stable_str`].
    pub fn from_stable_str(value: &str) -> Option<Self> {
        match value {
            "validation" => Some(Self::Validation),
            "not_found" => Some(Self::NotFound),
            "conflict" => Some(Self::Conflict),
            "unauthorized" => Some(Self::Unauthorized),
            "forbidden" => Some(Self::Forbidden),
            "cancelled" => Some(Self::Cancelled),
            "timeout" => Some(Self::Timeout),
            "unsupported" => Some(Self::Unsupported),
            "transient" => Some(Self::Transient),
            "unavailable" => Some(Self::Unavailable),
            "internal" => Some(Self::Internal),
            _ => None,
        }
    }
}

/// A structured Catga failure.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CatgaError {
    code: ErrorCode,
    message: Box<str>,
}

impl CatgaError {
    /// Creates an error with a stable category and an explanatory message.
    pub fn new(code: ErrorCode, message: impl Into<Box<str>>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    /// Returns the error category.
    pub const fn code(&self) -> ErrorCode {
        self.code
    }

    /// Returns the explanatory error text.
    pub fn message(&self) -> &str {
        &self.message
    }
}

/// The result returned by Catga operations.
pub type CatgaResult<T> = Result<T, CatgaError>;
