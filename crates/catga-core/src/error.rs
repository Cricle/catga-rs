/// Categories used to classify framework failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorCode {
    /// Input does not meet the handler's validation rules.
    Validation,
    /// A requested resource does not exist.
    NotFound,
    /// An operation conflicts with persisted state.
    Conflict,
    /// Work was cancelled before completion.
    Cancelled,
    /// Work exceeded its configured deadline.
    Timeout,
    /// No configured component supports the requested operation.
    Unsupported,
    /// The operation may succeed when retried.
    Transient,
    /// An unexpected framework failure occurred.
    Internal,
}

/// A structured Catga failure.
#[derive(Clone, Debug, Eq, PartialEq)]
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
