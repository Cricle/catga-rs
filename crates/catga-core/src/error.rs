use std::fmt;

use serde::{Deserialize, Serialize, de};

/// Maximum UTF-8 byte length retained for optional error details.
pub const MAX_ERROR_DETAILS_BYTES: usize = 1024;

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
    ///
    /// This also accepts source-style compatibility aliases without introducing new untyped
    /// categories: `TRANSPORT_FAILED` maps to [`Self::Unavailable`] and
    /// `SERIALIZATION_FAILED` maps to [`Self::Internal`]. These aliases are accepted only on
    /// input; [`Self::as_stable_str`] always emits the stable typed names.
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
            "TRANSPORT_FAILED" => Some(Self::Unavailable),
            "SERIALIZATION_FAILED" => Some(Self::Internal),
            _ => None,
        }
    }

    /// Returns whether this category is normally safe to retry.
    pub const fn is_retryable(self) -> bool {
        matches!(self, Self::Transient | Self::Timeout | Self::Unavailable)
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
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CatgaError {
    code: ErrorCode,
    message: Box<str>,
    #[serde(default)]
    details: Option<Box<str>>,
    #[serde(default)]
    retryable: Option<bool>,
}

#[derive(Deserialize)]
struct CatgaErrorWire {
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
        let wire = CatgaErrorWire::deserialize(deserializer)?;
        Ok(Self {
            code: wire.code,
            message: wire.message,
            details: wire.details.map(|details| details.0),
            retryable: wire.retryable,
        })
    }
}

impl CatgaError {
    /// Creates an error with a stable category and an explanatory message.
    pub fn new(code: ErrorCode, message: impl Into<Box<str>>) -> Self {
        Self {
            code,
            message: message.into(),
            details: None,
            retryable: Some(code.is_retryable()),
        }
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
    /// Errors constructed with [`Self::new`] derive this from their category. Legacy wire
    /// frames that omit retryability derive the same value when this accessor is called.
    pub const fn is_retryable(&self) -> bool {
        match self.retryable {
            Some(retryable) => retryable,
            None => self.code.is_retryable(),
        }
    }
}

fn bounded_details(details: &str) -> Box<str> {
    let mut end = details.len().min(MAX_ERROR_DETAILS_BYTES);
    while end > 0 && !details.is_char_boundary(end) {
        end -= 1;
    }
    details[..end].into()
}

/// The result returned by Catga operations.
pub type CatgaResult<T> = Result<T, CatgaError>;
