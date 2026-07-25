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
#[serde(field_identifier, rename_all = "snake_case")]
enum CatgaErrorField {
    Code,
    Message,
    Details,
    Retryable,
}

struct CatgaErrorVisitor;

impl<'de> de::Visitor<'de> for CatgaErrorVisitor {
    type Value = CatgaError;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a Catga error")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        let code = sequence
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(0, &self))?;
        let message = sequence
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(1, &self))?;
        // Postcard reports a missing trailing sequence element as an error rather than `None`.
        // Treat that legacy end-of-frame condition as the serde default for new fields.
        let details = match sequence.next_element::<Option<Box<str>>>() {
            Ok(details) => details.flatten(),
            Err(_) => None,
        };
        let retryable = match sequence.next_element::<Option<bool>>() {
            Ok(retryable) => retryable.flatten(),
            Err(_) => None,
        };

        Ok(CatgaError {
            code,
            message,
            details: details.map(|details| bounded_details(&details)),
            retryable,
        })
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        let mut code = None;
        let mut message = None;
        let mut details: Option<Option<Box<str>>> = None;
        let mut retryable: Option<Option<bool>> = None;

        while let Some(field) = map.next_key()? {
            match field {
                CatgaErrorField::Code => {
                    if code.is_some() {
                        return Err(de::Error::duplicate_field("code"));
                    }
                    code = Some(map.next_value()?);
                }
                CatgaErrorField::Message => {
                    if message.is_some() {
                        return Err(de::Error::duplicate_field("message"));
                    }
                    message = Some(map.next_value()?);
                }
                CatgaErrorField::Details => {
                    if details.is_some() {
                        return Err(de::Error::duplicate_field("details"));
                    }
                    details = Some(map.next_value()?);
                }
                CatgaErrorField::Retryable => {
                    if retryable.is_some() {
                        return Err(de::Error::duplicate_field("retryable"));
                    }
                    retryable = Some(map.next_value()?);
                }
            }
        }

        Ok(CatgaError {
            code: code.ok_or_else(|| de::Error::missing_field("code"))?,
            message: message.ok_or_else(|| de::Error::missing_field("message"))?,
            details: match details {
                Some(details) => details.map(|details| bounded_details(&details)),
                None => None,
            },
            retryable: retryable.flatten(),
        })
    }
}

impl<'de> Deserialize<'de> for CatgaError {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_struct(
            "CatgaError",
            &["code", "message", "details", "retryable"],
            CatgaErrorVisitor,
        )
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
