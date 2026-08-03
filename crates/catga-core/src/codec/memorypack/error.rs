use thiserror::Error;

/// Errors returned while encoding or decoding MemoryPack data.
#[derive(Debug, Error)]
pub enum MemoryPackError {
    /// A received frame exceeded a configured resource budget.
    #[error("MemoryPack receive limit exceeded for {resource}: maximum is {limit}")]
    LimitExceeded {
        /// The budgeted resource that was exceeded.
        resource: &'static str,
        /// The configured upper bound.
        limit: usize,
    },

    #[error("invalid MemoryPack decode limit: {0}")]
    /// A requested decode-limit configuration is invalid.
    InvalidLimit(String),

    #[error("trailing bytes remain after decoding a MemoryPack frame")]
    /// A decoder completed before consuming its complete frame.
    TrailingBytes,

    #[error(transparent)]
    /// An underlying byte-reader I/O error.
    Io(#[from] std::io::Error),

    #[error(transparent)]
    /// UTF-8 validation failed while materializing an owned string.
    Utf8Error(#[from] std::string::FromUtf8Error),

    #[error("Invalid UTF-8 or UTF-16 string data")]
    /// UTF-8 or UTF-16 text was malformed.
    InvalidUtf8,

    #[error("Invalid length: {0}")]
    /// A signed MemoryPack length marker was invalid.
    InvalidLength(i32),

    #[error("Serialization error: {0}")]
    /// Encoding could not represent the requested value.
    SerializationError(String),

    #[error("Deserialization error: {0}")]
    /// The received representation does not describe a valid value.
    DeserializationError(String),

    #[error("Buffer too small")]
    /// The supplied output buffer cannot hold the encoded representation.
    BufferTooSmall,

    #[error("Invalid Unicode code point")]
    /// A UTF-16 sequence did not represent a Unicode scalar value.
    InvalidCodePoint,

    #[error("Unexpected end of data")]
    /// The input ended before the requested value was complete.
    UnexpectedEnd,

    #[error("Unexpected end of buffer")]
    /// A cursor operation would move beyond the received frame.
    UnexpectedEndOfBuffer,

    #[error("UTF-16 strings are not supported for zero-copy deserialization")]
    /// A zero-copy string operation encountered a UTF-16 wire representation.
    Utf16NotSupportedForZeroCopy,
}
