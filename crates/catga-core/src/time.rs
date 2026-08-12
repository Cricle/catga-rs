//! Unix-epoch millisecond helpers shared by in-memory stores and adapters.
//!
//! The helpers differ only in how they handle a system clock set before the
//! Unix epoch and millisecond counts beyond the representable range. Callers
//! pick the conversion whose failure mode matches their persisted data: most
//! stores clamp to a default, databases keep a signed value, and validations
//! report an error.

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

/// Returns the current Unix epoch time in milliseconds.
///
/// A system clock preceding the epoch yields `0`; millisecond counts beyond
/// the `u64` range saturate at [`u64::MAX`].
///
/// ```
/// let now = catga_core::time::now_unix_millis();
/// assert!(now > 0);
/// ```
#[must_use]
pub fn now_unix_millis() -> u64 {
    now_unix_millis_or(0)
}

/// Returns the current Unix epoch time in milliseconds, or `default` when the
/// system clock precedes the epoch.
///
/// Millisecond counts beyond the `u64` range saturate at [`u64::MAX`].
/// Callers storing the value in an atomic "last success" cell pass a
/// non-zero `default` so an unset cell stays distinguishable from a real
/// timestamp.
#[must_use]
pub fn now_unix_millis_or(default: u64) -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(default, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// Reports why a [`SystemTime`] cannot convert to unsigned Unix epoch
/// milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnixMillisError {
    /// The time precedes the Unix epoch.
    BeforeEpoch,
    /// The millisecond count exceeds the `u64` range.
    ExceedsRange,
}

impl fmt::Display for UnixMillisError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BeforeEpoch => formatter.write_str("time precedes the Unix epoch"),
            Self::ExceedsRange => formatter.write_str("millisecond count exceeds the u64 range"),
        }
    }
}

impl std::error::Error for UnixMillisError {}

/// Converts a system time to unsigned Unix epoch milliseconds.
///
/// # Errors
///
/// Returns [`UnixMillisError::BeforeEpoch`] when `time` precedes the epoch
/// and [`UnixMillisError::ExceedsRange`] when the millisecond count exceeds
/// the `u64` range.
///
/// ```
/// use std::time::{Duration, SystemTime};
///
/// let epoch = SystemTime::UNIX_EPOCH;
/// assert_eq!(catga_core::time::checked_unix_millis(epoch), Ok(0));
/// assert_eq!(
///     catga_core::time::checked_unix_millis(epoch + Duration::from_millis(7)),
///     Ok(7)
/// );
/// assert!(catga_core::time::checked_unix_millis(epoch - Duration::from_millis(1)).is_err());
/// ```
pub fn checked_unix_millis(time: SystemTime) -> Result<u64, UnixMillisError> {
    let elapsed = time
        .duration_since(UNIX_EPOCH)
        .map_err(|_| UnixMillisError::BeforeEpoch)?;
    u64::try_from(elapsed.as_millis()).map_err(|_| UnixMillisError::ExceedsRange)
}

/// Converts a system time to signed Unix epoch milliseconds for database
/// columns, preserving times preceding the epoch as negative values.
///
/// Returns `None` when the magnitude exceeds the `i64` range in either
/// direction.
///
/// ```
/// use std::time::{Duration, SystemTime};
///
/// let epoch = SystemTime::UNIX_EPOCH;
/// assert_eq!(catga_core::time::signed_unix_millis(epoch), Some(0));
/// assert_eq!(
///     catga_core::time::signed_unix_millis(epoch - Duration::from_millis(7)),
///     Some(-7)
/// );
/// ```
#[must_use]
pub fn signed_unix_millis(value: SystemTime) -> Option<i64> {
    match value.duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis()).ok(),
        Err(error) => {
            let milliseconds = i64::try_from(error.duration().as_millis()).ok()?;
            milliseconds.checked_neg()
        }
    }
}
