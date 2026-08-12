//! Helper functions for the DSL flow execution engine.

use std::time::Duration;

/// Computes an exponential backoff delay for retry operations.
///
/// Each retry doubles the `initial_delay` and saturates at `Duration::MAX`.
pub fn retry_delay(initial_delay: Duration, retry: usize) -> Duration {
    crate::resilience::retry_delay(initial_delay, retry)
}
