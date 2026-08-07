//! Helper functions for the DSL flow execution engine.

use std::time::Duration;

/// Computes an exponential backoff delay for retry operations.
///
/// Each retry doubles the `initial_delay` and saturates at `Duration::MAX`.
pub fn retry_delay(initial_delay: Duration, retry: usize) -> Duration {
    let multiplier = u32::try_from(retry)
        .ok()
        .and_then(|retry| 1_u32.checked_shl(retry))
        .unwrap_or(u32::MAX);
    initial_delay.saturating_mul(multiplier)
}
