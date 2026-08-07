//! Bounded, deterministic retry-jitter policies.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

/// Stable non-zero seed for the bounded production full-jitter sequence.
const DEFAULT_FULL_JITTER_SEED: u64 = 0xd1b5_4a32_d192_ed03;

/// Chooses how a retry delay is jittered.
///
/// [`RetryJitter::None`] preserves the configured exponential delay. Full
/// jitter selects a delay in the inclusive range from zero to that delay by
/// using a fixed-size atomic pseudo-random state. Fixed jitter is intended for
/// deterministic tests and schedulers that supply their own delay policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryJitter {
    /// Keep the calculated exponential delay unchanged.
    None,
    /// Select a deterministic full-jitter value using `seed` as its initial state.
    Full {
        /// Initial state for the bounded pseudo-random sequence.
        seed: u64,
    },
    /// Replace every calculated delay with one deterministic delay.
    Fixed {
        /// Delay returned for every retry.
        delay: Duration,
    },
}

impl RetryJitter {
    /// Returns the bounded full-jitter policy used by production constructors.
    pub const fn production_default() -> Self {
        Self::Full {
            seed: DEFAULT_FULL_JITTER_SEED,
        }
    }

    /// Returns the compatibility policy that leaves calculated delays unchanged.
    pub const fn none() -> Self {
        Self::None
    }

    /// Returns a full-jitter policy seeded with `seed`.
    pub const fn full(seed: u64) -> Self {
        Self::Full { seed }
    }

    /// Returns a deterministic policy that uses `delay` for every retry.
    pub const fn fixed(delay: Duration) -> Self {
        Self::Fixed { delay }
    }

    /// Converts a known full-jitter sample into a delay.
    ///
    /// A sample of zero returns zero and `u64::MAX` returns `base`. This helper
    /// is deterministic so tests can verify delay bounds without sleeping.
    pub fn delay_for_sample(self, base: Duration, sample: u64) -> Duration {
        match self {
            Self::None => base,
            Self::Fixed { delay } => delay,
            Self::Full { .. } => scale_duration(base, sample),
        }
    }
}

pub(crate) struct RetryJitterState {
    jitter: RetryJitter,
    state: AtomicU64,
}

impl RetryJitterState {
    pub(crate) const fn new(jitter: RetryJitter) -> Self {
        let state = match jitter {
            RetryJitter::Full { seed } => seed,
            RetryJitter::None | RetryJitter::Fixed { .. } => 0,
        };
        Self {
            jitter,
            state: AtomicU64::new(state),
        }
    }

    pub(crate) fn delay(&self, base: Duration) -> Duration {
        let sample = match self.jitter {
            RetryJitter::Full { .. } => self.next_sample(),
            RetryJitter::None | RetryJitter::Fixed { .. } => 0,
        };
        self.jitter.delay_for_sample(base, sample)
    }

    pub(crate) const fn policy(&self) -> RetryJitter {
        self.jitter
    }

    fn next_sample(&self) -> u64 {
        let mut current = self.state.load(Ordering::Relaxed);
        loop {
            let next = current.wrapping_add(0x9e37_79b9_7f4a_7c15);
            match self.state.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return mix64(next),
                Err(observed) => current = observed,
            }
        }
    }
}

fn scale_duration(base: Duration, sample: u64) -> Duration {
    let base_nanos = base.as_nanos();
    let divisor = u128::from(u64::MAX);
    let sample = u128::from(sample);
    let whole = base_nanos / divisor;
    let remainder = base_nanos % divisor;
    let nanos = whole
        .saturating_mul(sample)
        .saturating_add(remainder.saturating_mul(sample) / divisor);
    duration_from_nanos(nanos)
}

fn duration_from_nanos(nanos: u128) -> Duration {
    const NANOS_PER_SECOND: u128 = 1_000_000_000;
    let seconds = nanos / NANOS_PER_SECOND;
    if seconds > u128::from(u64::MAX) {
        return Duration::MAX;
    }
    Duration::new(seconds as u64, (nanos % NANOS_PER_SECOND) as u32)
}

fn mix64(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_jitter_variants() {
        assert_eq!(RetryJitter::None, RetryJitter::None);
        assert_eq!(RetryJitter::None, RetryJitter::none());

        let full = RetryJitter::full(42);
        assert!(matches!(full, RetryJitter::Full { seed: 42 }));

        let fixed = RetryJitter::fixed(Duration::from_secs(1));
        assert!(matches!(fixed, RetryJitter::Fixed { delay } if delay == Duration::from_secs(1)));

        assert_eq!(
            RetryJitter::production_default(),
            RetryJitter::full(DEFAULT_FULL_JITTER_SEED)
        );
    }

    #[test]
    fn delay_for_sample_none_preserves_base() {
        let jitter = RetryJitter::none();
        let base = Duration::from_secs(5);
        assert_eq!(jitter.delay_for_sample(base, 0), base);
        assert_eq!(jitter.delay_for_sample(base, u64::MAX), base);
    }

    #[test]
    fn delay_for_sample_fixed_returns_configured_delay() {
        let delay = Duration::from_millis(100);
        let jitter = RetryJitter::fixed(delay);
        let base = Duration::from_secs(10);
        assert_eq!(jitter.delay_for_sample(base, 0), delay);
        assert_eq!(jitter.delay_for_sample(base, u64::MAX), delay);
    }

    #[test]
    fn delay_for_sample_full_jitter_bounds() {
        let jitter = RetryJitter::full(0);
        let base = Duration::from_secs(1);

        // sample of 0 returns 0
        assert_eq!(jitter.delay_for_sample(base, 0), Duration::ZERO);

        // sample of u64::MAX returns base
        assert_eq!(jitter.delay_for_sample(base, u64::MAX), base);
    }

    #[test]
    fn delay_for_sample_full_jitter_scales_proportionally() {
        let jitter = RetryJitter::full(0);
        let base = Duration::from_secs(10);

        let half_sample = u64::MAX / 2;
        let result = jitter.delay_for_sample(base, half_sample);
        assert!(result <= base);
        assert!(result > Duration::ZERO);
    }

    #[test]
    fn retry_jitter_state_construction() {
        let none_state = RetryJitterState::new(RetryJitter::None);
        assert_eq!(none_state.policy(), RetryJitter::None);
        assert_eq!(
            none_state.delay(Duration::from_secs(1)),
            Duration::from_secs(1)
        );

        let fixed_state = RetryJitterState::new(RetryJitter::fixed(Duration::from_millis(50)));
        assert!(
            matches!(fixed_state.policy(), RetryJitter::Fixed { delay } if delay == Duration::from_millis(50))
        );
        assert_eq!(
            fixed_state.delay(Duration::from_secs(1)),
            Duration::from_millis(50)
        );
    }

    #[test]
    fn retry_jitter_state_full_jitter_sampling() {
        let seed = 0x1234_5678_9ABC_DEF0;
        let state = RetryJitterState::new(RetryJitter::full(seed));
        assert!(matches!(state.policy(), RetryJitter::Full { .. }));

        // Multiple samples should produce different values
        let base = Duration::from_secs(1);
        let sample1 = state.delay(base);
        let sample2 = state.delay(base);
        let sample3 = state.delay(base);

        // All should be bounded by [0, base]
        assert!(sample1 <= base);
        assert!(sample2 <= base);
        assert!(sample3 <= base);

        // At least one should be different (with high probability)
        let all_same = sample1 == sample2 && sample2 == sample3;
        assert!(
            !all_same,
            "Samples should vary (with overwhelming probability)"
        );
    }

    #[test]
    fn scale_duration_zero_base_returns_zero() {
        let result = scale_duration(Duration::ZERO, u64::MAX);
        assert_eq!(result, Duration::ZERO);
    }

    #[test]
    fn scale_duration_zero_sample_returns_zero() {
        let result = scale_duration(Duration::from_secs(10), 0);
        assert_eq!(result, Duration::ZERO);
    }

    #[test]
    fn scale_duration_max_sample_returns_base() {
        let base = Duration::from_secs(5);
        let result = scale_duration(base, u64::MAX);
        assert_eq!(result, base);
    }

    #[test]
    fn duration_from_nanos_basic() {
        assert_eq!(duration_from_nanos(0), Duration::ZERO);
        assert_eq!(duration_from_nanos(1_000_000_000), Duration::from_secs(1));
        assert_eq!(
            duration_from_nanos(1_500_000_000),
            Duration::new(1, 500_000_000)
        );
    }

    #[test]
    fn duration_from_nanos_excessive_returns_max() {
        // Duration::MAX in nanos is approximately 2^64 seconds worth
        let huge = u128::MAX;
        assert_eq!(duration_from_nanos(huge), Duration::MAX);
    }

    #[test]
    fn mix64_produces_deterministic_output() {
        let input = 0x1234_5678_9ABC_DEF0_u64;
        let output1 = mix64(input);
        let output2 = mix64(input);
        assert_eq!(output1, output2);
        assert_ne!(output1, input);
    }

    #[test]
    fn mix64_is_bijective_for_different_inputs() {
        // Different inputs should produce different outputs
        let results: Vec<u64> = (0..100).map(mix64).collect();
        let unique: std::collections::HashSet<u64> = results.iter().cloned().collect();
        assert_eq!(
            results.len(),
            unique.len(),
            "mix64 should produce unique values for inputs 0..100"
        );
    }
}
