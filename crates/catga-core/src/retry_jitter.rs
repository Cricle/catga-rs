//! Bounded, deterministic retry-jitter policies.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

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
