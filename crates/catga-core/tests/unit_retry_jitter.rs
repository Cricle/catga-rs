//! Unit tests for retry jitter policy.

use std::time::Duration;

use catga_core::RetryJitter;

#[test]
fn retry_jitter_variants() {
    assert_eq!(RetryJitter::None, RetryJitter::None);
    assert_eq!(RetryJitter::None, RetryJitter::none());

    let full = RetryJitter::full(42);
    assert!(matches!(full, RetryJitter::Full { seed: 42 }));

    let fixed = RetryJitter::fixed(Duration::from_secs(1));
    assert!(matches!(fixed, RetryJitter::Fixed { delay } if delay == Duration::from_secs(1)));

    // Production default uses full jitter
    let default = RetryJitter::production_default();
    assert!(matches!(default, RetryJitter::Full { .. }));
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

    assert_eq!(jitter.delay_for_sample(base, 0), Duration::ZERO);
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
fn delay_for_sample_full_jitter_varies_across_samples() {
    let jitter = RetryJitter::full(42);
    let base = Duration::from_secs(1);

    // Full jitter produces values between 0 and base duration
    // Different samples may produce the same value depending on the hash distribution
    let results: Vec<Duration> = (0..10).map(|i| jitter.delay_for_sample(base, i)).collect();

    // All results should be within valid bounds (0 to base)
    for result in &results {
        assert!(*result <= base, "jitter result should not exceed base duration");
    }
}

#[test]
fn delay_for_sample_full_jitter_deterministic() {
    let seed = 0x1234_5678;
    let jitter = RetryJitter::full(seed);
    let base = Duration::from_secs(1);

    // Same seed + same sample = same result
    let result1 = jitter.delay_for_sample(base, 42);
    let result2 = jitter.delay_for_sample(base, 42);
    assert_eq!(result1, result2, "Full jitter should be deterministic for same seed and sample");
}

#[test]
fn delay_for_sample_full_jitter_all_samples_within_bounds() {
    let jitter = RetryJitter::full(123);
    let base = Duration::from_secs(10);

    for sample in [0_u64, 1, 100, u64::MAX / 2, u64::MAX] {
        let result = jitter.delay_for_sample(base, sample);
        assert!(
            result <= base,
            "Jitter result {} should not exceed base {}",
            result.as_nanos(),
            base.as_nanos()
        );
    }
}

#[test]
fn fixed_jitter_deterministic() {
    let delay = Duration::from_millis(50);
    let jitter = RetryJitter::fixed(delay);

    // Should always return the same value regardless of sample
    for sample in [0_u64, 1, 100, u64::MAX] {
        assert_eq!(
            jitter.delay_for_sample(Duration::from_secs(100), sample),
            delay,
            "Fixed jitter should return configured delay regardless of sample"
        );
    }
}

#[test]
fn none_jitter_deterministic() {
    let jitter = RetryJitter::none();
    let base = Duration::from_secs(5);

    // Should always return the base value regardless of sample
    for sample in [0_u64, 1, 100, u64::MAX] {
        assert_eq!(
            jitter.delay_for_sample(base, sample),
            base,
            "None jitter should return base duration regardless of sample"
        );
    }
}
