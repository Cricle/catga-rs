//! Tests for retry_jitter module.

use std::time::Duration;

use catga_core::RetryJitter;

#[test]
fn retry_jitter_none() {
    let jitter = RetryJitter::none();
    assert!(matches!(jitter, RetryJitter::None));
}

#[test]
fn retry_jitter_full_with_seed() {
    let jitter = RetryJitter::full(12345);
    assert!(matches!(jitter, RetryJitter::Full { seed: 12345 }));
}

#[test]
fn retry_jitter_fixed() {
    let jitter = RetryJitter::fixed(Duration::from_secs(5));
    assert!(matches!(jitter, RetryJitter::Fixed { delay } if delay == Duration::from_secs(5)));
}

#[test]
fn retry_jitter_production_default() {
    let jitter = RetryJitter::production_default();
    assert!(matches!(jitter, RetryJitter::Full { seed: _ }));
}

#[test]
fn retry_jitter_delay_for_sample_none() {
    let jitter = RetryJitter::none();
    let base = Duration::from_secs(10);
    assert_eq!(jitter.delay_for_sample(base, 0), base);
    assert_eq!(jitter.delay_for_sample(base, u64::MAX), base);
}

#[test]
fn retry_jitter_delay_for_sample_fixed() {
    let fixed_delay = Duration::from_secs(7);
    let jitter = RetryJitter::fixed(fixed_delay);
    let base = Duration::from_secs(10);
    assert_eq!(jitter.delay_for_sample(base, 0), fixed_delay);
    assert_eq!(jitter.delay_for_sample(base, u64::MAX), fixed_delay);
}

#[test]
fn retry_jitter_delay_for_sample_full_zero_sample() {
    let jitter = RetryJitter::full(42);
    let base = Duration::from_secs(10);
    assert_eq!(jitter.delay_for_sample(base, 0), Duration::ZERO);
}

#[test]
fn retry_jitter_delay_for_sample_full_max_sample() {
    let jitter = RetryJitter::full(42);
    let base = Duration::from_secs(10);
    assert_eq!(jitter.delay_for_sample(base, u64::MAX), base);
}

#[test]
fn retry_jitter_delay_for_sample_full_half_sample() {
    let jitter = RetryJitter::full(42);
    let base = Duration::from_secs(10);
    let half = u64::MAX / 2;
    let delay = jitter.delay_for_sample(base, half);
    assert!(delay > Duration::ZERO);
    assert!(delay < base);
}

#[test]
fn retry_jitter_equality() {
    assert_eq!(RetryJitter::none(), RetryJitter::none());
    assert_eq!(RetryJitter::full(123), RetryJitter::full(123));
    assert_ne!(RetryJitter::full(123), RetryJitter::full(456));
    assert_eq!(RetryJitter::fixed(Duration::from_secs(1)), RetryJitter::fixed(Duration::from_secs(1)));
    assert_ne!(RetryJitter::fixed(Duration::from_secs(1)), RetryJitter::fixed(Duration::from_secs(2)));
}

#[test]
fn retry_jitter_debug() {
    let jitter = RetryJitter::none();
    let debug_str = format!("{:?}", jitter);
    assert!(debug_str.contains("None"));
}

#[test]
fn retry_jitter_clone() {
    let jitter = RetryJitter::full(12345);
    let cloned = jitter;
    assert_eq!(jitter, cloned);
}

#[test]
fn retry_jitter_copy() {
    let jitter = RetryJitter::none();
    let copied = jitter;
    assert_eq!(jitter, copied);
}

#[test]
fn retry_jitter_scale_duration_base_nanos() {
    let jitter = RetryJitter::full(42);
    let base = Duration::from_nanos(1);
    let delay = jitter.delay_for_sample(base, 1);
    assert_eq!(delay, Duration::ZERO);
}

#[test]
fn retry_jitter_scale_duration_large_base() {
    let jitter = RetryJitter::full(42);
    let base = Duration::from_secs(u64::MAX);
    let delay = jitter.delay_for_sample(base, u64::MAX);
    assert_eq!(delay, base);
}

#[test]
fn retry_jitter_scale_duration_edge_cases() {
    let jitter = RetryJitter::full(42);
    let base = Duration::from_millis(100);

    let zero = jitter.delay_for_sample(base, 0);
    let max = jitter.delay_for_sample(base, u64::MAX);
    let half = jitter.delay_for_sample(base, u64::MAX / 2);

    assert_eq!(zero, Duration::ZERO);
    assert_eq!(max, base);
    assert!(half > Duration::ZERO);
    assert!(half < base);
}

#[test]
fn retry_jitter_delay_for_sample_full_boundary_samples() {
    let jitter = RetryJitter::full(42);
    let base = Duration::from_millis(100);

    // Zero sample should give zero duration
    assert_eq!(jitter.delay_for_sample(base, 0), Duration::ZERO);

    // Max sample should give full base duration
    assert_eq!(jitter.delay_for_sample(base, u64::MAX), base);

    // Half sample should give something between 0 and base
    let half = u64::MAX / 2;
    let delay = jitter.delay_for_sample(base, half);
    assert!(delay > Duration::ZERO);
    assert!(delay < base);
}

#[test]
fn retry_jitter_delay_for_sample_full_quarter_sample() {
    let jitter = RetryJitter::full(42);
    let base = Duration::from_millis(1000);
    let quarter = u64::MAX / 4;
    let delay = jitter.delay_for_sample(base, quarter);
    assert!(delay > Duration::ZERO);
    assert!(delay.as_millis() < 250);
}

#[test]
fn retry_jitter_delay_for_sample_full_three_quarter_sample() {
    let jitter = RetryJitter::full(42);
    let base = Duration::from_millis(1000);
    let three_quarter = (u64::MAX / 4) * 3;
    let delay = jitter.delay_for_sample(base, three_quarter);
    assert!(delay > Duration::from_millis(250));
    assert!(delay < base);
}

#[test]
fn retry_jitter_delay_for_sample_full_bounded() {
    let jitter = RetryJitter::full(42);
    let base = Duration::from_secs(30);
    let samples = [0u64, u64::MAX / 4, u64::MAX / 2, (u64::MAX / 4) * 3, u64::MAX];
    for sample in samples {
        let delay = jitter.delay_for_sample(base, sample);
        assert!(delay >= Duration::ZERO);
        assert!(delay <= base);
    }
}

#[test]
fn retry_jitter_delay_for_sample_full_monotonic() {
    let jitter = RetryJitter::full(42);
    let base = Duration::from_secs(10);

    // Large samples should produce non-zero delays
    let samples = [u64::MAX / 2, u64::MAX / 4 * 3, u64::MAX - 1];
    for sample in samples {
        let delay = jitter.delay_for_sample(base, sample);
        assert!(delay > Duration::ZERO, "sample {} should produce non-zero delay", sample);
        assert!(delay < base);
    }
}

#[test]
fn retry_jitter_full_debug() {
    let jitter = RetryJitter::full(42);
    let debug_str = format!("{:?}", jitter);
    assert!(debug_str.contains("Full"));
    assert!(debug_str.contains("42"));
}

#[test]
fn retry_jitter_fixed_debug() {
    let jitter = RetryJitter::fixed(Duration::from_secs(5));
    let debug_str = format!("{:?}", jitter);
    assert!(debug_str.contains("Fixed"));
    assert!(debug_str.contains("5"));
}

#[test]
fn retry_jitter_mixed_operations() {
    let base = Duration::from_millis(500);

    let none_delay = RetryJitter::none().delay_for_sample(base, 100);
    assert_eq!(none_delay, base);

    let fixed_delay = RetryJitter::fixed(Duration::from_millis(200)).delay_for_sample(base, 100);
    assert_eq!(fixed_delay, Duration::from_millis(200));

    let full_zero = RetryJitter::full(42).delay_for_sample(base, 0);
    assert_eq!(full_zero, Duration::ZERO);

    let full_max = RetryJitter::full(42).delay_for_sample(base, u64::MAX);
    assert_eq!(full_max, base);
}

#[test]
fn retry_jitter_large_base() {
    let jitter = RetryJitter::full(42);
    let base = Duration::from_secs(3600); // 1 hour
    let sample = u64::MAX / 2;
    let delay = jitter.delay_for_sample(base, sample);
    assert!(delay > Duration::ZERO);
    assert!(delay < base);
}

#[test]
fn retry_jitter_microsecond_base() {
    let jitter = RetryJitter::full(42);
    let base = Duration::from_micros(1);
    let delay = jitter.delay_for_sample(base, u64::MAX);
    assert_eq!(delay, base);
}
