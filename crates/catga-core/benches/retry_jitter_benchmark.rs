//! Benchmarks for retry jitter policies

#![feature(test)]

extern crate test;

use catga_core::RetryJitter;
use std::time::Duration;

// Benchmark: RetryJitter::none() creation
#[bench]
fn bench_retry_jitter_none(b: &mut test::Bencher) {
    b.iter(|| {
        let jitter = RetryJitter::none();
        test::black_box(&jitter);
    });
}

// Benchmark: RetryJitter::production_default() creation
#[bench]
fn bench_retry_jitter_production_default(b: &mut test::Bencher) {
    b.iter(|| {
        let jitter = RetryJitter::production_default();
        test::black_box(&jitter);
    });
}

// Benchmark: RetryJitter::full() creation
#[bench]
fn bench_retry_jitter_full(b: &mut test::Bencher) {
    b.iter(|| {
        let jitter = RetryJitter::full(12345);
        test::black_box(&jitter);
    });
}

// Benchmark: RetryJitter::fixed() creation
#[bench]
fn bench_retry_jitter_fixed(b: &mut test::Bencher) {
    b.iter(|| {
        let jitter = RetryJitter::fixed(Duration::from_millis(100));
        test::black_box(&jitter);
    });
}

// Benchmark: RetryJitter::delay_for_sample (None variant)
#[bench]
fn bench_retry_jitter_delay_none(b: &mut test::Bencher) {
    let jitter = RetryJitter::none();
    let base = Duration::from_millis(100);
    b.iter(|| {
        test::black_box(jitter.delay_for_sample(base, 5000));
    });
}

// Benchmark: RetryJitter::delay_for_sample (Fixed variant)
#[bench]
fn bench_retry_jitter_delay_fixed(b: &mut test::Bencher) {
    let jitter = RetryJitter::fixed(Duration::from_millis(100));
    let base = Duration::from_millis(100);
    b.iter(|| {
        test::black_box(jitter.delay_for_sample(base, 5000));
    });
}

// Benchmark: RetryJitter::delay_for_sample (Full jitter variant)
#[bench]
fn bench_retry_jitter_delay_full(b: &mut test::Bencher) {
    let jitter = RetryJitter::full(12345);
    let base = Duration::from_millis(100);
    b.iter(|| {
        test::black_box(jitter.delay_for_sample(base, 5000));
    });
}

// Benchmark: RetryJitter clone (for passing to tasks)
#[bench]
fn bench_retry_jitter_clone(b: &mut test::Bencher) {
    let jitter = RetryJitter::production_default();
    b.iter(|| {
        test::black_box(jitter);
    });
}

// Benchmark: Multiple jitter calculations in sequence
#[bench]
fn bench_retry_jitter_multiple_calculations(b: &mut test::Bencher) {
    let jitter = RetryJitter::full(12345);
    let base = Duration::from_millis(100);
    b.iter(|| {
        let _d1 = jitter.delay_for_sample(base, 5000);
        let _d2 = jitter.delay_for_sample(base, 5001);
        let _d3 = jitter.delay_for_sample(base, 5002);
    });
}
