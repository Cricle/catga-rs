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

// Benchmark: RetryJitter clone (None variant)
#[bench]
fn bench_retry_jitter_clone_none(b: &mut test::Bencher) {
    let jitter = RetryJitter::none();
    b.iter(|| {
        test::black_box(jitter);
    });
}

// Benchmark: RetryJitter clone (Full variant)
#[bench]
fn bench_retry_jitter_clone_full(b: &mut test::Bencher) {
    let jitter = RetryJitter::full(12345);
    b.iter(|| {
        test::black_box(jitter);
    });
}

// Benchmark: RetryJitter struct size
#[bench]
fn bench_retry_jitter_sizeof(b: &mut test::Bencher) {
    let jitter = RetryJitter::production_default();
    b.iter(|| {
        test::black_box(std::mem::size_of_val(&jitter));
    });
}
