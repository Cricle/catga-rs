//! Mediator throughput benchmarks using criterion.
//!
//! Run: cargo bench -p catga-core --bench mediator_throughput
//!
//! Target: >10M ops/s for Mediator operations (single-threaded)

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use catga_core::{CatgaResult, Mediator, Message, Registry, Request, request_handler};
use std::hint::black_box;

/// Simple ping message for throughput testing
struct Ping(u64);

impl Message for Ping {}

impl Request for Ping {
    type Response = u64;
    type TypeId = catga_core::DefaultMessageTypeId;
}

/// Creates a mediator with a simple identity handler
fn create_mediator() -> CatgaResult<Mediator> {
    let mut registry = Registry::new();
    registry.register_request::<Ping, _>(request_handler(|msg: Ping| async move { Ok(msg.0) }))?;
    Ok(Mediator::new(registry))
}

/// Benchmark mediator throughput with increasing batch sizes
fn mediator_throughput(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mediator = create_mediator().unwrap();

    let mut group = c.benchmark_group("mediator_throughput");

    // Test different batch sizes to measure throughput scaling
    for count in [100_000, 1_000_000, 5_000_000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &count| {
            let mediator = &mediator;
            let runtime = &runtime;
            b.iter(|| {
                let mut sum = 0u64;
                for _ in 0..count {
                    let result = runtime.block_on(mediator.send(Ping(1)));
                    if let Ok(value) = result {
                        sum = sum.wrapping_add(value);
                    }
                }
                black_box(sum);
            });
        });
    }

    group.finish();
}

/// Benchmark single request latency (average over many iterations)
fn mediator_single_request(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mediator = create_mediator().unwrap();

    let mut group = c.benchmark_group("mediator_single_request");

    group.bench_function("send", |b| {
        let mediator = &mediator;
        let runtime = &runtime;
        b.iter(|| {
            let result = runtime.block_on(mediator.send(Ping(42)));
            let _ = black_box(result);
        });
    });

    group.finish();
}

/// Benchmark batch send throughput
fn mediator_batch_send(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mediator = create_mediator().unwrap();

    let mut group = c.benchmark_group("mediator_batch_send");

    // Test batch send throughput
    for batch_size in [100, 1_000, 10_000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(batch_size), batch_size, |b, &batch_size| {
            let mediator = &mediator;
            let runtime = &runtime;
            b.iter(|| {
                let results = runtime.block_on(
                    mediator.send_batch(
                        std::iter::repeat_with(|| Ping(1)).take(batch_size),
                        batch_size,
                    )
                );
                let _ = black_box(results);
            });
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(std::time::Duration::from_secs(5));
    targets = mediator_throughput, mediator_single_request, mediator_batch_send
}
criterion_main!(benches);
