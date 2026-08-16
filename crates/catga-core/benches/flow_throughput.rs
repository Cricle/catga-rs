//! Flow execution throughput benchmarks
//!
//! Measures performance of DslFlow execution patterns.
//!
//! Run: cargo bench -p catga-core --bench flow_throughput

use catga_core::flow::DslFlow;
use catga_core::{CatgaError, ErrorCode};
use criterion::{Criterion, criterion_group, criterion_main};

type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

// =============================================================================
// DslFlow benchmarks (action only)
// =============================================================================

fn dsl_flow_single_action_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("dsl_flow_single_action", |b| {
        b.iter(|| {
            let mut state = 0u32;
            let flow =
                DslFlow::<u32>::new().action(|_s| Box::pin(async { Ok(()) }) as BoxFuture<'_, _>);
            let result = rt.block_on(flow.run(&mut state));
            assert!(result.is_ok());
        });
    });
}

fn dsl_flow_two_actions_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("dsl_flow_two_actions", |b| {
        b.iter(|| {
            let mut state = 0u32;
            let flow = DslFlow::<u32>::new()
                .action(|_s| Box::pin(async { Ok(()) }) as BoxFuture<'_, _>)
                .action(|_s| Box::pin(async { Ok(()) }) as BoxFuture<'_, _>);
            let result = rt.block_on(flow.run(&mut state));
            assert!(result.is_ok());
        });
    });
}

fn dsl_flow_five_actions_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("dsl_flow_five_actions", |b| {
        b.iter(|| {
            let mut state = 0u32;
            let flow = DslFlow::<u32>::new()
                .action(|_s| Box::pin(async { Ok(()) }) as BoxFuture<'_, _>)
                .action(|_s| Box::pin(async { Ok(()) }) as BoxFuture<'_, _>)
                .action(|_s| Box::pin(async { Ok(()) }) as BoxFuture<'_, _>)
                .action(|_s| Box::pin(async { Ok(()) }) as BoxFuture<'_, _>)
                .action(|_s| Box::pin(async { Ok(()) }) as BoxFuture<'_, _>);
            let result = rt.block_on(flow.run(&mut state));
            assert!(result.is_ok());
        });
    });
}

// =============================================================================
// DslFlow with compensation benchmarks
// =============================================================================

fn dsl_flow_compensate_single_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("dsl_flow_compensate_single", |b| {
        b.iter(|| {
            let mut state = 0u32;
            let mut flow =
                DslFlow::<u32>::new().compensate(|_s| async { Ok(()) }, |_s| async { Ok(()) });
            let result = rt.block_on(flow.run_compensatable(&mut state));
            assert!(result.is_ok());
        });
    });
}

fn dsl_flow_compensate_three_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("dsl_flow_compensate_three", |b| {
        b.iter(|| {
            let mut state = 0u32;
            let mut flow = DslFlow::<u32>::new()
                .compensate(|_s| async { Ok(()) }, |_s| async { Ok(()) })
                .compensate(|_s| async { Ok(()) }, |_s| async { Ok(()) })
                .compensate(|_s| async { Ok(()) }, |_s| async { Ok(()) });
            let result = rt.block_on(flow.run_compensatable(&mut state));
            assert!(result.is_ok());
        });
    });
}

fn dsl_flow_compensate_five_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("dsl_flow_compensate_five", |b| {
        b.iter(|| {
            let mut state = 0u32;
            let mut flow = DslFlow::<u32>::new()
                .compensate(|_s| async { Ok(()) }, |_s| async { Ok(()) })
                .compensate(|_s| async { Ok(()) }, |_s| async { Ok(()) })
                .compensate(|_s| async { Ok(()) }, |_s| async { Ok(()) })
                .compensate(|_s| async { Ok(()) }, |_s| async { Ok(()) })
                .compensate(|_s| async { Ok(()) }, |_s| async { Ok(()) });
            let result = rt.block_on(flow.run_compensatable(&mut state));
            assert!(result.is_ok());
        });
    });
}

// =============================================================================
// Failure handling benchmark
// =============================================================================

fn dsl_flow_failure_handling_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("dsl_flow_failure_handling", |b| {
        b.iter(|| {
            let mut state = 0u32;
            let flow = DslFlow::<u32>::new()
                .action(|_s| Box::pin(async { Ok(()) }) as BoxFuture<'_, _>)
                .action(|_s| {
                    Box::pin(async { Err(CatgaError::new(ErrorCode::Internal, "fail")) })
                        as BoxFuture<'_, _>
                });
            let result = rt.block_on(flow.run(&mut state));
            assert!(result.is_err());
        });
    });
}

criterion_group!(
    benches,
    // Action only benchmarks
    dsl_flow_single_action_throughput,
    dsl_flow_two_actions_throughput,
    dsl_flow_five_actions_throughput,
    // Compensation benchmarks
    dsl_flow_compensate_single_throughput,
    dsl_flow_compensate_three_throughput,
    dsl_flow_compensate_five_throughput,
    // Failure handling
    dsl_flow_failure_handling_throughput,
);
criterion_main!(benches);
