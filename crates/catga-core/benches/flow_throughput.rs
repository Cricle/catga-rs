//! Flow execution throughput benchmarks
//!
//! Measures performance of Flow and DslFlow execution patterns.
//!
//! Run: cargo bench -p catga-core --bench flow_throughput

use catga_core::flow::{DslFlow, Flow};
use catga_core::{CatgaError, ErrorCode};
use criterion::{Criterion, criterion_group, criterion_main};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// Benchmark single step flow execution
fn single_step_flow_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark should not fail");

    c.bench_function("single_step_flow_execution", |b| {
        b.iter(|| {
            let flow = Flow::new("bench").step(|| async { Ok(()) }, || async { Ok(()) });
            let result = rt.block_on(flow.run());
            assert!(result.is_success());
        });
    });
}

/// Benchmark three-step flow execution
fn multi_step_flow_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark should not fail");

    c.bench_function("three_step_flow_execution", |b| {
        b.iter(|| {
            let flow = Flow::new("bench")
                .step(|| async { Ok(()) }, || async { Ok(()) })
                .step(|| async { Ok(()) }, || async { Ok(()) })
                .step(|| async { Ok(()) }, || async { Ok(()) });
            let result = rt.block_on(flow.run());
            assert!(result.is_success());
        });
    });
}

/// Benchmark five-step flow execution
fn multi_step_flow_5_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark should not fail");

    c.bench_function("five_step_flow_execution", |b| {
        b.iter(|| {
            let flow = Flow::new("bench")
                .step(|| async { Ok(()) }, || async { Ok(()) })
                .step(|| async { Ok(()) }, || async { Ok(()) })
                .step(|| async { Ok(()) }, || async { Ok(()) })
                .step(|| async { Ok(()) }, || async { Ok(()) })
                .step(|| async { Ok(()) }, || async { Ok(()) });
            let result = rt.block_on(flow.run());
            assert!(result.is_success());
        });
    });
}

/// Benchmark DSL flow with two actions
fn dsl_flow_two_actions_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark should not fail");

    c.bench_function("dsl_flow_two_actions", |b| {
        b.iter(|| {
            let flow = DslFlow::<u32>::new()
                .action(|state: &mut u32| {
                    Box::pin(async move {
                        *state += 1;
                        Ok(())
                    })
                })
                .action(|state: &mut u32| {
                    Box::pin(async move {
                        *state += 1;
                        Ok(())
                    })
                });
            let mut state = 0u32;
            rt.block_on(flow.run(&mut state))
                .expect("benchmark should not fail");
        });
    });
}

/// Benchmark DSL flow with five actions
fn dsl_flow_five_actions_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark should not fail");

    c.bench_function("dsl_flow_five_actions", |b| {
        b.iter(|| {
            let flow = DslFlow::<u32>::new()
                .action(|state: &mut u32| {
                    Box::pin(async move {
                        *state += 1;
                        Ok(())
                    })
                })
                .action(|state: &mut u32| {
                    Box::pin(async move {
                        *state += 1;
                        Ok(())
                    })
                })
                .action(|state: &mut u32| {
                    Box::pin(async move {
                        *state += 1;
                        Ok(())
                    })
                })
                .action(|state: &mut u32| {
                    Box::pin(async move {
                        *state += 1;
                        Ok(())
                    })
                })
                .action(|state: &mut u32| {
                    Box::pin(async move {
                        *state += 1;
                        Ok(())
                    })
                });
            let mut state = 0u32;
            rt.block_on(flow.run(&mut state))
                .expect("benchmark should not fail");
        });
    });
}

/// Benchmark compensation execution on failure
fn flow_compensation_on_failure_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark should not fail");

    c.bench_function("flow_compensation_on_failure", |b| {
        b.iter(|| {
            // Create a fresh compensation flag and flow for each iteration
            let compensation_run = Arc::new(AtomicBool::new(false));
            let comp = compensation_run.clone();

            // The compensate closure captures comp by cloning
            // First step succeeds, second step fails, triggering compensation of first step
            let flow = Flow::new("compensation-bench")
                .step(
                    || async { Ok(()) },
                    move || {
                        let comp_clone = comp.clone();
                        async move {
                            comp_clone.store(true, Ordering::SeqCst);
                            Ok(())
                        }
                    },
                )
                .step(
                    || async { Err(CatgaError::new(ErrorCode::Internal, "fail")) },
                    || async { Ok(()) },
                );

            let result = rt.block_on(flow.run());
            // Result should indicate failure (error is present)
            assert!(result.error().is_some());
            assert!(compensation_run.load(Ordering::SeqCst));
        });
    });
}

criterion_group!(
    benches,
    single_step_flow_throughput,
    multi_step_flow_throughput,
    multi_step_flow_5_throughput,
    dsl_flow_two_actions_throughput,
    dsl_flow_five_actions_throughput,
    flow_compensation_on_failure_throughput
);
criterion_main!(benches);
