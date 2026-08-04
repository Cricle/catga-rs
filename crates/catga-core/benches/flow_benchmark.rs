//! Benchmarks for Flow creation and execution

#![feature(test)]

extern crate test;

use catga_core::flow::DslFlow;
use catga_core::flow::local::Flow;

// Benchmark: Flow creation (empty)
#[bench]
fn bench_flow_new(b: &mut test::Bencher) {
    b.iter(|| {
        let _flow = Flow::new("bench-flow");
    });
}

// Benchmark: Flow with 1 step
#[bench]
fn bench_flow_1_step(b: &mut test::Bencher) {
    b.iter(|| {
        let _flow = Flow::new("bench-flow").step(|| async { Ok(()) }, || async { Ok(()) });
    });
}

// Benchmark: Flow with 3 steps
#[bench]
fn bench_flow_3_steps(b: &mut test::Bencher) {
    b.iter(|| {
        let _flow = Flow::new("bench-flow")
            .step(|| async { Ok(()) }, || async { Ok(()) })
            .step(|| async { Ok(()) }, || async { Ok(()) })
            .step(|| async { Ok(()) }, || async { Ok(()) });
    });
}

// Benchmark: Flow with 10 steps
#[bench]
fn bench_flow_10_steps(b: &mut test::Bencher) {
    b.iter(|| {
        let mut flow = Flow::new("bench-flow");
        for _ in 0..10 {
            flow = flow.step(|| async { Ok(()) }, || async { Ok(()) });
        }
        flow
    });
}

// Benchmark: DslFlow creation (empty)
#[bench]
fn bench_dsl_flow_new(b: &mut test::Bencher) {
    b.iter(|| {
        let _flow: DslFlow<u32> = DslFlow::new();
    });
}

// Benchmark: DslFlow with 1 action (using action method directly)
#[bench]
fn bench_dsl_flow_1_action(b: &mut test::Bencher) {
    b.iter(|| {
        let _flow: DslFlow<u32> = DslFlow::new().action(|state: &mut u32| {
            Box::pin(async move {
                *state += 1;
                Ok(())
            })
        });
    });
}

// Benchmark: DslFlow with 3 actions
#[bench]
fn bench_dsl_flow_3_actions(b: &mut test::Bencher) {
    b.iter(|| {
        let _flow: DslFlow<u32> = DslFlow::new()
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
    });
}

// Benchmark: DslFlow with 10 actions
#[bench]
fn bench_dsl_flow_10_actions(b: &mut test::Bencher) {
    b.iter(|| {
        let mut flow: DslFlow<u32> = DslFlow::new();
        for _ in 0..10 {
            flow = flow.action(|state: &mut u32| {
                Box::pin(async move {
                    *state += 1;
                    Ok(())
                })
            });
        }
        flow
    });
}

// Benchmark: Flow struct size
#[bench]
fn bench_flow_sizeof(b: &mut test::Bencher) {
    let flow = Flow::new("bench-flow");
    b.iter(|| {
        test::black_box(std::mem::size_of_val(&flow));
    });
}

// Benchmark: DslFlow struct size
#[bench]
fn bench_dsl_flow_sizeof(b: &mut test::Bencher) {
    let flow: DslFlow<u32> = DslFlow::new();
    b.iter(|| {
        test::black_box(std::mem::size_of_val(&flow));
    });
}

// Benchmark: Flow struct size with 10 steps
#[bench]
fn bench_flow_sizeof_10_steps(b: &mut test::Bencher) {
    let mut flow = Flow::new("bench-flow");
    for _ in 0..10 {
        flow = flow.step(|| async { Ok(()) }, || async { Ok(()) });
    }
    b.iter(|| {
        test::black_box(std::mem::size_of_val(&flow));
    });
}

// Benchmark: DslFlow struct size with 10 actions
#[bench]
fn bench_dsl_flow_sizeof_10_actions(b: &mut test::Bencher) {
    let mut flow: DslFlow<u32> = DslFlow::new();
    for _ in 0..10 {
        flow = flow.action(|state: &mut u32| {
            Box::pin(async move {
                *state += 1;
                Ok(())
            })
        });
    }
    b.iter(|| {
        test::black_box(std::mem::size_of_val(&flow));
    });
}
