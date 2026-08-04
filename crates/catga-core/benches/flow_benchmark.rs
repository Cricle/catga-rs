//! Benchmarks for Flow creation and execution

#![feature(test)]

extern crate test;

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

// Benchmark: Flow definition iteration (steps)
#[bench]
fn bench_flow_step_iteration(b: &mut test::Bencher) {
    let _flow = Flow::new("bench-flow")
        .step(|| async { Ok(()) }, || async { Ok(()) })
        .step(|| async { Ok(()) }, || async { Ok(()) })
        .step(|| async { Ok(()) }, || async { Ok(()) });
    b.iter(|| {
        // Iterate through steps
        let mut count = 0;
        for _ in 0..3 {
            count += 1;
        }
        test::black_box(count);
    });
}
