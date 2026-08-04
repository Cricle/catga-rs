//! Benchmarks for lifecycle components

#![feature(test)]

extern crate test;

use catga_core::AcceptanceGate;

// Benchmark: AcceptanceGate::default() creation
#[bench]
fn bench_acceptance_gate_default(b: &mut test::Bencher) {
    b.iter(|| {
        let gate = AcceptanceGate::default();
        test::black_box(&gate);
    });
}

// Benchmark: AcceptanceGate::is_accepting() (true state)
#[bench]
fn bench_acceptance_gate_is_accepting_true(b: &mut test::Bencher) {
    let gate = AcceptanceGate::default();
    b.iter(|| {
        test::black_box(gate.is_accepting());
    });
}

// Benchmark: AcceptanceGate::is_accepting() (false state)
#[bench]
fn bench_acceptance_gate_is_accepting_false(b: &mut test::Bencher) {
    let gate = AcceptanceGate::default();
    gate.stop_accepting();
    b.iter(|| {
        test::black_box(gate.is_accepting());
    });
}

// Benchmark: AcceptanceGate::stop_accepting()
#[bench]
fn bench_acceptance_gate_stop_accepting(b: &mut test::Bencher) {
    b.iter(|| {
        let gate = AcceptanceGate::default();
        gate.stop_accepting();
    });
}

// Benchmark: AcceptanceGate clone
#[bench]
fn bench_acceptance_gate_clone(b: &mut test::Bencher) {
    let gate = AcceptanceGate::default();
    b.iter(|| {
        test::black_box(gate.clone());
    });
}

// Benchmark: AcceptanceGate multiple clones (shared state test)
#[bench]
fn bench_acceptance_gate_multiple_clones(b: &mut test::Bencher) {
    b.iter(|| {
        let gate = AcceptanceGate::default();
        let _g1 = gate.clone();
        let _g2 = gate.clone();
        let _g3 = gate.clone();
        gate.stop_accepting();
    });
}

// Benchmark: AcceptanceGate check after stop
#[bench]
fn bench_acceptance_gate_check_after_stop(b: &mut test::Bencher) {
    b.iter(|| {
        let gate = AcceptanceGate::default();
        gate.stop_accepting();
        test::black_box(gate.is_accepting());
    });
}
