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

// Benchmark: AcceptanceGate struct size
#[bench]
fn bench_acceptance_gate_sizeof(b: &mut test::Bencher) {
    let gate = AcceptanceGate::default();
    b.iter(|| {
        test::black_box(std::mem::size_of_val(&gate));
    });
}

// Benchmark: AcceptanceGate multiple clones share state
#[bench]
fn bench_acceptance_gate_shared_state(b: &mut test::Bencher) {
    b.iter(|| {
        let gate = AcceptanceGate::default();
        let _g1 = gate.clone();
        let _g2 = gate.clone();
        let _g3 = gate.clone();
        gate.stop_accepting();
    });
}
