//! Benchmarks for correlation context (task-local storage)

#![feature(test)]

extern crate test;

use catga_core::{
    EnvelopeHeaders, TransportContext, current_correlation_id, current_correlation_value,
    scope_correlation_id, scope_correlation_value,
};
use std::sync::Arc;

// Benchmark: current_correlation_id access (no context set)
#[bench]
fn bench_current_correlation_id_empty(b: &mut test::Bencher) {
    b.iter(|| {
        test::black_box(current_correlation_id());
    });
}

// Benchmark: current_correlation_id access (with context set)
#[bench]
fn bench_current_correlation_id_with_context(b: &mut test::Bencher) {
    // Set up correlation ID once
    let _ = scope_correlation_id(12345u64, async {
        b.iter(|| {
            test::black_box(current_correlation_id());
        });
    });
}

// Benchmark: current_correlation_value access (no context set)
#[bench]
fn bench_current_correlation_value_empty(b: &mut test::Bencher) {
    b.iter(|| {
        test::black_box(current_correlation_value());
    });
}

// Benchmark: scope_correlation_id overhead
#[bench]
fn bench_scope_correlation_id(b: &mut test::Bencher) {
    b.iter(|| {
        let _ = scope_correlation_id(12345u64, async { 42u32 });
    });
}

// Benchmark: scope_correlation_value overhead
#[bench]
fn bench_scope_correlation_value(b: &mut test::Bencher) {
    let value: Arc<str> = "test-correlation-value".into();
    b.iter(|| {
        let _ = scope_correlation_value(value.clone(), async { 42u32 });
    });
}

// Benchmark: TransportContext creation (from_headers)
#[bench]
fn bench_transport_context_from_headers(b: &mut test::Bencher) {
    let headers = EnvelopeHeaders::try_new([("x-tenant", "acme")]).unwrap();
    b.iter(|| {
        test::black_box(TransportContext::from_headers(headers.clone()));
    });
}

// Benchmark: TransportContext correlation_id access
#[bench]
fn bench_transport_context_correlation_id(b: &mut test::Bencher) {
    let context =
        TransportContext::from_headers(EnvelopeHeaders::try_new([("x-tenant", "acme")]).unwrap());
    b.iter(|| {
        test::black_box(context.correlation_id());
    });
}

// Benchmark: TransportContext priority access
#[bench]
fn bench_transport_context_priority(b: &mut test::Bencher) {
    let context =
        TransportContext::from_headers(EnvelopeHeaders::try_new([("x-tenant", "acme")]).unwrap());
    b.iter(|| {
        test::black_box(context.priority());
    });
}

// Benchmark: TransportContext headers access
#[bench]
fn bench_transport_context_headers(b: &mut test::Bencher) {
    let context =
        TransportContext::from_headers(EnvelopeHeaders::try_new([("x-tenant", "acme")]).unwrap());
    b.iter(|| {
        test::black_box(context.headers());
    });
}

// Benchmark: TransportContext clone
#[bench]
fn bench_transport_context_clone(b: &mut test::Bencher) {
    let context =
        TransportContext::from_headers(EnvelopeHeaders::try_new([("x-tenant", "acme")]).unwrap());
    b.iter(|| {
        test::black_box(context.clone());
    });
}

// Benchmark: TransportContext struct size
#[bench]
fn bench_transport_context_sizeof(b: &mut test::Bencher) {
    let context =
        TransportContext::from_headers(EnvelopeHeaders::try_new([("x-tenant", "acme")]).unwrap());
    b.iter(|| {
        test::black_box(std::mem::size_of_val(&context));
    });
}
