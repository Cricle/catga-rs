//! Handler dispatch overhead benchmarks
//!
//! Measures the overhead of calling Handler::handle() through:
//! - Direct dispatch (stack reference)
//! - Arc-wrapped dispatch (heap-indirect reference)
//!
//! Run: cargo bench -p catga-core --bench handler_dispatch -- --noplot

use async_trait::async_trait;
use catga_core::{CatgaResult, Handler, Message, Request};
use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use std::sync::Arc;

/// Simple ping message for handler testing
struct Ping(u64);

impl Message for Ping {}

impl Request for Ping {
    type Response = u64;
    type TypeId = catga_core::DefaultMessageTypeId;
}

/// Handler that doubles the input value
struct DoubleHandler;

#[async_trait]
impl Handler<Ping> for DoubleHandler {
    async fn handle(&self, msg: Ping) -> CatgaResult<u64> {
        Ok(msg.0 * 2)
    }
}

/// Benchmark: Direct handler dispatch (no Arc wrapping)
fn handler_direct_dispatch(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let handler = DoubleHandler;

    c.bench_function("handler_direct_dispatch", |b| {
        let runtime = &runtime;
        b.iter(|| {
            let result = runtime.block_on(handler.handle(Ping(21)));
            let _ = black_box(result);
        });
    });
}

/// Benchmark: Arc-wrapped handler dispatch
fn handler_arc_dispatch(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let handler = Arc::new(DoubleHandler);

    c.bench_function("handler_arc_dispatch", |b| {
        let runtime = &runtime;
        b.iter(|| {
            let h = Arc::clone(&handler);
            let result = runtime.block_on(h.handle(Ping(21)));
            let _ = black_box(result);
        });
    });
}

/// Benchmark: Multiple Arc clones (simulates shared handler across components)
fn handler_arc_clone_overhead(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let handler = Arc::new(DoubleHandler);

    c.bench_function("handler_arc_clone_overhead", |b| {
        let runtime = &runtime;
        let handler = &handler;
        b.iter(|| {
            // Clone the Arc (cheap pointer copy)
            let h = Arc::clone(handler);
            // Then dispatch through it
            let result = runtime.block_on(h.handle(Ping(21)));
            let _ = black_box(result);
        });
    });
}

/// Benchmark: Dynamic dispatch through trait object
fn handler_box_dispatch(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let handler: Box<dyn Handler<Ping> + Send + Sync> = Box::new(DoubleHandler);

    c.bench_function("handler_box_dispatch", |b| {
        let runtime = &runtime;
        let handler = &handler;
        b.iter(|| {
            let result = runtime.block_on(handler.handle(Ping(21)));
            let _ = black_box(result);
        });
    });
}

/// Benchmark: Arc<Box<dyn Handler>> combined overhead
fn handler_arc_box_dispatch(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let handler: Arc<Box<dyn Handler<Ping> + Send + Sync>> = Arc::new(Box::new(DoubleHandler));

    c.bench_function("handler_arc_box_dispatch", |b| {
        let runtime = &runtime;
        b.iter(|| {
            let h = Arc::clone(&handler);
            let result = runtime.block_on(h.handle(Ping(21)));
            let _ = black_box(result);
        });
    });
}

criterion_group!(
    benches,
    handler_direct_dispatch,
    handler_arc_dispatch,
    handler_arc_clone_overhead,
    handler_box_dispatch,
    handler_arc_box_dispatch
);
criterion_main!(benches);
