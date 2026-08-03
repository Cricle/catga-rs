//! Pure mediator dispatch throughput — no yield, no batch scheduler overhead.
//!
//! Run: `cargo test --release -p catga-tests --test mediator_pure_throughput -- --ignored --nocapture`

use std::time::Instant;

use async_trait::async_trait;
use catga_core::{CatgaResult, Event, EventHandler, Handler, Mediator, Message, Registry, Request};

#[path = "support/performance_report.rs"]
mod performance_report;

const MESSAGE_COUNT: usize = 100_000;
const EVENT_COUNT: usize = 100_000;

struct Ping(u64);
impl Message for Ping {}
impl Request for Ping {
    type Response = u64;
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct PingHandler;

#[async_trait]
impl Handler<Ping> for PingHandler {
    async fn handle(&self, message: Ping) -> CatgaResult<u64> {
        Ok(message.0)
    }
}

#[derive(Clone)]
struct Tick(u64);
impl Message for Tick {}
impl Event for Tick {
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct TickHandler;

#[async_trait]
impl EventHandler<Tick> for TickHandler {
    async fn handle(&self, message: Tick) -> CatgaResult<()> {
        let _ = message.0;
        Ok(())
    }
}

fn write_workload_report(
    name: &'static str,
    operations: usize,
    elapsed: std::time::Duration,
    rss_before_bytes: Option<u64>,
) -> CatgaResult<()> {
    let mut result = performance_report::measured(
        name,
        None,
        elapsed,
        vec![elapsed],
        "whole workload",
        rss_before_bytes,
    );
    result.operations = operations as u64;
    result.operations_per_second = operations as f64 / elapsed.as_secs_f64();
    let report = performance_report::PerformanceReport {
        schema_version: 1,
        source: "in-process pure mediator",
        environment: performance_report::environment(),
        results: vec![result],
        database_metric_deltas: Vec::new(),
    };
    performance_report::write_report_if_configured(&report)
        .map_err(|error| catga_core::CatgaError::new(catga_core::ErrorCode::Internal, error))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual performance benchmark"]
async fn mediator_pure_sequential_send() -> CatgaResult<()> {
    let mut registry = Registry::new();
    registry.register_request::<Ping, _>(PingHandler)?;
    let mediator = Mediator::new(registry);

    // Warmup
    for i in 0..1000 {
        mediator.send(Ping(i)).await?;
    }

    let rss_before_bytes = performance_report::current_rss_bytes();
    let started = Instant::now();
    for i in 0..MESSAGE_COUNT {
        let result = mediator.send(Ping(i as u64)).await?;
        assert_eq!(result, i as u64);
    }
    let elapsed = started.elapsed();
    let ops_per_sec = MESSAGE_COUNT as f64 / elapsed.as_secs_f64();
    let p50_approx_ns = elapsed.as_nanos() / MESSAGE_COUNT as u128;

    println!("=== Mediator Pure Sequential Send ===");
    println!("  messages:    {MESSAGE_COUNT}");
    println!("  total:       {elapsed:?}");
    println!("  throughput:  {:.0} msg/s", ops_per_sec);
    println!("  avg latency: {p50_approx_ns} ns");
    println!();

    write_workload_report(
        "mediator_pure_sequential_send",
        MESSAGE_COUNT,
        elapsed,
        rss_before_bytes,
    )?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual performance benchmark"]
async fn mediator_pure_concurrent_send() -> CatgaResult<()> {
    let mut registry = Registry::new();
    registry.register_request::<Ping, _>(PingHandler)?;
    let mediator = std::sync::Arc::new(Mediator::new(registry));

    // Warmup
    for i in 0..1000 {
        mediator.send(Ping(i)).await?;
    }

    let rss_before_bytes = performance_report::current_rss_bytes();
    let started = Instant::now();
    let mut handles = Vec::with_capacity(16);
    let per_task = MESSAGE_COUNT / 16;
    for task_id in 0..16u64 {
        let mediator = std::sync::Arc::clone(&mediator);
        handles.push(tokio::spawn(async move {
            let base = task_id * per_task as u64;
            for i in 0..per_task {
                let _ = mediator.send(Ping(base + i as u64)).await;
            }
        }));
    }
    for handle in handles {
        handle.await.expect("task panicked");
    }
    let elapsed = started.elapsed();
    let ops_per_sec = MESSAGE_COUNT as f64 / elapsed.as_secs_f64();

    println!("=== Mediator Pure Concurrent Send (16 tasks) ===");
    println!("  messages:    {MESSAGE_COUNT}");
    println!("  total:       {elapsed:?}");
    println!("  throughput:  {:.0} msg/s", ops_per_sec);
    println!(
        "  avg latency: {} ns",
        elapsed.as_nanos() / MESSAGE_COUNT as u128
    );
    println!();

    write_workload_report(
        "mediator_pure_concurrent_send_16",
        MESSAGE_COUNT,
        elapsed,
        rss_before_bytes,
    )?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual performance benchmark"]
async fn mediator_batch_no_yield() -> CatgaResult<()> {
    let mut registry = Registry::new();
    registry.register_request::<Ping, _>(PingHandler)?;
    let mediator = Mediator::new(registry);

    // Warmup
    let _: Vec<_> = mediator.send_batch((0..100).map(Ping), 64).await?;

    let rss_before_bytes = performance_report::current_rss_bytes();
    let started = Instant::now();
    let mut total = 0usize;
    for batch_start in (0..MESSAGE_COUNT).step_by(1024) {
        let batch_end = (batch_start + 1024).min(MESSAGE_COUNT);
        let responses = mediator
            .send_batch((batch_start..batch_end).map(|i| Ping(i as u64)), 64)
            .await?;
        total += responses.len();
    }
    let elapsed = started.elapsed();
    assert_eq!(total, MESSAGE_COUNT);
    let ops_per_sec = MESSAGE_COUNT as f64 / elapsed.as_secs_f64();

    println!("=== Mediator Batch Send (1024 batch, 64 concurrency, no yield) ===");
    println!("  messages:    {MESSAGE_COUNT}");
    println!("  total:       {elapsed:?}");
    println!("  throughput:  {:.0} msg/s", ops_per_sec);
    println!(
        "  avg latency: {} ns",
        elapsed.as_nanos() / MESSAGE_COUNT as u128
    );
    println!();

    write_workload_report(
        "mediator_pure_batch_send",
        MESSAGE_COUNT,
        elapsed,
        rss_before_bytes,
    )?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual performance benchmark"]
async fn event_publish_sequential() -> CatgaResult<()> {
    let mut registry = Registry::new();
    registry.register_event::<Tick, _>(TickHandler);
    let mediator = Mediator::new(registry);

    for i in 0..1000 {
        mediator.publish(Tick(i)).await?;
    }

    let rss_before_bytes = performance_report::current_rss_bytes();
    let started = Instant::now();
    for i in 0..EVENT_COUNT {
        mediator.publish(Tick(i as u64)).await?;
    }
    let elapsed = started.elapsed();
    let ops_per_sec = EVENT_COUNT as f64 / elapsed.as_secs_f64();

    println!("=== Event Publish Sequential (1 handler) ===");
    println!("  events:      {EVENT_COUNT}");
    println!("  total:       {elapsed:?}");
    println!("  throughput:  {:.0} events/s", ops_per_sec);
    println!(
        "  avg latency: {} ns",
        elapsed.as_nanos() / EVENT_COUNT as u128
    );
    println!();

    write_workload_report(
        "mediator_pure_event_publish",
        EVENT_COUNT,
        elapsed,
        rss_before_bytes,
    )?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual performance benchmark"]
async fn event_publish_fan_out_3() -> CatgaResult<()> {
    let mut registry = Registry::new();
    registry.register_event::<Tick, _>(TickHandler);
    registry.register_event::<Tick, _>(TickHandler);
    registry.register_event::<Tick, _>(TickHandler);
    let mediator = Mediator::new(registry);

    for i in 0..1000 {
        mediator.publish(Tick(i)).await?;
    }

    let rss_before_bytes = performance_report::current_rss_bytes();
    let started = Instant::now();
    for i in 0..EVENT_COUNT {
        mediator.publish(Tick(i as u64)).await?;
    }
    let elapsed = started.elapsed();
    let ops_per_sec = EVENT_COUNT as f64 / elapsed.as_secs_f64();

    println!("=== Event Publish Sequential (3 handlers fan-out) ===");
    println!("  events:      {EVENT_COUNT}");
    println!("  total:       {elapsed:?}");
    println!("  throughput:  {:.0} events/s", ops_per_sec);
    println!(
        "  avg latency: {} ns",
        elapsed.as_nanos() / EVENT_COUNT as u128
    );
    println!();

    write_workload_report(
        "mediator_pure_event_fan_out_3",
        EVENT_COUNT,
        elapsed,
        rss_before_bytes,
    )?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual performance benchmark"]
async fn event_publish_concurrent_16_tasks() -> CatgaResult<()> {
    let mut registry = Registry::new();
    registry.register_event::<Tick, _>(TickHandler);
    let mediator = std::sync::Arc::new(Mediator::new(registry));

    for i in 0..1000 {
        mediator.publish(Tick(i)).await?;
    }

    let rss_before_bytes = performance_report::current_rss_bytes();
    let started = Instant::now();
    let mut handles = Vec::with_capacity(16);
    let per_task = EVENT_COUNT / 16;
    for task_id in 0..16u64 {
        let mediator = std::sync::Arc::clone(&mediator);
        handles.push(tokio::spawn(async move {
            let base = task_id * per_task as u64;
            for i in 0..per_task {
                let _ = mediator.publish(Tick(base + i as u64)).await;
            }
        }));
    }
    for handle in handles {
        handle.await.expect("task panicked");
    }
    let elapsed = started.elapsed();
    let ops_per_sec = EVENT_COUNT as f64 / elapsed.as_secs_f64();

    println!("=== Event Publish Concurrent (16 tasks, 1 handler) ===");
    println!("  events:      {EVENT_COUNT}");
    println!("  total:       {elapsed:?}");
    println!("  throughput:  {:.0} events/s", ops_per_sec);
    println!(
        "  avg latency: {} ns",
        elapsed.as_nanos() / EVENT_COUNT as u128
    );
    println!();

    write_workload_report(
        "mediator_pure_event_concurrent_16",
        EVENT_COUNT,
        elapsed,
        rss_before_bytes,
    )?;
    Ok(())
}
