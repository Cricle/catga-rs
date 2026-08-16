//! Typed mediator zero-allocation dispatch benchmark and correctness tests.
//!
//! Run: `cargo test --release -p catga-tests --test typed_mediator_bench -- --ignored --nocapture`

use std::time::Instant;

use async_trait::async_trait;
use catga_core::{
    CatgaResult, Command, CommandHandler, Event, EventHandler, Handler, Message, Request,
    catga_typed_mediator,
};

#[path = "support/performance_report.rs"]
mod performance_report;

const MESSAGE_COUNT: usize = 100_000;

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

struct Ping(u64);
impl Message for Ping {}
impl Request for Ping {
    type Response = u64;
}

#[derive(Clone)]
struct Tick(u64);
impl Message for Tick {}
impl Event for Tick {}

#[derive(Clone)]
struct DoWork;
impl Message for DoWork {}
impl Command for DoWork {}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

struct PingHandler;

#[async_trait]
impl Handler<Ping> for PingHandler {
    async fn handle(&self, message: Ping) -> CatgaResult<u64> {
        Ok(message.0)
    }
}

#[derive(Clone)]
struct TickHandler;

#[async_trait]
impl EventHandler<Tick> for TickHandler {
    async fn handle(&self, message: Tick) -> CatgaResult<()> {
        let _ = message.0;
        Ok(())
    }
}

struct WorkHandler;

#[async_trait]
impl CommandHandler<DoWork> for WorkHandler {
    async fn handle(&self, _: DoWork) -> CatgaResult<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Typed mediator generation
// ---------------------------------------------------------------------------

catga_typed_mediator! {
    pub struct BenchMediator;
    request Ping => PingHandler;
    command DoWork => WorkHandler;
    event Tick => [TickHandler];
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
        source: "in-process typed mediator",
        environment: performance_report::environment(),
        results: vec![result],
        database_metric_deltas: Vec::new(),
    };
    performance_report::write_report_if_configured(&report)
        .map_err(|error| catga_core::CatgaError::new(catga_core::ErrorCode::Internal, error))
}

// ---------------------------------------------------------------------------
// Correctness tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn typed_mediator_request_dispatch() -> CatgaResult<()> {
    let mediator = BenchMediator::new(PingHandler, WorkHandler, [TickHandler]);
    assert_eq!(mediator.send(Ping(42)).await?, 42);
    assert_eq!(mediator.send(Ping(0)).await?, 0);
    Ok(())
}

#[tokio::test]
async fn typed_mediator_command_dispatch() -> CatgaResult<()> {
    let mediator = BenchMediator::new(PingHandler, WorkHandler, [TickHandler]);
    mediator.send_command(DoWork).await?;
    Ok(())
}

#[tokio::test]
async fn typed_mediator_event_dispatch() -> CatgaResult<()> {
    let mediator = BenchMediator::new(PingHandler, WorkHandler, [TickHandler]);
    mediator.publish(Tick(1)).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual performance benchmark"]
async fn typed_mediator_sequential_send() -> CatgaResult<()> {
    let mediator = BenchMediator::new(PingHandler, WorkHandler, [TickHandler]);

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

    println!("=== Typed Mediator Sequential Send ===");
    println!("  messages:    {MESSAGE_COUNT}");
    println!("  total:       {elapsed:?}");
    println!("  throughput:  {:.0} msg/s", ops_per_sec);
    println!(
        "  avg latency: {} ns",
        elapsed.as_nanos() / MESSAGE_COUNT as u128
    );
    println!();

    write_workload_report(
        "typed_mediator_sequential_send",
        MESSAGE_COUNT,
        elapsed,
        rss_before_bytes,
    )?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual performance benchmark"]
async fn typed_mediator_concurrent_send() -> CatgaResult<()> {
    let mediator = std::sync::Arc::new(BenchMediator::new(PingHandler, WorkHandler, [TickHandler]));

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

    println!("=== Typed Mediator Concurrent Send (16 tasks) ===");
    println!("  messages:    {MESSAGE_COUNT}");
    println!("  total:       {elapsed:?}");
    println!("  throughput:  {:.0} msg/s", ops_per_sec);
    println!(
        "  avg latency: {} ns",
        elapsed.as_nanos() / MESSAGE_COUNT as u128
    );
    println!();

    write_workload_report(
        "typed_mediator_concurrent_send_16",
        MESSAGE_COUNT,
        elapsed,
        rss_before_bytes,
    )?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual performance benchmark"]
async fn typed_mediator_event_publish() -> CatgaResult<()> {
    let mediator = BenchMediator::new(PingHandler, WorkHandler, [TickHandler]);

    for i in 0..1000 {
        mediator.publish(Tick(i)).await?;
    }

    let rss_before_bytes = performance_report::current_rss_bytes();
    let started = Instant::now();
    for i in 0..MESSAGE_COUNT {
        mediator.publish(Tick(i as u64)).await?;
    }
    let elapsed = started.elapsed();
    let ops_per_sec = MESSAGE_COUNT as f64 / elapsed.as_secs_f64();

    println!("=== Typed Mediator Event Publish (1 handler) ===");
    println!("  events:      {MESSAGE_COUNT}");
    println!("  total:       {elapsed:?}");
    println!("  throughput:  {:.0} events/s", ops_per_sec);
    println!(
        "  avg latency: {} ns",
        elapsed.as_nanos() / MESSAGE_COUNT as u128
    );
    println!();

    write_workload_report(
        "typed_mediator_event_publish",
        MESSAGE_COUNT,
        elapsed,
        rss_before_bytes,
    )?;
    Ok(())
}
