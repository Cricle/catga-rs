//! Manual release-only performance measurements for Catga's crate-local memory paths.
//!
//! Run only when measuring performance:
//! `cargo test --release -p catga-memory --test memory_performance -- --ignored --nocapture`
//!
//! The report records throughput, per-operation p50/p95/p99 latency, and Linux `/proc/self/status`
//! RSS values. It intentionally has no host-dependent performance threshold.

use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

use catga_core::{
    CatgaError, CatgaResult, Envelope, ErrorCode, Mediator, Message, MessageMetadata,
    MessageTransport, OutboxMessage, OutboxStore, Registry, Request, request_handler,
};
use catga_flow::Flow;
use catga_memory::{MemoryOutbox, MemoryTransport};
use serde::Serialize;
use tokio::sync::mpsc;

const OPERATION_COUNT: u64 = 4_096;
const PAYLOAD_BYTES: usize = 256;

struct MemoryRequest(u64);

impl Message for MemoryRequest {}

impl Request for MemoryRequest {
    type Response = u64;
}

#[derive(Serialize)]
struct MemoryPerformanceResult {
    name: &'static str,
    operations: u64,
    elapsed_nanoseconds: u64,
    operations_per_second: f64,
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    rss_before_bytes: u64,
    rss_after_bytes: u64,
    rss_peak_bytes: u64,
}

#[derive(Serialize)]
struct MemoryPerformanceReport {
    schema_version: u8,
    environment: MemoryEnvironment,
    results: Vec<MemoryPerformanceResult>,
}

#[derive(Serialize)]
struct MemoryEnvironment {
    operating_system: &'static str,
    rss_source: &'static str,
}

/// Measures the complete publish, receive, and acknowledgement path in [`MemoryTransport`].
#[tokio::test]
#[ignore = "manual release-only memory performance benchmark; run with --release --ignored --nocapture"]
async fn memory_performance_report() -> CatgaResult<()> {
    let transport = MemoryTransport::new(1)?;
    let mut registry = Registry::new();
    registry.register_request::<MemoryRequest, _>(request_handler(
        |request: MemoryRequest| async move { Ok(request.0) },
    ))?;
    let mediator = Mediator::new(registry);

    warm_memory_transport(&transport).await?;
    assert_eq!(mediator.send(MemoryRequest(0)).await?, 0);
    assert_successful_flow(memory_flow().run().await);

    let native_transport_result = measure_tokio_mpsc().await?;
    let transport_result = measure_memory_transport(&transport).await?;
    let mediator_result = measure_mediator(&mediator).await?;
    let flow_result = measure_flow().await?;
    let outbox_result = measure_outbox_retention().await?;
    let report = MemoryPerformanceReport {
        schema_version: 1,
        environment: MemoryEnvironment {
            operating_system: "linux",
            rss_source: "/proc/self/status (VmRSS and VmHWM)",
        },
        results: vec![
            native_transport_result,
            transport_result,
            mediator_result,
            flow_result,
            outbox_result,
        ],
    };

    write_report(&report)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report).map_err(debug_error)?
    );
    Ok(())
}

async fn warm_memory_transport(transport: &MemoryTransport) -> CatgaResult<()> {
    transport.publish(memory_envelope(0)).await?;
    transport.ack(transport.receive().await?).await
}

async fn measure_memory_transport(
    transport: &MemoryTransport,
) -> CatgaResult<MemoryPerformanceResult> {
    let rss_before_bytes = current_rss_bytes()?;
    let started = Instant::now();
    let mut latencies = Vec::with_capacity(OPERATION_COUNT as usize);
    for id in 1..=OPERATION_COUNT {
        let operation_started = Instant::now();
        transport.publish(memory_envelope(id)).await?;
        let delivery = transport.receive().await?;
        assert_eq!(delivery.envelope().id(), id);
        transport.ack(delivery).await?;
        latencies.push(operation_started.elapsed());
    }
    measured(
        "memory_transport_round_trip",
        started.elapsed(),
        latencies,
        rss_before_bytes,
    )
}

async fn measure_tokio_mpsc() -> CatgaResult<MemoryPerformanceResult> {
    let (sender, mut receiver) = mpsc::channel(1);
    sender
        .send(memory_envelope(0))
        .await
        .map_err(|_| performance_error("Tokio mpsc receiver closed during warm-up"))?;
    let _ = receiver
        .recv()
        .await
        .ok_or_else(|| performance_error("Tokio mpsc sender closed during warm-up"))?;

    let rss_before_bytes = current_rss_bytes()?;
    let started = Instant::now();
    let mut latencies = Vec::with_capacity(OPERATION_COUNT as usize);
    for id in 1..=OPERATION_COUNT {
        let operation_started = Instant::now();
        sender
            .send(memory_envelope(id))
            .await
            .map_err(|_| performance_error("Tokio mpsc receiver closed"))?;
        let envelope = receiver
            .recv()
            .await
            .ok_or_else(|| performance_error("Tokio mpsc sender closed"))?;
        assert_eq!(envelope.id(), id);
        latencies.push(operation_started.elapsed());
    }
    measured(
        "tokio_mpsc_round_trip_lower_bound",
        started.elapsed(),
        latencies,
        rss_before_bytes,
    )
}

async fn measure_mediator(mediator: &Mediator) -> CatgaResult<MemoryPerformanceResult> {
    let rss_before_bytes = current_rss_bytes()?;
    let started = Instant::now();
    let mut latencies = Vec::with_capacity(OPERATION_COUNT as usize);
    for id in 1..=OPERATION_COUNT {
        let operation_started = Instant::now();
        assert_eq!(mediator.send(MemoryRequest(id)).await?, id);
        latencies.push(operation_started.elapsed());
    }
    measured(
        "mediator_request",
        started.elapsed(),
        latencies,
        rss_before_bytes,
    )
}

async fn measure_flow() -> CatgaResult<MemoryPerformanceResult> {
    let flows = (0..OPERATION_COUNT)
        .map(|_| memory_flow())
        .collect::<Vec<_>>();
    let rss_before_bytes = current_rss_bytes()?;
    let started = Instant::now();
    let mut latencies = Vec::with_capacity(OPERATION_COUNT as usize);
    for flow in flows {
        let operation_started = Instant::now();
        assert_successful_flow(flow.run().await);
        latencies.push(operation_started.elapsed());
    }
    measured(
        "flow_execution",
        started.elapsed(),
        latencies,
        rss_before_bytes,
    )
}

async fn measure_outbox_retention() -> CatgaResult<MemoryPerformanceResult> {
    let rss_before_bytes = current_rss_bytes()?;
    let outbox = MemoryOutbox::new(OPERATION_COUNT as usize)?;
    let started = Instant::now();
    let mut latencies = Vec::with_capacity(OPERATION_COUNT as usize);
    for id in 1..=OPERATION_COUNT {
        let operation_started = Instant::now();
        outbox
            .enqueue(OutboxMessage::new(memory_envelope(id)))
            .await?;
        latencies.push(operation_started.elapsed());
    }
    measured(
        "memory_outbox_retained_records",
        started.elapsed(),
        latencies,
        rss_before_bytes,
    )
}

fn memory_flow() -> Flow {
    Flow::new("memory-performance-flow")
        .step(|| async { Ok(()) }, || async { Ok(()) })
        .step(|| async { Ok(()) }, || async { Ok(()) })
        .step(|| async { Ok(()) }, || async { Ok(()) })
}

fn assert_successful_flow(result: catga_flow::FlowResult) {
    assert!(result.is_success());
    assert_eq!(result.completed_steps(), 3);
}

fn memory_envelope(id: u64) -> Envelope {
    Envelope::new(
        id,
        "memory.performance",
        vec![0xA5; PAYLOAD_BYTES],
        MessageMetadata::new(id, None),
    )
}

fn measured(
    name: &'static str,
    elapsed: Duration,
    latencies: Vec<Duration>,
    rss_before_bytes: u64,
) -> CatgaResult<MemoryPerformanceResult> {
    let operations = u64::try_from(latencies.len()).map_err(debug_error)?;
    let elapsed_nanoseconds = u64::try_from(elapsed.as_nanos()).map_err(debug_error)?;
    let rss_after_bytes = current_rss_bytes()?;
    let rss_peak_bytes = peak_rss_bytes()?;
    Ok(MemoryPerformanceResult {
        name,
        operations,
        elapsed_nanoseconds,
        operations_per_second: operations as f64 / elapsed.as_secs_f64(),
        p50_ns: percentile_nanoseconds(&latencies, 50),
        p95_ns: percentile_nanoseconds(&latencies, 95),
        p99_ns: percentile_nanoseconds(&latencies, 99),
        rss_before_bytes,
        rss_after_bytes,
        rss_peak_bytes,
    })
}

fn percentile_nanoseconds(latencies: &[Duration], percentile: usize) -> u64 {
    assert!(!latencies.is_empty());
    assert!((1..=100).contains(&percentile));
    let mut nanoseconds = latencies
        .iter()
        .map(|latency| u64::try_from(latency.as_nanos()).unwrap_or(u64::MAX))
        .collect::<Vec<_>>();
    nanoseconds.sort_unstable();
    let rank = (nanoseconds.len() * percentile)
        .div_ceil(100)
        .saturating_sub(1);
    nanoseconds[rank]
}

fn current_rss_bytes() -> CatgaResult<u64> {
    proc_status_bytes("VmRSS:")
}

fn peak_rss_bytes() -> CatgaResult<u64> {
    proc_status_bytes("VmHWM:")
}

fn proc_status_bytes(field: &str) -> CatgaResult<u64> {
    if !cfg!(target_os = "linux") {
        return Err(performance_error(format!(
            "{field} measurement requires Linux /proc/self/status"
        )));
    }
    let status = std::fs::read_to_string("/proc/self/status").map_err(|error| {
        performance_error("read /proc/self/status").with_details(error.to_string())
    })?;
    let kibibytes = status
        .lines()
        .find_map(|line| line.strip_prefix(field))
        .and_then(|line| line.split_whitespace().next())
        .ok_or_else(|| performance_error(format!("/proc/self/status does not contain {field}")))?
        .parse::<u64>()
        .map_err(|error| {
            performance_error(format!("parse {field} from /proc/self/status"))
                .with_details(error.to_string())
        })?;
    Ok(kibibytes * 1024)
}

fn write_report(report: &MemoryPerformanceReport) -> CatgaResult<()> {
    let Some(path) = report_path() else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| {
            performance_error("create memory performance report directory")
                .with_details(error.to_string())
        })?;
    }
    let serialized = serde_json::to_vec_pretty(report).map_err(debug_error)?;
    std::fs::write(path, serialized).map_err(|error| {
        performance_error("write memory performance report").with_details(error.to_string())
    })?;
    Ok(())
}

fn report_path() -> Option<PathBuf> {
    let value = std::env::var_os("CATGA_PERFORMANCE_RESULTS")?;
    let path = PathBuf::from(value);
    Some(if path.extension().is_some() {
        path
    } else {
        path.join("memory-performance.json")
    })
}

fn debug_error(error: impl std::fmt::Debug) -> CatgaError {
    performance_error(format!("{error:?}"))
}

fn performance_error(message: impl Into<Box<str>>) -> CatgaError {
    CatgaError::new(ErrorCode::Internal, message)
}
