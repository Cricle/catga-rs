//! Manual end-to-end in-process performance benchmark for Catga's critical application path.
//!
//! Run only when measuring performance:
//! `cargo test -p catga-tests --test critical_path_performance -- --ignored --nocapture`
//!
//! Each measured workflow performs a typed CQRS quote, two successful compensating Flow steps,
//! then publishes, receives, and explicitly acknowledges an envelope. The benchmark makes no
//! host-dependent timing assertion; it validates every result and prints the observed throughput.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use async_trait::async_trait;
use catga_core::{
    CatgaResult, Envelope, Handler, Mediator, Message, MessageMetadata, MessageTransport, Registry,
    Request,
};
use catga_flow::Flow;
use catga_memory::MemoryTransport;

#[path = "support/performance_report.rs"]
mod performance_report;

const WORKFLOW_COUNT: u64 = 4_096;

/// A fixed-cost query keeps the measured CQRS path independent of serialization and I/O.
struct Quote(u64);

impl Message for Quote {}

impl Request for Quote {
    type Response = u64;
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct QuoteHandler;

#[async_trait]
impl Handler<Quote> for QuoteHandler {
    async fn handle(&self, quote: Quote) -> CatgaResult<u64> {
        Ok(quote.0)
    }
}

/// Measures one caller-visible CQRS, Flow, and transport workflow per iteration.
#[tokio::test]
#[ignore = "manual performance benchmark; run with --ignored --nocapture"]
async fn critical_application_path_throughput_benchmark() -> CatgaResult<()> {
    let mut registry = Registry::new();
    registry.register_request::<Quote, _>(QuoteHandler)?;
    let mediator = Mediator::new(registry);
    let transport = MemoryTransport::new(16)?;
    let reservations = Arc::new(AtomicUsize::new(0));
    let charges = Arc::new(AtomicUsize::new(0));

    run_workflow(
        0,
        &mediator,
        &transport,
        Arc::clone(&reservations),
        Arc::clone(&charges),
    )
    .await?;

    let rss_before_bytes = performance_report::current_rss_bytes();
    let started = Instant::now();
    let mut latencies = Vec::with_capacity(WORKFLOW_COUNT as usize);
    for id in 1..=WORKFLOW_COUNT {
        let operation_started = Instant::now();
        run_workflow(
            id,
            &mediator,
            &transport,
            Arc::clone(&reservations),
            Arc::clone(&charges),
        )
        .await?;
        latencies.push(operation_started.elapsed());
    }
    let elapsed = started.elapsed();

    assert_eq!(
        reservations.load(Ordering::Acquire),
        WORKFLOW_COUNT as usize + 1
    );
    assert_eq!(charges.load(Ordering::Acquire), WORKFLOW_COUNT as usize + 1);
    let workflows_per_second = WORKFLOW_COUNT as f64 / elapsed.as_secs_f64();
    println!(
        "critical_application_path: workflows={WORKFLOW_COUNT}, total={elapsed:?}, workflows_per_second={workflows_per_second:.2}"
    );
    let report = performance_report::PerformanceReport {
        schema_version: 1,
        source: "in-process critical path",
        environment: performance_report::environment(),
        results: vec![performance_report::measured(
            "critical_application_path",
            None,
            elapsed,
            latencies,
            "workflow",
            rss_before_bytes,
        )],
        database_metric_deltas: Vec::new(),
    };
    performance_report::write_report_if_configured(&report)
        .map_err(|error| catga_core::CatgaError::new(catga_core::ErrorCode::Internal, error))?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("serialize report")
    );
    Ok(())
}

async fn run_workflow(
    id: u64,
    mediator: &Mediator,
    transport: &MemoryTransport,
    reservations: Arc<AtomicUsize>,
    charges: Arc<AtomicUsize>,
) -> CatgaResult<()> {
    let total_cents = mediator.send(Quote(2_598)).await?;
    let reserve = Arc::clone(&reservations);
    let release = Arc::clone(&reservations);
    let charge = Arc::clone(&charges);
    let refund = Arc::clone(&charges);
    let flow = Flow::new("critical-performance-checkout")
        .step(
            move || {
                let reserve = Arc::clone(&reserve);
                async move {
                    reserve.fetch_add(1, Ordering::AcqRel);
                    Ok(())
                }
            },
            move || {
                let release = Arc::clone(&release);
                async move {
                    release.fetch_sub(1, Ordering::AcqRel);
                    Ok(())
                }
            },
        )
        .step(
            move || {
                let charge = Arc::clone(&charge);
                async move {
                    charge.fetch_add(1, Ordering::AcqRel);
                    Ok(())
                }
            },
            move || {
                let refund = Arc::clone(&refund);
                async move {
                    refund.fetch_sub(1, Ordering::AcqRel);
                    Ok(())
                }
            },
        );
    assert!(flow.run().await.is_success());

    transport
        .publish(Envelope::new(
            id,
            "checkout.completed",
            total_cents.to_le_bytes().to_vec(),
            MessageMetadata::new(id, None),
        ))
        .await?;
    let delivery = transport.receive().await?;
    assert_eq!(delivery.envelope().id(), id);
    transport.ack(delivery).await
}
