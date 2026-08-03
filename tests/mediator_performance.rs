//! Manual mediator scheduling performance contract.
//!
//! Run only when measuring scheduler throughput:
//! `cargo test --manifest-path tests/Cargo.toml --test mediator_performance -- --ignored --nocapture`
//!
//! The benchmark deliberately has no host-dependent timing threshold. It verifies that
//! [`Mediator::send_batch`] preserves response order and does not exceed its declared
//! in-flight limit, then reports the observed throughput for a fixed workload.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use async_trait::async_trait;
use catga_core::{CatgaResult, Handler, MAX_MEDIATOR_BATCH_SIZE, Mediator, Registry, Request};

#[path = "support/performance_report.rs"]
mod performance_report;

const MESSAGE_COUNT: usize = 4_096;
const CONCURRENCY_LIMIT: usize = 32;

/// A fixed-size request that keeps the scheduler path independent of serialization costs.
#[derive(Debug)]
struct ScheduledWork(usize);

impl catga_core::Message for ScheduledWork {}

impl Request for ScheduledWork {
    type Response = usize;
    type TypeId = catga_core::DefaultMessageTypeId;
}

/// Records the maximum number of request handlers active at the same time.
struct SchedulingHandler {
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

#[async_trait]
impl Handler<ScheduledWork> for SchedulingHandler {
    async fn handle(&self, message: ScheduledWork) -> CatgaResult<usize> {
        let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
        update_peak(&self.peak, active);
        tokio::task::yield_now().await;
        self.active.fetch_sub(1, Ordering::AcqRel);
        Ok(message.0)
    }
}

/// Raises `peak` to `candidate` when the observed concurrency is larger.
fn update_peak(peak: &AtomicUsize, candidate: usize) {
    let mut observed = peak.load(Ordering::Acquire);
    while candidate > observed {
        match peak.compare_exchange_weak(observed, candidate, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return,
            Err(current) => observed = current,
        }
    }
}

/// Measures bounded batch dispatch throughput without a timing threshold.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual performance benchmark; run with --ignored --nocapture"]
async fn mediator_batch_scheduler_throughput_benchmark() -> CatgaResult<()> {
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry.register_request::<ScheduledWork, _>(SchedulingHandler {
        active: Arc::clone(&active),
        peak: Arc::clone(&peak),
    })?;
    let mediator = Mediator::new(registry);

    let rss_before_bytes = performance_report::current_rss_bytes();
    let started = Instant::now();
    let mut batch_latencies = Vec::new();
    let mut responses = Vec::with_capacity(MESSAGE_COUNT);
    for batch_start in (0..MESSAGE_COUNT).step_by(MAX_MEDIATOR_BATCH_SIZE) {
        let batch_end = (batch_start + MAX_MEDIATOR_BATCH_SIZE).min(MESSAGE_COUNT);
        let batch_started = Instant::now();
        responses.extend(
            mediator
                .send_batch(
                    (batch_start..batch_end).map(ScheduledWork),
                    CONCURRENCY_LIMIT,
                )
                .await?,
        );
        batch_latencies.push(batch_started.elapsed());
    }
    let elapsed = started.elapsed();

    assert_eq!(responses.len(), MESSAGE_COUNT);
    for (index, response) in responses.into_iter().enumerate() {
        assert_eq!(response?, index);
    }
    assert_eq!(active.load(Ordering::Acquire), 0);
    assert!(peak.load(Ordering::Acquire) <= CONCURRENCY_LIMIT);

    let messages_per_second = (MESSAGE_COUNT as f64) / elapsed.as_secs_f64();
    println!(
        "mediator_batch_scheduler_throughput: messages={MESSAGE_COUNT}, concurrency_limit={CONCURRENCY_LIMIT}, peak_in_flight={}, total={elapsed:?}, messages_per_second={messages_per_second:.2}",
        peak.load(Ordering::Acquire),
    );
    let report = performance_report::PerformanceReport {
        schema_version: 1,
        source: "in-process mediator",
        environment: performance_report::environment(),
        results: vec![performance_report::measured(
            "mediator_batch_scheduler",
            None,
            elapsed,
            batch_latencies,
            "batch",
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
