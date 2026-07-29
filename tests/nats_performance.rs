//! Manual NATS JetStream performance benchmark.
//!
//! Run only when measuring performance:
//! `cargo test -p catga-tests --test nats_performance -- --ignored --nocapture`
//!
//! The benchmark excludes NATS resource provisioning and envelope construction from the timed
//! interval. It measures a durable `AtLeastOnce` publish acknowledgement, receive, and explicit
//! acknowledgement for each message. `nats_e2e::server_url` uses `CATGA_NATS_URL` when set;
//! otherwise it starts and removes an isolated JetStream container.

use std::{
    fmt::Debug,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use catga_core::{Envelope, MessageMetadata, MessageTransport};
use catga_nats::{NatsConfig, NatsTransport};

#[path = "support/nats_e2e.rs"]
mod nats_e2e;
#[path = "support/performance_report.rs"]
mod performance_report;

const MESSAGE_COUNT: u64 = 1_000;
const PAYLOAD_BYTES: usize = 256;

/// Measures durable JetStream round trips without enforcing a timing threshold.
#[tokio::test]
#[ignore = "manual performance benchmark; run with --ignored --nocapture"]
async fn nats_jetstream_publish_receive_ack_benchmark() -> Result<(), String> {
    let server = nats_e2e::server_url().await;
    let suffix = benchmark_suffix();
    let transport = NatsTransport::connect(NatsConfig {
        server: server.url().into(),
        stream: format!("CATGA_BENCH_{suffix}").into(),
        subject: format!("catga.bench.{suffix}").into(),
        consumer: format!("catga_bench_{suffix}").into(),
    })
    .await
    .map_err(debug_error)?;

    transport
        .publish(benchmark_envelope(0))
        .await
        .map_err(debug_error)?;
    let warmup = transport.receive().await.map_err(debug_error)?;
    transport.ack(warmup).await.map_err(debug_error)?;

    let envelopes = (1..=MESSAGE_COUNT)
        .map(benchmark_envelope)
        .collect::<Vec<_>>();
    let rss_before_bytes = performance_report::current_rss_bytes();
    let started = Instant::now();
    let mut latencies = Vec::with_capacity(MESSAGE_COUNT as usize);
    for envelope in envelopes {
        let operation_started = Instant::now();
        let expected_id = envelope.id();
        transport.publish(envelope).await.map_err(debug_error)?;
        let delivery = transport.receive().await.map_err(debug_error)?;
        assert_eq!(delivery.envelope().id(), expected_id);
        transport.ack(delivery).await.map_err(debug_error)?;
        latencies.push(operation_started.elapsed());
    }
    let elapsed = started.elapsed();
    let elapsed_per_message = elapsed / (MESSAGE_COUNT as u32);
    let messages_per_second = (MESSAGE_COUNT as f64) / elapsed.as_secs_f64();

    println!(
        "nats_jetstream_publish_receive_ack: messages={MESSAGE_COUNT}, payload_bytes={PAYLOAD_BYTES}, total={elapsed:?}, per_message={elapsed_per_message:?}, messages_per_second={messages_per_second:.2}"
    );
    let report = performance_report::PerformanceReport {
        schema_version: 1,
        source: "NATS JetStream",
        environment: performance_report::environment(),
        results: vec![performance_report::measured(
            "nats_jetstream_publish_receive_ack",
            Some(PAYLOAD_BYTES),
            elapsed,
            latencies,
            "message round trip",
            rss_before_bytes,
        )],
        database_metric_deltas: Vec::new(),
    };
    performance_report::write_report_if_configured(&report)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report).map_err(debug_error)?
    );

    server.close().await.map_err(debug_error)?;
    Ok(())
}

fn benchmark_envelope(id: u64) -> Envelope {
    Envelope::new(
        id,
        "benchmark.message",
        vec![0xA5; PAYLOAD_BYTES],
        MessageMetadata::new(id, None),
    )
}

fn benchmark_suffix() -> String {
    let epoch_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("{}_{}", std::process::id(), epoch_nanos)
}

fn debug_error(error: impl Debug) -> String {
    format!("{error:?}")
}
