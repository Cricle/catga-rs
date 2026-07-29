//! Docker-backed performance measurements for Catga's HTTP and durable-message paths.
//!
//! Run through `scripts/performance.sh`, which starts the repository Compose stack and sets
//! `CATGA_NATS_URL`. Set `CATGA_PERFORMANCE_RESULTS` to write the machine-readable summary.

use std::{
    future::IntoFuture,
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use catga_core::{
    CatgaResult, Envelope, Handler, Mediator, Message, MessageMetadata, MessageTransport, Registry,
    Request,
};
use catga_nats::{NatsConfig, NatsTransport};
use serde::{Deserialize, Serialize};

const HTTP_REQUEST_COUNT: u64 = 512;
const NATS_MESSAGE_COUNT: u64 = 512;
const PAYLOAD_BYTES: usize = 256;

#[derive(Deserialize, Serialize)]
struct PriceOrder {
    quantity: u32,
    unit_price_cents: u64,
}

impl Message for PriceOrder {}

impl Request for PriceOrder {
    type Response = PriceResponse;
}

#[derive(Deserialize, Serialize)]
struct PriceResponse {
    total_cents: u64,
}

struct PriceHandler;

#[async_trait]
impl Handler<PriceOrder> for PriceHandler {
    async fn handle(&self, order: PriceOrder) -> CatgaResult<PriceResponse> {
        Ok(PriceResponse {
            total_cents: u64::from(order.quantity) * order.unit_price_cents,
        })
    }
}

#[derive(Serialize)]
struct PerformanceResult {
    name: &'static str,
    operations: u64,
    elapsed_nanoseconds: u128,
    operations_per_second: f64,
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
}

#[derive(Serialize)]
struct PerformanceReport {
    schema_version: u8,
    environment: &'static str,
    results: Vec<PerformanceResult>,
}

/// Exercises real TCP Axum requests and a Docker NATS JetStream round trip without thresholds.
#[tokio::test]
#[ignore = "Docker E2E performance benchmark; run scripts/performance.sh --nocapture"]
async fn docker_backed_http_and_nats_performance() -> Result<(), String> {
    let nats_url = std::env::var("CATGA_NATS_URL")
        .map_err(|_| "CATGA_NATS_URL must be configured for Docker E2E performance tests")?;
    let mut registry = Registry::new();
    registry
        .register_request::<PriceOrder, _>(PriceHandler)
        .map_err(debug_error)?;
    let mediator = Arc::new(Mediator::new(registry));
    let app = catga_axum::catga_routes! {
        mediator = mediator;
        requests { @post "/orders/price" => PriceOrder }
        events {}
    }
    .map_err(debug_error)?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(debug_error)?;
    let endpoint = format!(
        "http://{}/orders/price",
        listener.local_addr().map_err(debug_error)?
    );
    let server = tokio::spawn(axum::serve(listener, app).into_future());
    let client = reqwest::Client::new();

    send_price(&client, &endpoint).await?;
    let http_started = Instant::now();
    let mut http_latencies = Vec::with_capacity(HTTP_REQUEST_COUNT as usize);
    for _ in 0..HTTP_REQUEST_COUNT {
        let operation_started = Instant::now();
        send_price(&client, &endpoint).await?;
        http_latencies.push(operation_started.elapsed());
    }
    let http_elapsed = http_started.elapsed();
    server.abort();

    let suffix = unique_suffix();
    let transport = NatsTransport::connect(NatsConfig {
        server: nats_url.into(),
        stream: format!("CATGA_PERF_{suffix}").into(),
        subject: format!("catga.perf.{suffix}").into(),
        consumer: format!("catga_perf_{suffix}").into(),
    })
    .await
    .map_err(debug_error)?;
    transport
        .publish(performance_envelope(0))
        .await
        .map_err(debug_error)?;
    transport
        .ack(transport.receive().await.map_err(debug_error)?)
        .await
        .map_err(debug_error)?;
    let nats_started = Instant::now();
    let mut nats_latencies = Vec::with_capacity(NATS_MESSAGE_COUNT as usize);
    for id in 1..=NATS_MESSAGE_COUNT {
        let operation_started = Instant::now();
        transport
            .publish(performance_envelope(id))
            .await
            .map_err(debug_error)?;
        let delivery = transport.receive().await.map_err(debug_error)?;
        assert_eq!(delivery.envelope().id(), id);
        transport.ack(delivery).await.map_err(debug_error)?;
        nats_latencies.push(operation_started.elapsed());
    }
    let nats_elapsed = nats_started.elapsed();

    let report = PerformanceReport {
        schema_version: 1,
        environment: "docker-compose",
        results: vec![
            measured(
                "axum_http_quote",
                HTTP_REQUEST_COUNT,
                http_elapsed,
                http_latencies,
            ),
            measured(
                "nats_jetstream_round_trip",
                NATS_MESSAGE_COUNT,
                nats_elapsed,
                nats_latencies,
            ),
        ],
    };
    write_report(&report)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report).map_err(debug_error)?
    );
    Ok(())
}

async fn send_price(client: &reqwest::Client, endpoint: &str) -> Result<(), String> {
    let response = client
        .post(endpoint)
        .json(&PriceOrder {
            quantity: 2,
            unit_price_cents: 1_299,
        })
        .send()
        .await
        .map_err(debug_error)?;
    if !response.status().is_success() {
        return Err(format!(
            "Axum quote endpoint returned {}",
            response.status()
        ));
    }
    let price = response
        .json::<PriceResponse>()
        .await
        .map_err(debug_error)?;
    if price.total_cents != 2_598 {
        return Err(format!("Axum quote returned {} cents", price.total_cents));
    }
    Ok(())
}

fn performance_envelope(id: u64) -> Envelope {
    Envelope::new(
        id,
        "checkout.completed",
        vec![0xA5; PAYLOAD_BYTES],
        MessageMetadata::new(id, None),
    )
}

fn measured(
    name: &'static str,
    operations: u64,
    elapsed: std::time::Duration,
    latencies: Vec<std::time::Duration>,
) -> PerformanceResult {
    PerformanceResult {
        name,
        operations,
        elapsed_nanoseconds: elapsed.as_nanos(),
        operations_per_second: operations as f64 / elapsed.as_secs_f64(),
        p50_ns: percentile_nanoseconds(&latencies, 50),
        p95_ns: percentile_nanoseconds(&latencies, 95),
        p99_ns: percentile_nanoseconds(&latencies, 99),
    }
}

fn percentile_nanoseconds(latencies: &[std::time::Duration], percentile: usize) -> u64 {
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

#[test]
fn latency_percentiles_use_nearest_rank() {
    let samples = (1..=100)
        .map(std::time::Duration::from_nanos)
        .collect::<Vec<_>>();
    assert_eq!(percentile_nanoseconds(&samples, 50), 50);
    assert_eq!(percentile_nanoseconds(&samples, 95), 95);
    assert_eq!(percentile_nanoseconds(&samples, 99), 99);
}

fn unique_suffix() -> String {
    let epoch_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("{}_{}", std::process::id(), epoch_nanos)
}

fn write_report(report: &PerformanceReport) -> Result<(), String> {
    let Ok(path) = std::env::var("CATGA_PERFORMANCE_RESULTS") else {
        return Ok(());
    };
    let serialized = serde_json::to_vec_pretty(report).map_err(debug_error)?;
    std::fs::write(path, serialized).map_err(debug_error)
}

fn debug_error(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
}
