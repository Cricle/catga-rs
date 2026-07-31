//! NATS integration tests for the [`Bus`] facade against a real JetStream server.
//!
//! These cover behavior the in-memory transport cannot: broker-driven redelivery, attempt-counted
//! dead-lettering, and competing consumption across bus instances. They are `#[ignore]`d by
//! default; provide `CATGA_NATS_URL` (for example `nats://127.0.0.1:4222` against a `nats -js`
//! server) and run with `cargo test -p catga-auto --test nats_integration -- --ignored`.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use catga_auto::{Bus, PublisherHandle};
use catga_codec_memorypack::{MemoryPackCodec, MemoryPackable};
use catga_core::{
    CatgaError, CatgaResult, DeadLetterStore, Destination, Message, SnowflakeIdGenerator,
    SnowflakeLayout, TypedDeliveryHandler, TypedTransport,
};
use catga_memory::MemoryDeadLetters;
use catga_nats::{NatsConfig, NatsDestinationConfig, NatsTransport};

#[derive(MemoryPackable)]
struct Ping(u32);
impl Message for Ping {}

struct Counter(Arc<AtomicUsize>);

#[async_trait]
impl TypedDeliveryHandler<Ping> for Counter {
    async fn handle(&self, _: &Ping) -> CatgaResult<()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct AlwaysFail;

#[async_trait]
impl TypedDeliveryHandler<Ping> for AlwaysFail {
    async fn handle(&self, _: &Ping) -> CatgaResult<()> {
        Err(CatgaError::new(
            catga_core::ErrorCode::Transient,
            "always fails",
        ))
    }
}

/// Fails the first `fail_until` deliveries, then succeeds: exercises broker redelivery.
struct Flaky {
    seen: Arc<AtomicUsize>,
    fail_until: usize,
}

#[async_trait]
impl TypedDeliveryHandler<Ping> for Flaky {
    async fn handle(&self, _: &Ping) -> CatgaResult<()> {
        let attempt = self.seen.fetch_add(1, Ordering::SeqCst);
        if attempt < self.fail_until {
            Err(CatgaError::new(
                catga_core::ErrorCode::Transient,
                "transient",
            ))
        } else {
            Ok(())
        }
    }
}

fn nats_url() -> Option<String> {
    std::env::var("CATGA_NATS_URL")
        .ok()
        .filter(|url| !url.trim().is_empty())
}

/// Builds a process-unique JetStream stream name so parallel tests never share state.
fn unique_stream(prefix: &str) -> String {
    static SEQ: AtomicUsize = AtomicUsize::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    format!("{prefix}_{millis}_{seq}")
}

async fn connect(url: &str, stream: &str, consumer: &str) -> Arc<NatsTransport> {
    Arc::new(
        NatsTransport::connect(NatsConfig {
            server: url.to_string().into(),
            stream: stream.to_string().into(),
            subject: format!("{stream}.cmds").into(),
            consumer: consumer.to_string().into(),
        })
        .await
        .expect("connect to NATS"),
    )
}

fn publisher(transport: Arc<NatsTransport>) -> TypedTransport<NatsTransport, MemoryPackCodec> {
    let ids = Arc::new(SnowflakeIdGenerator::new(1, SnowflakeLayout::default()).expect("ids"));
    TypedTransport::<NatsTransport, MemoryPackCodec>::new(transport, ids)
}

async fn wait_until<F: Fn() -> bool>(condition: F) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !condition() {
        assert!(
            std::time::Instant::now() < deadline,
            "condition not met within 30s"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires CATGA_NATS_URL pointing at a JetStream server"]
async fn nats_bus_consumes_published_commands() {
    let Some(url) = nats_url() else { return };
    let stream = unique_stream("CONSUME");
    let transport = connect(&url, &stream, "worker").await;
    let count = Arc::new(AtomicUsize::new(0));

    let bus = Bus::builder(transport.clone())
        .endpoint(
            "cmds",
            Arc::new(Counter(count.clone())),
            Arc::new(MemoryPackCodec::default()),
            4,
        )
        .expect("endpoint")
        .build();

    let publisher = publisher(transport);
    const TOTAL: u32 = 10;
    let driver = {
        let count = count.clone();
        let token = bus.shutdown_token();
        async move {
            for i in 0..TOTAL {
                publisher.publish(&Ping(i)).await.expect("publish");
            }
            wait_until(|| count.load(Ordering::SeqCst) >= TOTAL as usize).await;
            token.cancel();
        }
    };

    let (runs_result, ()) = tokio::join!(bus.run_until_cancelled(), driver);
    let runs = runs_result.expect("bus runs");
    assert_eq!(runs[0].acknowledged(), TOTAL as usize);
    assert_eq!(runs[0].rejected(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires CATGA_NATS_URL pointing at a JetStream server"]
async fn nats_routed_endpoint_rejects_an_unprovisioned_destination() {
    let Some(url) = nats_url() else { return };
    let stream = unique_stream("UNPROVISIONED_ROUTE");
    let transport = connect(&url, &stream, "worker").await;

    let error = Bus::builder(transport)
        .routed_endpoint::<Ping, _, _>(
            "commands",
            Arc::new(Counter(Arc::new(AtomicUsize::new(0)))),
            Arc::new(MemoryPackCodec::default()),
            1,
        )
        .err()
        .expect("NATS destinations must be provisioned before Bus construction");

    assert_eq!(error.code(), catga_core::ErrorCode::NotFound);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires CATGA_NATS_URL pointing at a JetStream server"]
async fn nats_bus_redelivers_a_transient_failure_until_success() {
    let Some(url) = nats_url() else { return };
    let stream = unique_stream("REDELIVER");
    let transport = connect(&url, &stream, "worker").await;
    let seen = Arc::new(AtomicUsize::new(0));

    // Fail the first two deliveries; the third succeeds after JetStream redelivers.
    let bus = Bus::builder(transport.clone())
        .endpoint(
            "cmds",
            Arc::new(Flaky {
                seen: seen.clone(),
                fail_until: 2,
            }),
            Arc::new(MemoryPackCodec::default()),
            1,
        )
        .expect("endpoint")
        .build();

    let publisher = publisher(transport);
    let driver = {
        let seen = seen.clone();
        let token = bus.shutdown_token();
        async move {
            publisher.publish(&Ping(1)).await.expect("publish");
            wait_until(|| seen.load(Ordering::SeqCst) >= 3).await;
            token.cancel();
        }
    };

    let (runs_result, ()) = tokio::join!(bus.run_until_cancelled(), driver);
    let runs = runs_result.expect("bus runs");
    assert_eq!(runs[0].acknowledged(), 1);
    assert!(seen.load(Ordering::SeqCst) >= 3, "message was redelivered");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires CATGA_NATS_URL pointing at a JetStream server"]
async fn nats_bus_dead_letters_after_max_attempts() {
    let Some(url) = nats_url() else { return };
    let stream = unique_stream("DEADLETTER");
    let transport = connect(&url, &stream, "worker").await;
    let dead_letters = Arc::new(MemoryDeadLetters::new(16).expect("dead letters"));

    const MAX_ATTEMPTS: u32 = 3;
    let bus = Bus::builder(transport.clone())
        .endpoint_with_dead_letters(
            "cmds",
            Arc::new(AlwaysFail),
            Arc::new(MemoryPackCodec::default()),
            1,
            MAX_ATTEMPTS,
            dead_letters.clone(),
        )
        .expect("endpoint")
        .build();

    let publisher = publisher(transport);
    let driver = {
        let dead_letters = dead_letters.clone();
        let token = bus.shutdown_token();
        async move {
            publisher.publish(&Ping(1)).await.expect("publish");
            let deadline = Instant::now() + Duration::from_secs(30);
            while dead_letters.list(10).await.expect("list").is_empty() {
                assert!(
                    Instant::now() < deadline,
                    "dead letter not written within 30s"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            token.cancel();
        }
    };

    let (runs_result, ()) = tokio::join!(bus.run_until_cancelled(), driver);
    let runs = runs_result.expect("bus runs");
    assert_eq!(runs[0].dead_lettered(), 1);
    assert_eq!(dead_letters.list(10).await.expect("list").len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires CATGA_NATS_URL pointing at a JetStream server"]
async fn nats_two_buses_compete_for_one_durable_consumer() {
    let Some(url) = nats_url() else { return };
    let stream = unique_stream("COMPETE");
    // Both buses share the same durable consumer name → one JetStream competing-consumer group.
    let transport_a = connect(&url, &stream, "shared-worker").await;
    let transport_b = connect(&url, &stream, "shared-worker").await;
    let count = Arc::new(AtomicUsize::new(0));
    let decoder = Arc::new(MemoryPackCodec::default());

    let bus_a = Bus::builder(transport_a)
        .endpoint("cmds", Arc::new(Counter(count.clone())), decoder.clone(), 2)
        .expect("endpoint a")
        .build();
    let bus_b = Bus::builder(transport_b)
        .endpoint("cmds", Arc::new(Counter(count.clone())), decoder, 2)
        .expect("endpoint b")
        .build();

    let publisher = publisher(connect(&url, &stream, "shared-worker").await);
    const TOTAL: u32 = 20;
    let driver = {
        let count = count.clone();
        let token_a = bus_a.shutdown_token();
        let token_b = bus_b.shutdown_token();
        async move {
            for i in 0..TOTAL {
                publisher.publish(&Ping(i)).await.expect("publish");
            }
            wait_until(|| count.load(Ordering::SeqCst) >= TOTAL as usize).await;
            token_a.cancel();
            token_b.cancel();
        }
    };

    let (runs_a, runs_b, ()) = tokio::join!(
        bus_a.run_until_cancelled(),
        bus_b.run_until_cancelled(),
        driver
    );
    let total: usize = runs_a
        .expect("bus a")
        .iter()
        .chain(runs_b.expect("bus b").iter())
        .map(|run| run.acknowledged())
        .sum();
    assert_eq!(total, TOTAL as usize);
    assert_eq!(count.load(Ordering::SeqCst), TOTAL as usize);
}

#[derive(Clone, MemoryPackable)]
struct PlaceOrder(u32);
impl Message for PlaceOrder {}

#[derive(Clone, MemoryPackable)]
struct OrderPlaced(u32);
impl Message for OrderPlaced {}

struct PlaceOrderHandler {
    publisher: PublisherHandle<NatsTransport, MemoryPackCodec>,
}

#[async_trait]
impl TypedDeliveryHandler<PlaceOrder> for PlaceOrderHandler {
    async fn handle(&self, cmd: &PlaceOrder) -> CatgaResult<()> {
        self.publisher.publish(&OrderPlaced(cmd.0)).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_routed_endpoints_isolate_message_types() {
    let Some(url) = nats_url() else { return };

    let cmd_stream = unique_stream("TOPO_CMD");
    let evt_stream = unique_stream("TOPO_EVT");

    let transport = Arc::new(
        NatsTransport::connect(NatsConfig {
            server: url.to_string().into(),
            stream: cmd_stream.clone().into(),
            subject: format!("{cmd_stream}.cmds").into(),
            consumer: format!("{cmd_stream}_consumer").into(),
        })
        .await
        .expect("connect"),
    );

    transport
        .provision_destination(
            Destination::parse("commands").expect("valid"),
            NatsDestinationConfig {
                stream: cmd_stream.clone().into(),
                subject: format!("{cmd_stream}.cmds").into(),
                consumer: format!("{cmd_stream}_consumer").into(),
            },
        )
        .await
        .expect("provision commands");

    transport
        .provision_destination(
            Destination::parse("events").expect("valid"),
            NatsDestinationConfig {
                stream: evt_stream.clone().into(),
                subject: format!("{evt_stream}.evts").into(),
                consumer: format!("{evt_stream}_consumer").into(),
            },
        )
        .await
        .expect("provision events");

    let handle = PublisherHandle::new();
    let events_seen = Arc::new(AtomicUsize::new(0));
    let events_counter = events_seen.clone();

    struct EventCounter(Arc<AtomicUsize>);
    #[async_trait]
    impl TypedDeliveryHandler<OrderPlaced> for EventCounter {
        async fn handle(&self, _: &OrderPlaced) -> CatgaResult<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let (bus, publisher) = Bus::builder(Arc::clone(&transport))
        .routed_endpoint::<PlaceOrder, _, _>(
            "commands",
            Arc::new(PlaceOrderHandler {
                publisher: handle.clone(),
            }),
            Arc::new(MemoryPackCodec::default()),
            1,
        )
        .expect("command endpoint")
        .routed_endpoint::<OrderPlaced, _, _>(
            "events",
            Arc::new(EventCounter(events_counter)),
            Arc::new(MemoryPackCodec::default()),
            1,
        )
        .expect("event endpoint")
        .build_with_publisher(MemoryPackCodec::default())
        .expect("build");

    handle.bind(publisher);

    handle
        .publish(&PlaceOrder(99))
        .await
        .expect("publish command");

    let token = bus.shutdown_token();
    let driver = async {
        wait_until(|| events_seen.load(Ordering::SeqCst) >= 1).await;
        token.cancel();
    };
    let (result, ()) = tokio::join!(bus.run_until_cancelled(), driver);
    let runs = result.expect("bus run");

    assert_eq!(runs[0].acknowledged(), 1, "command endpoint");
    assert_eq!(runs[1].acknowledged(), 1, "event endpoint");
    assert_eq!(events_seen.load(Ordering::SeqCst), 1);
}
