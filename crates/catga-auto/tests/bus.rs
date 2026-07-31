//! End-to-end tests for the declarative [`Bus`] facade over an in-memory transport.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use catga_auto::Bus;
use catga_codec_memorypack::{MemoryPackCodec, MemoryPackable};
use catga_core::{
    CatgaError, CatgaResult, DeadLetterStore, DeliveryHandler, Envelope, ErrorCode, Message,
    ShutdownCoordinator, SnowflakeIdGenerator, SnowflakeLayout, TypedDeliveryHandler,
    TypedTransport,
};
use catga_memory::{MemoryDeadLetters, MemoryTransport};

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

#[tokio::test(flavor = "current_thread")]
async fn bus_drives_an_endpoint_until_shutdown() {
    let transport = Arc::new(MemoryTransport::new(64).expect("bounded transport"));
    let count = Arc::new(AtomicUsize::new(0));

    let bus = Bus::builder(transport.clone())
        .endpoint(
            "pings",
            Arc::new(Counter(count.clone())),
            Arc::new(MemoryPackCodec::default()),
            2,
        )
        .expect("valid endpoint")
        .build();
    assert_eq!(bus.endpoint_names(), vec!["pings"]);

    let ids = Arc::new(SnowflakeIdGenerator::new(1, SnowflakeLayout::default()).expect("ids"));
    let publisher = TypedTransport::<MemoryTransport, MemoryPackCodec>::new(transport, ids);

    const TOTAL: u32 = 5;
    let driver = {
        let count = count.clone();
        let bus = bus.shutdown_token();
        async move {
            for i in 0..TOTAL {
                publisher.publish(&Ping(i)).await.expect("publish");
            }
            while count.load(Ordering::SeqCst) < TOTAL as usize {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            // All messages handled; request a graceful stop.
            bus.cancel();
        }
    };

    let (runs_result, ()) = tokio::join!(bus.run_until_cancelled(), driver);
    let runs = runs_result.expect("bus runs");
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].acknowledged(), TOTAL as usize);
    assert_eq!(runs[0].rejected(), 0);
    assert_eq!(count.load(Ordering::SeqCst), TOTAL as usize);
}

#[tokio::test(flavor = "current_thread")]
async fn bus_rejects_a_zero_concurrency_endpoint() {
    let transport = Arc::new(MemoryTransport::new(8).expect("bounded transport"));
    let result = Bus::builder(transport).endpoint(
        "bad",
        Arc::new(Counter(Arc::new(AtomicUsize::new(0)))),
        Arc::new(MemoryPackCodec::default()),
        0,
    );
    assert!(result.is_err());
}

struct AlwaysFail;

#[async_trait]
impl TypedDeliveryHandler<Ping> for AlwaysFail {
    async fn handle(&self, _: &Ping) -> CatgaResult<()> {
        Err(CatgaError::new(ErrorCode::HandlerFailed, "poison message"))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn bus_dead_letters_a_poison_message() {
    let transport = Arc::new(MemoryTransport::new(16).expect("bounded transport"));
    let dead_letters = Arc::new(MemoryDeadLetters::new(16).expect("bounded dead letters"));

    let bus = Bus::builder(transport.clone())
        .endpoint_with_dead_letters(
            "poison",
            Arc::new(AlwaysFail),
            Arc::new(MemoryPackCodec::default()),
            1,
            // The in-memory transport never redelivers, so the first failure is already
            // attempt one and is dead-lettered immediately.
            1,
            dead_letters.clone(),
        )
        .expect("valid endpoint")
        .build();

    let ids = Arc::new(SnowflakeIdGenerator::new(1, SnowflakeLayout::default()).expect("ids"));
    let publisher = TypedTransport::<MemoryTransport, MemoryPackCodec>::new(transport, ids);

    let driver = {
        let dead_letters = dead_letters.clone();
        let token = bus.shutdown_token();
        async move {
            publisher.publish(&Ping(1)).await.expect("publish");
            while dead_letters.list(10).await.expect("list").is_empty() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            token.cancel();
        }
    };

    let (runs_result, ()) = tokio::join!(bus.run_until_cancelled(), driver);
    let runs = runs_result.expect("bus runs");
    assert_eq!(runs[0].dead_lettered(), 1);
    assert_eq!(runs[0].acknowledged(), 1);
    assert_eq!(dead_letters.list(10).await.expect("list").len(), 1);
}

// ---------------------------------------------------------------------------
// Configuration validation
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn bus_reports_endpoint_names_in_registration_order() {
    let transport = Arc::new(MemoryTransport::new(8).expect("bounded transport"));
    let decoder = Arc::new(MemoryPackCodec::default());
    let count = Arc::new(AtomicUsize::new(0));
    let bus = Bus::builder(transport)
        .endpoint(
            "alpha",
            Arc::new(Counter(count.clone())),
            decoder.clone(),
            1,
        )
        .expect("alpha")
        .endpoint("beta", Arc::new(Counter(count.clone())), decoder.clone(), 1)
        .expect("beta")
        .endpoint("gamma", Arc::new(Counter(count)), decoder, 1)
        .expect("gamma")
        .build();
    assert_eq!(bus.endpoint_names(), vec!["alpha", "beta", "gamma"]);
}

#[tokio::test(flavor = "current_thread")]
async fn endpoint_with_dead_letters_rejects_zero_max_attempts() {
    let transport = Arc::new(MemoryTransport::new(8).expect("bounded transport"));
    let dead_letters = Arc::new(MemoryDeadLetters::new(8).expect("bounded dead letters"));
    let result = Bus::builder(transport).endpoint_with_dead_letters(
        "bad",
        Arc::new(Counter(Arc::new(AtomicUsize::new(0)))),
        Arc::new(MemoryPackCodec::default()),
        1,
        0, // zero attempts is not a valid terminal threshold
        dead_letters,
    );
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// Runtime orchestration
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn bus_drives_multiple_competing_endpoints_over_one_transport() {
    let transport = Arc::new(MemoryTransport::new(64).expect("bounded transport"));
    let count = Arc::new(AtomicUsize::new(0));
    let decoder = Arc::new(MemoryPackCodec::default());

    // Two endpoints of the same message type compete for one queue; the total
    // acknowledged across both must equal the number published.
    let bus = Bus::builder(transport.clone())
        .endpoint(
            "worker-a",
            Arc::new(Counter(count.clone())),
            decoder.clone(),
            2,
        )
        .expect("worker-a")
        .endpoint("worker-b", Arc::new(Counter(count.clone())), decoder, 2)
        .expect("worker-b")
        .build();

    let ids = Arc::new(SnowflakeIdGenerator::new(1, SnowflakeLayout::default()).expect("ids"));
    let publisher = TypedTransport::<MemoryTransport, MemoryPackCodec>::new(transport, ids);

    const TOTAL: u32 = 20;
    let driver = {
        let count = count.clone();
        let token = bus.shutdown_token();
        async move {
            for i in 0..TOTAL {
                publisher.publish(&Ping(i)).await.expect("publish");
            }
            while count.load(Ordering::SeqCst) < TOTAL as usize {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            token.cancel();
        }
    };

    let (runs_result, ()) = tokio::join!(bus.run_until_cancelled(), driver);
    let runs = runs_result.expect("bus runs");
    assert_eq!(runs.len(), 2);
    let total_acknowledged: usize = runs.iter().map(|run| run.acknowledged()).sum();
    assert_eq!(total_acknowledged, TOTAL as usize);
    assert_eq!(count.load(Ordering::SeqCst), TOTAL as usize);
}

#[tokio::test(flavor = "current_thread")]
async fn bus_with_no_endpoints_returns_empty_runs() {
    let transport = Arc::new(MemoryTransport::new(8).expect("bounded transport"));
    let bus = Bus::builder(transport).build();
    assert!(bus.endpoint_names().is_empty());
    // With nothing to drive, join_all resolves immediately without awaiting the token.
    let runs = bus.run_until_cancelled().await.expect("empty bus runs");
    assert!(runs.is_empty());
}

struct EnvelopeCounter(Arc<AtomicUsize>);

#[async_trait]
impl DeliveryHandler for EnvelopeCounter {
    async fn handle(&self, _: &Envelope) -> CatgaResult<()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn bus_drives_a_raw_envelope_endpoint() {
    let transport = Arc::new(MemoryTransport::new(16).expect("bounded transport"));
    let seen = Arc::new(AtomicUsize::new(0));
    let bus = Bus::builder(transport.clone())
        .endpoint_raw("raw", Arc::new(EnvelopeCounter(seen.clone())), 1)
        .expect("raw endpoint")
        .build();

    let ids = Arc::new(SnowflakeIdGenerator::new(1, SnowflakeLayout::default()).expect("ids"));
    let publisher = TypedTransport::<MemoryTransport, MemoryPackCodec>::new(transport, ids);

    let driver = {
        let seen = seen.clone();
        let token = bus.shutdown_token();
        async move {
            publisher.publish(&Ping(7)).await.expect("publish");
            while seen.load(Ordering::SeqCst) < 1 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            token.cancel();
        }
    };

    let (runs_result, ()) = tokio::join!(bus.run_until_cancelled(), driver);
    let runs = runs_result.expect("bus runs");
    assert_eq!(runs[0].acknowledged(), 1);
    assert_eq!(seen.load(Ordering::SeqCst), 1);
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn bus_honors_an_external_shutdown_token() {
    let transport = Arc::new(MemoryTransport::new(8).expect("bounded transport"));
    let coordinator = ShutdownCoordinator::default();
    let bus = Bus::builder(transport)
        .with_shutdown(coordinator.clone())
        .endpoint(
            "idle",
            Arc::new(Counter(Arc::new(AtomicUsize::new(0)))),
            Arc::new(MemoryPackCodec::default()),
            1,
        )
        .expect("endpoint")
        .build();

    // Cancelling the application-owned coordinator stops the bus without any traffic.
    let driver = async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        coordinator.request_shutdown();
    };

    let (runs_result, ()) = tokio::join!(bus.run_until_cancelled(), driver);
    let runs = runs_result.expect("bus runs");
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].acknowledged(), 0);
}

#[test]
fn bus_shutdown_is_idempotent() {
    // A synchronous test: requesting shutdown repeatedly must not panic or error.
    let transport = Arc::new(MemoryTransport::new(8).expect("bounded transport"));
    let bus = Bus::builder(transport).build();
    bus.shutdown();
    bus.shutdown();
    assert!(bus.shutdown_token().is_cancelled());
}

// ---------------------------------------------------------------------------
// Error propagation
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn bus_surfaces_redelivery_failure_when_no_dead_letter_policy_is_set() {
    let transport = Arc::new(MemoryTransport::new(16).expect("bounded transport"));
    // No dead-letter policy: a handler failure asks the transport to redeliver.
    let bus = Bus::builder(transport.clone())
        .endpoint(
            "failing",
            Arc::new(AlwaysFail),
            Arc::new(MemoryPackCodec::default()),
            1,
        )
        .expect("endpoint")
        .build();

    let ids = Arc::new(SnowflakeIdGenerator::new(1, SnowflakeLayout::default()).expect("ids"));
    let publisher = TypedTransport::<MemoryTransport, MemoryPackCodec>::new(transport, ids);
    publisher.publish(&Ping(1)).await.expect("publish");

    // The in-memory transport cannot negatively acknowledge, so the failed redelivery
    // request surfaces as an Unsupported error instead of looping forever.
    let error = bus
        .run_until_cancelled()
        .await
        .expect_err("nack is unsupported");
    assert_eq!(error.code(), ErrorCode::Unsupported);
}

#[tokio::test(flavor = "current_thread")]
async fn bus_returns_when_one_of_multiple_endpoints_fails() {
    let transport = Arc::new(MemoryTransport::new(16).expect("bounded transport"));
    let bus = Bus::builder(transport.clone())
        .endpoint(
            "failing",
            Arc::new(AlwaysFail),
            Arc::new(MemoryPackCodec::default()),
            1,
        )
        .expect("failing endpoint")
        .endpoint(
            "idle",
            Arc::new(Counter(Arc::new(AtomicUsize::new(0)))),
            Arc::new(MemoryPackCodec::default()),
            1,
        )
        .expect("idle endpoint")
        .build();

    let ids = Arc::new(SnowflakeIdGenerator::new(1, SnowflakeLayout::default()).expect("ids"));
    TypedTransport::<MemoryTransport, MemoryPackCodec>::new(transport, ids)
        .publish(&Ping(1))
        .await
        .expect("publish");

    let result = tokio::time::timeout(Duration::from_millis(100), bus.run_until_cancelled()).await;
    let error = result
        .expect("an endpoint failure must stop the bus")
        .expect_err("nack is unsupported");
    assert_eq!(error.code(), ErrorCode::Unsupported);
}
