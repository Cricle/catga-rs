//! Strict tests for Bus utilities: FilteredHandler, scheduling, edge cases, ordering.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use catga_auto::{Bus, FilteredHandler, PublisherHandle};
use catga_codec_memorypack::{MemoryPackCodec, MemoryPackable};
use catga_core::{CatgaResult, Destination, Message, TypedDeliveryHandler};
use catga_memory::MemoryTransport;

#[derive(Clone, MemoryPackable)]
struct Task {
    id: u32,
    priority: u8,
}
impl Message for Task {}

struct RecordTasks {
    ids: Arc<std::sync::Mutex<Vec<u32>>>,
}

#[async_trait]
impl TypedDeliveryHandler<Task> for RecordTasks {
    async fn handle(&self, task: &Task) -> CatgaResult<()> {
        self.ids.lock().expect("not poisoned").push(task.id);
        Ok(())
    }
}

// --- FilteredHandler tests ---

#[tokio::test(flavor = "current_thread")]
async fn filtered_handler_only_processes_matching_messages() {
    let transport = Arc::new(MemoryTransport::new(64).expect("transport"));
    let ids = Arc::new(std::sync::Mutex::new(Vec::new()));
    let inner = Arc::new(RecordTasks {
        ids: Arc::clone(&ids),
    });
    // Only process high-priority tasks (priority >= 5).
    let handler = Arc::new(FilteredHandler::new(inner, |t: &Task| t.priority >= 5));

    let bus = Bus::builder(Arc::clone(&transport))
        .endpoint::<Task, _, _>("tasks", handler, Arc::new(MemoryPackCodec::default()), 1)
        .expect("endpoint")
        .build();

    let publisher = {
        let id_gen = Arc::new(
            catga_core::SnowflakeIdGenerator::new(1, catga_core::SnowflakeLayout::default())
                .expect("ids"),
        );
        catga_core::TypedTransport::<MemoryTransport, MemoryPackCodec>::new(
            Arc::clone(&transport),
            id_gen,
        )
    };
    // Mix of low and high priority.
    publisher
        .publish(&Task { id: 1, priority: 2 })
        .await
        .expect("p1");
    publisher
        .publish(&Task { id: 2, priority: 7 })
        .await
        .expect("p2");
    publisher
        .publish(&Task { id: 3, priority: 1 })
        .await
        .expect("p3");
    publisher
        .publish(&Task { id: 4, priority: 9 })
        .await
        .expect("p4");

    let token = bus.shutdown_token();
    let run = async { bus.run_until_cancelled().await };
    let stop = async {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        token.cancel();
    };
    let (result, _) = tokio::join!(run, stop);
    let runs = result.expect("bus");
    // All 4 messages are acknowledged (filtered ones are acked as no-ops).
    assert_eq!(runs[0].acknowledged(), 4);
    // But only high-priority ones were processed.
    let processed = ids.lock().expect("lock");
    assert_eq!(*processed, vec![2, 4]);
}

#[tokio::test(flavor = "current_thread")]
async fn filtered_handler_rejects_all_when_predicate_always_false() {
    let transport = Arc::new(MemoryTransport::new(64).expect("transport"));
    let count = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&count);

    struct Counter(Arc<AtomicUsize>);
    #[async_trait]
    impl TypedDeliveryHandler<Task> for Counter {
        async fn handle(&self, _: &Task) -> CatgaResult<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let handler = Arc::new(FilteredHandler::new(
        Arc::new(Counter(counter)),
        |_: &Task| false,
    ));

    let bus = Bus::builder(Arc::clone(&transport))
        .endpoint::<Task, _, _>("tasks", handler, Arc::new(MemoryPackCodec::default()), 1)
        .expect("endpoint")
        .build();

    let publisher = {
        let id_gen = Arc::new(
            catga_core::SnowflakeIdGenerator::new(1, catga_core::SnowflakeLayout::default())
                .expect("ids"),
        );
        catga_core::TypedTransport::<MemoryTransport, MemoryPackCodec>::new(
            Arc::clone(&transport),
            id_gen,
        )
    };
    publisher
        .publish(&Task { id: 1, priority: 5 })
        .await
        .expect("p");

    let token = bus.shutdown_token();
    let run = async { bus.run_until_cancelled().await };
    let stop = async {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        token.cancel();
    };
    let (result, _) = tokio::join!(run, stop);
    result.expect("bus");
    assert_eq!(count.load(Ordering::SeqCst), 0);
}

// --- Scheduling tests ---

#[tokio::test(flavor = "current_thread")]
async fn schedule_delivers_after_delay() {
    let transport = Arc::new(MemoryTransport::new(64).expect("transport"));
    let handle = PublisherHandle::new();

    let (bus, publisher) = Bus::builder(Arc::clone(&transport))
        .routed_endpoint::<Task, _, _>(
            "tasks",
            Arc::new(RecordTasks {
                ids: Arc::new(std::sync::Mutex::new(Vec::new())),
            }),
            Arc::new(MemoryPackCodec::default()),
            1,
        )
        .expect("endpoint")
        .build_with_publisher(MemoryPackCodec::default())
        .expect("build");

    handle.bind(publisher);

    let token = bus.shutdown_token();
    let run = async { bus.run_until_cancelled().await };
    let driver = async {
        let before = std::time::Instant::now();
        handle
            .schedule(
                &Task {
                    id: 99,
                    priority: 1,
                },
                std::time::Duration::from_millis(80),
            )
            .await
            .expect("schedule");
        let elapsed = before.elapsed();
        assert!(
            elapsed >= std::time::Duration::from_millis(70),
            "schedule should wait at least ~80ms, got {elapsed:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        token.cancel();
    };
    let (result, _) = tokio::join!(run, driver);
    let runs = result.expect("bus");
    assert_eq!(runs[0].acknowledged(), 1);
}

// --- Ordering guarantee ---

#[tokio::test(flavor = "current_thread")]
async fn routed_endpoint_preserves_publish_order() {
    let transport = Arc::new(MemoryTransport::new(64).expect("transport"));
    let ids = Arc::new(std::sync::Mutex::new(Vec::new()));

    let (bus, publisher) = Bus::builder(Arc::clone(&transport))
        .routed_endpoint::<Task, _, _>(
            "tasks",
            Arc::new(RecordTasks {
                ids: Arc::clone(&ids),
            }),
            Arc::new(MemoryPackCodec::default()),
            1, // concurrency=1 guarantees order
        )
        .expect("endpoint")
        .build_with_publisher(MemoryPackCodec::default())
        .expect("build");

    let token = bus.shutdown_token();
    let run = async { bus.run_until_cancelled().await };
    let driver = async {
        for i in 0..10 {
            publisher
                .publish(&Task { id: i, priority: 0 })
                .await
                .expect("publish");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        token.cancel();
    };
    let (result, _) = tokio::join!(run, driver);
    let runs = result.expect("bus");
    assert_eq!(runs[0].acknowledged(), 10);

    let ordered = ids.lock().expect("lock");
    assert_eq!(*ordered, (0..10).collect::<Vec<_>>());
}

// --- Edge cases ---

#[tokio::test(flavor = "current_thread")]
async fn publish_to_unregistered_type_returns_not_found() {
    let transport = Arc::new(MemoryTransport::new(64).expect("transport"));

    #[derive(Clone, MemoryPackable)]
    struct Unknown(u32);
    impl Message for Unknown {}

    let (_bus, publisher) = Bus::builder(Arc::clone(&transport))
        .routed_endpoint::<Task, _, _>(
            "tasks",
            Arc::new(RecordTasks {
                ids: Arc::new(std::sync::Mutex::new(Vec::new())),
            }),
            Arc::new(MemoryPackCodec::default()),
            1,
        )
        .expect("endpoint")
        .build_with_publisher(MemoryPackCodec::default())
        .expect("build");

    let error = publisher
        .publish(&Unknown(1))
        .await
        .expect_err("should fail");
    assert_eq!(error.code(), catga_core::ErrorCode::NotFound);
}

#[tokio::test(flavor = "current_thread")]
async fn forwarder_on_empty_queue_returns_zero() {
    let transport = Arc::new(MemoryTransport::new(64).expect("transport"));
    let source = Destination::parse("empty-src").expect("valid");
    let target = Destination::parse("empty-tgt").expect("valid");
    transport
        .declare_destination(source.clone())
        .expect("declare");
    transport
        .declare_destination(target.clone())
        .expect("declare");

    let forwarder = catga_auto::MessageForwarder::new(transport);
    let count = forwarder
        .forward(&source, &target, 100)
        .await
        .expect("forward");
    assert_eq!(count, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn duplicate_routed_endpoint_same_type_fails() {
    let transport = Arc::new(MemoryTransport::new(64).expect("transport"));
    let result = Bus::builder(transport)
        .routed_endpoint::<Task, _, _>(
            "first",
            Arc::new(RecordTasks {
                ids: Arc::new(std::sync::Mutex::new(Vec::new())),
            }),
            Arc::new(MemoryPackCodec::default()),
            1,
        )
        .expect("first")
        .routed_endpoint::<Task, _, _>(
            "second",
            Arc::new(RecordTasks {
                ids: Arc::new(std::sync::Mutex::new(Vec::new())),
            }),
            Arc::new(MemoryPackCodec::default()),
            1,
        );
    assert!(result.is_err());
}
