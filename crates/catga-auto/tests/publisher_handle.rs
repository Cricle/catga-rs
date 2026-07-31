//! Tests for publish-from-handler via PublisherHandle late binding.

use std::sync::Arc;

use catga_auto::{Bus, PublisherHandle};
use catga_codec_memorypack::{MemoryPackCodec, MemoryPackable};
use catga_core::{CatgaResult, ErrorCode, Message, TypedDeliveryHandler};
use catga_memory::MemoryTransport;

#[derive(Clone, MemoryPackable)]
struct CreateOrder(u32);
impl Message for CreateOrder {}

#[derive(Clone, MemoryPackable)]
struct OrderCreated(u32);
impl Message for OrderCreated {}

struct OrderHandler {
    publisher: PublisherHandle<MemoryTransport, MemoryPackCodec>,
}

#[async_trait::async_trait]
impl TypedDeliveryHandler<CreateOrder> for OrderHandler {
    async fn handle(&self, cmd: &CreateOrder) -> CatgaResult<()> {
        self.publisher.publish(&OrderCreated(cmd.0)).await
    }
}

struct RecordEvents {
    events: Arc<std::sync::Mutex<Vec<u32>>>,
}

#[async_trait::async_trait]
impl TypedDeliveryHandler<OrderCreated> for RecordEvents {
    async fn handle(&self, event: &OrderCreated) -> CatgaResult<()> {
        self.events.lock().expect("not poisoned").push(event.0);
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn handler_publishes_event_via_handle() {
    let transport = Arc::new(MemoryTransport::new(64).expect("transport"));
    let handle = PublisherHandle::new();
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));

    let (bus, publisher) = Bus::builder(Arc::clone(&transport))
        .routed_endpoint::<CreateOrder, _, _>(
            "commands",
            Arc::new(OrderHandler {
                publisher: handle.clone(),
            }),
            Arc::new(MemoryPackCodec::default()),
            1,
        )
        .expect("command endpoint")
        .routed_endpoint::<OrderCreated, _, _>(
            "events",
            Arc::new(RecordEvents {
                events: Arc::clone(&events),
            }),
            Arc::new(MemoryPackCodec::default()),
            1,
        )
        .expect("event endpoint")
        .build_with_publisher(MemoryPackCodec::default())
        .expect("build");

    handle.bind(publisher);

    handle
        .publish(&CreateOrder(42))
        .await
        .expect("publish command");

    let token = bus.shutdown_token();
    let run = async { bus.run_until_cancelled().await };
    let stop = async {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        token.cancel();
    };
    let (result, _) = tokio::join!(run, stop);
    let runs = result.expect("bus run");

    // commands endpoint received 1 CreateOrder
    assert_eq!(runs[0].received(), 1);
    assert_eq!(runs[0].acknowledged(), 1);
    // events endpoint received 1 OrderCreated (published by the handler)
    assert_eq!(runs[1].received(), 1);
    assert_eq!(runs[1].acknowledged(), 1);

    let recorded = events.lock().expect("not poisoned");
    assert_eq!(*recorded, vec![42]);
}

#[tokio::test(flavor = "current_thread")]
async fn publish_before_bind_returns_unavailable() {
    let handle: PublisherHandle<MemoryTransport, MemoryPackCodec> = PublisherHandle::new();
    let error = handle
        .publish(&CreateOrder(1))
        .await
        .expect_err("should fail before bind");
    assert_eq!(error.code(), ErrorCode::Unavailable);
}

#[tokio::test(flavor = "current_thread")]
async fn handle_is_cloneable_and_shares_binding() {
    let transport = Arc::new(MemoryTransport::new(64).expect("transport"));
    let handle_a = PublisherHandle::<MemoryTransport, MemoryPackCodec>::new();
    let handle_b = handle_a.clone();

    let (_bus, publisher) = Bus::builder(Arc::clone(&transport))
        .routed_endpoint::<CreateOrder, _, _>(
            "commands",
            Arc::new(OrderHandler {
                publisher: handle_a.clone(),
            }),
            Arc::new(MemoryPackCodec::default()),
            1,
        )
        .expect("endpoint")
        .build_with_publisher(MemoryPackCodec::default())
        .expect("build");

    // Binding through one clone activates all clones.
    handle_a.bind(publisher);
    handle_b
        .publish(&CreateOrder(7))
        .await
        .expect("clone should work after bind");
}
