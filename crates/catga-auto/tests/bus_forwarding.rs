//! Tests for message forwarding between destinations.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use catga_auto::MessageForwarder;
use catga_codec_memorypack::{MemoryPackCodec, MemoryPackable};
use catga_core::{
    Acknowledger, CatgaError, CatgaResult, Delivery, Destination, DestinationTransport, Envelope,
    ErrorCode, Message, MessageTransport,
};
use catga_memory::MemoryTransport;

#[derive(Clone, MemoryPackable)]
struct Archived(u32);
impl Message for Archived {}

struct CountAcknowledgements(Arc<AtomicUsize>);

#[async_trait]
impl Acknowledger for CountAcknowledgements {
    async fn acknowledge(self: Box<Self>) -> CatgaResult<()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct TargetFailureTransport {
    source: Mutex<Option<Envelope>>,
    acknowledgements: Arc<AtomicUsize>,
}

#[async_trait]
impl MessageTransport for TargetFailureTransport {
    async fn publish(&self, _: Envelope) -> CatgaResult<()> {
        Err(CatgaError::new(
            ErrorCode::Unsupported,
            "not used by this test",
        ))
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        Err(CatgaError::new(
            ErrorCode::Unsupported,
            "not used by this test",
        ))
    }
}

#[async_trait]
impl DestinationTransport for TargetFailureTransport {
    async fn send_to(&self, _: &Destination, _: Envelope) -> CatgaResult<()> {
        Err(CatgaError::new(
            ErrorCode::Unavailable,
            "target is unavailable",
        ))
    }

    async fn receive_from(&self, _: &Destination) -> CatgaResult<Delivery> {
        let envelope = self
            .source
            .lock()
            .expect("source lock")
            .take()
            .ok_or_else(|| CatgaError::new(ErrorCode::NotFound, "source is empty"))?;
        Ok(Delivery::with_acknowledger(
            envelope,
            Box::new(CountAcknowledgements(Arc::clone(&self.acknowledgements))),
        ))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn forwarder_moves_messages_between_destinations() {
    let transport = Arc::new(MemoryTransport::new(64).expect("transport"));
    let source = Destination::parse("source").expect("valid");
    let target = Destination::parse("target").expect("valid");
    transport
        .declare_destination(source.clone())
        .expect("declare");
    transport
        .declare_destination(target.clone())
        .expect("declare");

    // Publish 3 messages to source.
    let publisher = {
        let ids = Arc::new(
            catga_core::SnowflakeIdGenerator::new(1, catga_core::SnowflakeLayout::default())
                .expect("ids"),
        );
        catga_core::TypedTransport::<MemoryTransport, MemoryPackCodec>::new(
            Arc::clone(&transport),
            ids,
        )
    };
    for i in 0..3 {
        publisher
            .send_to("source", &Archived(i))
            .await
            .expect("send");
    }

    // Forward all from source → target.
    let forwarder = MessageForwarder::new(Arc::clone(&transport));
    let count = forwarder
        .forward(&source, &target, 10)
        .await
        .expect("forward");
    assert_eq!(count, 3);

    // Verify messages arrived at target.
    for _ in 0..3 {
        let delivery = transport.receive_from(&target).await.expect("receive");
        transport.ack(delivery).await.expect("ack");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn forwarder_stops_at_max_count() {
    let transport = Arc::new(MemoryTransport::new(64).expect("transport"));
    let source = Destination::parse("src2").expect("valid");
    let target = Destination::parse("tgt2").expect("valid");
    transport
        .declare_destination(source.clone())
        .expect("declare");
    transport
        .declare_destination(target.clone())
        .expect("declare");

    let publisher = {
        let ids = Arc::new(
            catga_core::SnowflakeIdGenerator::new(1, catga_core::SnowflakeLayout::default())
                .expect("ids"),
        );
        catga_core::TypedTransport::<MemoryTransport, MemoryPackCodec>::new(
            Arc::clone(&transport),
            ids,
        )
    };
    for i in 0..5 {
        publisher.send_to("src2", &Archived(i)).await.expect("send");
    }

    let forwarder = MessageForwarder::new(Arc::clone(&transport));
    let count = forwarder
        .forward(&source, &target, 2)
        .await
        .expect("forward");
    assert_eq!(count, 2);
}

#[tokio::test(flavor = "current_thread")]
async fn forwarder_does_not_ack_source_when_target_send_fails() {
    let acknowledgements = Arc::new(AtomicUsize::new(0));
    let transport = Arc::new(TargetFailureTransport {
        source: Mutex::new(Some(Envelope::new(
            1,
            "archived",
            Vec::new(),
            catga_core::MessageMetadata::new(1, Some(1)),
        ))),
        acknowledgements: Arc::clone(&acknowledgements),
    });
    let source = Destination::parse("source").expect("valid");
    let target = Destination::parse("target").expect("valid");

    let error = MessageForwarder::new(transport)
        .forward(&source, &target, 1)
        .await
        .expect_err("target failure must be returned");

    assert_eq!(error.code(), ErrorCode::Unavailable);
    assert_eq!(acknowledgements.load(Ordering::SeqCst), 0);
}
