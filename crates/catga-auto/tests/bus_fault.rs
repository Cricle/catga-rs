//! Tests for Bus fault publishing: handler failures emit Fault<M> best-effort.

use std::sync::Arc;

use async_trait::async_trait;
use catga_auto::{Bus, BusFaultPublisher, FaultPublishingHandler};
use catga_codec_memorypack::{MemoryPackCodec, MemoryPackable};
use catga_core::{CatgaError, CatgaResult, ErrorCode, Fault, Message, TypedDeliveryHandler};
use catga_memory::MemoryTransport;
use tokio::sync::Mutex;

#[derive(Clone, MemoryPackable)]
struct Cmd(u32);
impl Message for Cmd {}

struct AlwaysFail;

#[async_trait]
impl TypedDeliveryHandler<Cmd> for AlwaysFail {
    async fn handle(&self, _: &Cmd) -> CatgaResult<()> {
        Err(CatgaError::new(ErrorCode::Validation, "always fails"))
    }
}

struct Succeed;

#[async_trait]
impl TypedDeliveryHandler<Cmd> for Succeed {
    async fn handle(&self, _: &Cmd) -> CatgaResult<()> {
        Ok(())
    }
}

#[derive(Default)]
struct CollectFaults {
    faults: Mutex<Vec<Fault<Cmd>>>,
}

#[async_trait]
impl BusFaultPublisher<Cmd> for CollectFaults {
    async fn publish_fault(&self, fault: Fault<Cmd>) -> CatgaResult<()> {
        self.faults.lock().await.push(fault);
        Ok(())
    }
}

struct FailingPublisher;

#[async_trait]
impl BusFaultPublisher<Cmd> for FailingPublisher {
    async fn publish_fault(&self, _: Fault<Cmd>) -> CatgaResult<()> {
        Err(CatgaError::new(ErrorCode::Internal, "publisher down"))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn fault_emitted_on_handler_failure() {
    let transport = Arc::new(MemoryTransport::new(64).expect("transport"));
    let faults = Arc::new(CollectFaults::default());
    let handler = Arc::new(FaultPublishingHandler::new(
        Arc::new(AlwaysFail),
        Arc::clone(&faults) as Arc<dyn BusFaultPublisher<Cmd>>,
    ));

    let bus = Bus::builder(Arc::clone(&transport))
        .endpoint::<Cmd, _, _>("cmds", handler, Arc::new(MemoryPackCodec::default()), 1)
        .expect("endpoint")
        .build();

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
    publisher.publish(&Cmd(42)).await.expect("publish");

    let token = bus.shutdown_token();
    let run = async { bus.run_until_cancelled().await };
    let stop = async {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        token.cancel();
    };
    let (result, _) = tokio::join!(run, stop);
    // MemoryTransport nack returns Unsupported, so the bus run errors — that's expected.
    let _ = result;

    let collected = faults.faults.lock().await;
    assert_eq!(collected.len(), 1);
    assert_eq!(collected[0].message().0, 42);
    assert_eq!(collected[0].error().code(), ErrorCode::Validation);
}

#[tokio::test(flavor = "current_thread")]
async fn fault_publisher_failure_does_not_mask_handler_error() {
    let transport = Arc::new(MemoryTransport::new(64).expect("transport"));
    let handler = Arc::new(FaultPublishingHandler::new(
        Arc::new(AlwaysFail),
        Arc::new(FailingPublisher) as Arc<dyn BusFaultPublisher<Cmd>>,
    ));

    let bus = Bus::builder(Arc::clone(&transport))
        .endpoint::<Cmd, _, _>("cmds", handler, Arc::new(MemoryPackCodec::default()), 1)
        .expect("endpoint")
        .build();

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
    publisher.publish(&Cmd(7)).await.expect("publish");

    let token = bus.shutdown_token();
    let run = async { bus.run_until_cancelled().await };
    let stop = async {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        token.cancel();
    };
    let (result, _) = tokio::join!(run, stop);
    // Bus should still complete (with nack Unsupported error), not panic.
    let _ = result;
}

#[tokio::test(flavor = "current_thread")]
async fn no_fault_on_successful_handler() {
    let transport = Arc::new(MemoryTransport::new(64).expect("transport"));
    let faults = Arc::new(CollectFaults::default());
    let handler = Arc::new(FaultPublishingHandler::new(
        Arc::new(Succeed),
        Arc::clone(&faults) as Arc<dyn BusFaultPublisher<Cmd>>,
    ));

    let bus = Bus::builder(Arc::clone(&transport))
        .endpoint::<Cmd, _, _>("cmds", handler, Arc::new(MemoryPackCodec::default()), 1)
        .expect("endpoint")
        .build();

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
    publisher.publish(&Cmd(1)).await.expect("publish");

    let token = bus.shutdown_token();
    let run = async { bus.run_until_cancelled().await };
    let stop = async {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        token.cancel();
    };
    let (result, _) = tokio::join!(run, stop);
    result.expect("bus should succeed");

    let collected = faults.faults.lock().await;
    assert!(collected.is_empty());
}
