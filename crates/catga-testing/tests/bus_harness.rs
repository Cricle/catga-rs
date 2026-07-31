//! Tests for the in-process [`BusTestHarness`].

use std::time::Duration;

use async_trait::async_trait;
use catga_codec_memorypack::MemoryPackable;
use catga_core::{CatgaError, CatgaResult, ErrorCode, Message, TypedDeliveryHandler};
use catga_testing::BusTestHarness;

#[derive(Clone, MemoryPackable)]
struct Ping(u32);
impl Message for Ping {}

#[derive(Clone, MemoryPackable)]
struct Tick(u32);
impl Message for Tick {}

struct Accept;

#[async_trait]
impl TypedDeliveryHandler<Ping> for Accept {
    async fn handle(&self, _: &Ping) -> CatgaResult<()> {
        Ok(())
    }
}

#[async_trait]
impl TypedDeliveryHandler<Tick> for Accept {
    async fn handle(&self, _: &Tick) -> CatgaResult<()> {
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn harness_consumes_a_published_message() {
    let mut harness = BusTestHarness::new().expect("harness");
    harness
        .endpoint::<Ping, _>("pings", Accept)
        .expect("endpoint");
    let harness = harness.start();

    harness.publish(&Ping(1)).await.expect("publish");
    harness
        .run_until_consumed::<Ping>(1)
        .await
        .expect("consumed");

    assert_eq!(harness.consumed_count::<Ping>(), 1);
    assert_eq!(harness.consumed::<Ping>().len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn harness_records_consumed_values_in_order() {
    let mut harness = BusTestHarness::new().expect("harness");
    harness
        .endpoint::<Ping, _>("pings", Accept)
        .expect("endpoint");
    let harness = harness.start();

    for i in 0..3 {
        harness.publish(&Ping(i)).await.expect("publish");
    }
    harness
        .run_until_consumed::<Ping>(3)
        .await
        .expect("consumed");

    let values: Vec<u32> = harness
        .consumed::<Ping>()
        .into_iter()
        .map(|ping| ping.0)
        .collect();
    assert_eq!(values, vec![0, 1, 2]);
}

#[tokio::test(flavor = "current_thread")]
async fn harness_counts_each_message_type_separately() {
    let mut harness = BusTestHarness::new().expect("harness");
    harness
        .endpoint::<Ping, _>("pings", Accept)
        .expect("ping endpoint");
    harness
        .endpoint::<Tick, _>("ticks", Accept)
        .expect("tick endpoint");
    let harness = harness.start();

    harness.publish(&Ping(1)).await.expect("publish ping");
    harness.publish(&Tick(1)).await.expect("publish tick");
    harness.publish(&Tick(2)).await.expect("publish tick");

    harness
        .run_until(|log| log.count_of::<Ping>() >= 1 && log.count_of::<Tick>() >= 2)
        .await
        .expect("consumed");

    assert_eq!(harness.consumed_count::<Ping>(), 1);
    assert_eq!(harness.consumed_count::<Tick>(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn harness_times_out_when_nothing_is_consumed() {
    let mut harness = BusTestHarness::new()
        .expect("harness")
        .with_condition_timeout(Duration::from_millis(150));
    // Only a Ping consumer is registered; Tick will never be consumed.
    harness
        .endpoint::<Ping, _>("pings", Accept)
        .expect("endpoint");
    let harness = harness.start();

    let error = harness
        .run_until_consumed::<Tick>(1)
        .await
        .expect_err("should time out");
    assert_eq!(error.code(), ErrorCode::Timeout);
}

#[tokio::test(flavor = "current_thread")]
async fn harness_reports_a_handler_failure_without_recording_it() {
    struct Reject;
    #[async_trait]
    impl TypedDeliveryHandler<Ping> for Reject {
        async fn handle(&self, _: &Ping) -> CatgaResult<()> {
            Err(CatgaError::new(ErrorCode::HandlerFailed, "nope"))
        }
    }

    let mut harness = BusTestHarness::new()
        .expect("harness")
        .with_condition_timeout(Duration::from_millis(150));
    harness
        .endpoint::<Ping, _>("pings", Reject)
        .expect("endpoint");
    let harness = harness.start();

    harness.publish(&Ping(1)).await.expect("publish");
    // The handler always fails, so nothing is recorded as consumed and the wait times out.
    let error = harness
        .run_until_consumed::<Ping>(1)
        .await
        .expect_err("should time out");
    assert_eq!(error.code(), ErrorCode::Timeout);
    assert_eq!(harness.consumed_count::<Ping>(), 0);
}
