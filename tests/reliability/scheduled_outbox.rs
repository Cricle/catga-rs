//! Scheduled outbox delivery tests.

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use catga_core::codec::memorypack::{MemoryPackScheduledOutbox, MemoryPackable};
use catga_core::{
    DelayedMessage, DistributedIdGenerator, Envelope, EnvelopeHeaders, Event, Message,
    MessageMetadata, MessagePriority, OutboxMessage, OutboxStore, QualityOfService,
    SnowflakeIdGenerator, SnowflakeLayout, scope_transport_context,
};
use catga_core::memory::MemoryOutbox;

#[derive(MemoryPackable, catga_core::Message)]
struct ShipOrder {
    order_id: u64,
}

#[derive(MemoryPackable, catga_core::Message)]
#[catga(version = 3, priority = high)]
struct VersionedShipOrder {
    order_id: u64,
}

#[derive(MemoryPackable, catga_core::Message)]
struct DeclaredDelayedShipOrder {
    order_id: u64,
    scheduled_at_unix_ms: u64,
}

impl DelayedMessage for DeclaredDelayedShipOrder {
    fn scheduled_at(&self) -> Option<SystemTime> {
        Some(SystemTime::UNIX_EPOCH + Duration::from_millis(self.scheduled_at_unix_ms))
    }

    fn delay(&self) -> Option<Duration> {
        Some(Duration::ZERO)
    }
}

#[derive(Clone, MemoryPackable, catga_core::Message)]
struct OrderScheduled(u64);

impl Event for OrderScheduled {
    type TypeId = catga_core::DefaultMessageTypeId;
}

#[derive(Clone, MemoryPackable, catga_core::Message)]
struct ReliableOrderScheduled(u64);

impl Event for ReliableOrderScheduled {
    type TypeId = catga_core::DefaultMessageTypeId;
}

#[tokio::test]
async fn outbox_claim_skips_messages_until_their_delivery_time() {
    let store = Arc::new(MemoryOutbox::default());
    let message = OutboxMessage::scheduled(
        Envelope::new(17, "orders.ship", vec![7], MessageMetadata::new(17, None)),
        SystemTime::now() + Duration::from_secs(60),
    )
    .expect("future scheduling time is valid");

    assert!(message.not_before().is_some());
    store.enqueue(message).await.expect("enqueue succeeds");
    assert!(
        store
            .claim("worker", 1)
            .await
            .expect("claim succeeds")
            .is_empty()
    );
}

#[tokio::test]
async fn pending_scheduled_message_can_be_cancelled_once() {
    let store = Arc::new(MemoryOutbox::default());
    let message = OutboxMessage::scheduled(
        Envelope::new(18, "orders.ship", vec![8], MessageMetadata::new(18, None)),
        SystemTime::now() + Duration::from_secs(60),
    )
    .expect("future scheduling time is valid");

    store.enqueue(message).await.expect("enqueue succeeds");
    assert!(store.cancel(18).await.expect("cancel succeeds"));
    assert!(!store.cancel(18).await.expect("second cancel succeeds"));
    assert!(
        store
            .claim("worker", 1)
            .await
            .expect("claim succeeds")
            .is_empty()
    );
}

#[tokio::test]
async fn memorypack_scheduler_persists_a_typed_message_until_its_deadline() {
    let store = Arc::new(MemoryOutbox::default());
    let ids: Arc<dyn DistributedIdGenerator> = Arc::new(
        SnowflakeIdGenerator::new(3, SnowflakeLayout::default())
            .expect("valid Snowflake configuration"),
    );
    let scheduler = MemoryPackScheduledOutbox::new(Arc::clone(&store), ids);

    let id = scheduler
        .schedule_at(
            &ShipOrder { order_id: 42 },
            SystemTime::now() + Duration::from_secs(60),
        )
        .await
        .expect("typed future message must be persisted");

    assert!(id > 0);
    assert!(
        store
            .claim("worker", 1)
            .await
            .expect("claim succeeds")
            .is_empty()
    );
    assert!(scheduler.cancel(id).await.expect("cancel succeeds"));
}

#[tokio::test]
async fn memorypack_scheduler_uses_a_message_declared_deadline() {
    let store = Arc::new(MemoryOutbox::default());
    let ids: Arc<dyn DistributedIdGenerator> = Arc::new(
        SnowflakeIdGenerator::new(3, SnowflakeLayout::default())
            .expect("valid Snowflake configuration"),
    );
    let scheduler = MemoryPackScheduledOutbox::new(Arc::clone(&store), ids);

    scheduler
        .schedule_delayed(&DeclaredDelayedShipOrder {
            order_id: 51,
            scheduled_at_unix_ms: u64::try_from(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("test clock is after the Unix epoch")
                    .as_millis(),
            )
            .expect("test deadline fits u64 milliseconds")
                + 60_000,
        })
        .await
        .expect("message declared delay is persisted durably");

    assert!(
        store
            .claim("worker", 1)
            .await
            .expect("claim succeeds")
            .is_empty()
    );
}

#[tokio::test]
async fn memorypack_scheduler_persists_the_derived_schema_version() {
    let store = Arc::new(MemoryOutbox::default());
    let ids: Arc<dyn DistributedIdGenerator> = Arc::new(
        SnowflakeIdGenerator::new(3, SnowflakeLayout::default())
            .expect("valid Snowflake configuration"),
    );
    let scheduler = MemoryPackScheduledOutbox::new(Arc::clone(&store), ids);

    scheduler
        .schedule_at(
            &VersionedShipOrder { order_id: 73 },
            SystemTime::now() - Duration::from_secs(1),
        )
        .await
        .expect("versioned message is scheduled");

    let claimed = store
        .claim("worker", 1)
        .await
        .expect("due versioned message is claimed");

    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].envelope().schema_version(), 3);
    assert_eq!(
        claimed[0].envelope().metadata().priority(),
        MessagePriority::High
    );
}

#[tokio::test]
async fn memorypack_scheduler_inherits_scoped_transport_headers_and_priority() {
    let store = Arc::new(MemoryOutbox::default());
    let ids: Arc<dyn DistributedIdGenerator> = Arc::new(
        SnowflakeIdGenerator::new(3, SnowflakeLayout::default())
            .expect("valid Snowflake configuration"),
    );
    let scheduler = MemoryPackScheduledOutbox::new(Arc::clone(&store), ids);
    let inbound = Envelope::new(
        81,
        "orders.received",
        vec![],
        MessageMetadata::new(81, Some(51)).with_priority(MessagePriority::High),
    )
    .with_headers(EnvelopeHeaders::try_new([("tenant", "blue")]).expect("valid inbound headers"));

    scope_transport_context(
        &inbound,
        scheduler.schedule_at(
            &ShipOrder { order_id: 74 },
            SystemTime::now() - Duration::from_secs(1),
        ),
    )
    .await
    .expect("scoped message is scheduled");

    let claimed = store
        .claim("worker", 1)
        .await
        .expect("due scoped message is claimed");

    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].envelope().metadata().correlation_id(), Some(51));
    assert_eq!(
        claimed[0].envelope().metadata().priority(),
        MessagePriority::High
    );
    assert_eq!(claimed[0].envelope().header("tenant"), Some("blue"));
}

#[tokio::test]
async fn memorypack_scheduler_preserves_event_delivery_defaults() {
    let store = Arc::new(MemoryOutbox::default());
    let ids: Arc<dyn DistributedIdGenerator> = Arc::new(
        SnowflakeIdGenerator::new(3, SnowflakeLayout::default())
            .expect("valid Snowflake configuration"),
    );
    let scheduler = MemoryPackScheduledOutbox::new(Arc::clone(&store), ids);
    let due = SystemTime::now() - Duration::from_secs(1);

    scheduler
        .schedule_event_at(&OrderScheduled(31), due)
        .await
        .expect("event is scheduled");
    scheduler
        .schedule_reliable_event_at(&ReliableOrderScheduled(32), due)
        .await
        .expect("reliable event is scheduled");

    let claimed = store
        .claim("worker", 2)
        .await
        .expect("due events are claimed");
    assert_eq!(claimed.len(), 2);
    assert!(claimed.iter().any(|message| {
        message.envelope().metadata().quality_of_service() == QualityOfService::AtMostOnce
    }));
    assert!(claimed.iter().any(|message| {
        message.envelope().metadata().quality_of_service() == QualityOfService::AtLeastOnce
    }));
}
