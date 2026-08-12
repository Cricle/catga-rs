//! Service-backed contract coverage for [`RedisTransport`] and [`RedisPubSubTransport`].

use std::time::Duration;

use catga_core::codec::memorypack::MemoryPackCodec;
use catga_core::{
    AsyncInitializable, CatgaError, CatgaResult, Destination, DestinationTransport, EnvelopeCodec,
    ErrorCode, HealthCheckable, MessageMetadata, MessageTransport, QualityOfService, Stoppable,
    Waitable,
};
use catga_redis::{
    RedisConfig, RedisPendingReclaimOptions, RedisPubSubConfig, RedisPubSubTransport,
    RedisTransport,
};
use redis::AsyncCommands;
use tokio_util::sync::CancellationToken;

#[path = "support/envelopes.rs"]
mod envelopes;
#[path = "support/raw.rs"]
mod raw;
#[path = "support/redis_err.rs"]
mod redis_err;
#[path = "support/service_url.rs"]
mod service_url;
#[path = "support/timeout_err.rs"]
mod timeout_err;

use envelopes::envelope;
use raw::raw_connection;
use redis_err::map_redis_error;
use timeout_err::timeout_error;

fn stream_config(url: &str, label: &str, consumer: &str) -> RedisConfig {
    RedisConfig {
        server: url.into(),
        stream: format!("catga-test-stream-{label}").into(),
        group: format!("catga-test-group-{label}").into(),
        consumer: consumer.into(),
    }
}

async fn bounded_receive(
    transport: &(impl MessageTransport + ?Sized),
    millis: u64,
) -> CatgaResult<catga_core::Delivery> {
    tokio::time::timeout(Duration::from_millis(millis), transport.receive())
        .await
        .map_err(|_| timeout_error("Redis receive timed out"))?
}

// ==================== streams transport ====================

#[tokio::test]
async fn transport_publishes_receives_and_acknowledges() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let label = uuid::Uuid::new_v4().to_string();
    let transport = RedisTransport::connect(stream_config(&url, &label, "worker-a")).await?;

    // Lifecycle surface: healthy, initializable, and accepting.
    assert!(transport.is_healthy());
    assert_eq!(transport.health_status(), Some("Redis transport is ready"));
    transport.initialize().await?;
    assert!(transport.is_accepting());
    assert_eq!(transport.pending_operations(), 0);

    let outbound = envelope(101, "catga.test.stream");
    transport.publish(outbound.clone()).await?;

    let delivery = bounded_receive(&transport, 5_000).await?;
    assert_eq!(delivery.envelope(), &outbound);
    assert_eq!(delivery.attempts(), 1);
    assert_eq!(transport.pending_operations(), 1);

    transport.ack(delivery).await?;
    assert_eq!(transport.pending_operations(), 0);
    transport
        .wait_for_completion(CancellationToken::new())
        .await?;
    Ok(())
}

#[tokio::test]
async fn transport_redelivers_a_nacked_delivery_from_owned_pending() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let label = uuid::Uuid::new_v4().to_string();
    let transport = RedisTransport::connect(stream_config(&url, &label, "worker-a")).await?;

    transport
        .publish(envelope(102, "catga.test.stream"))
        .await?;
    let first = bounded_receive(&transport, 5_000).await?;
    assert_eq!(first.attempts(), 1);

    // A negative acknowledgement keeps the entry pending for this consumer.
    transport.nack(first).await?;
    let second = bounded_receive(&transport, 5_000).await?;
    assert_eq!(second.envelope().id(), 102);
    assert!(second.attempts() >= 2);
    transport.ack(second).await?;
    Ok(())
}

#[tokio::test]
async fn transport_drop_releases_in_flight_without_acknowledging() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let label = uuid::Uuid::new_v4().to_string();
    let transport = RedisTransport::connect(stream_config(&url, &label, "worker-a")).await?;

    transport
        .publish(envelope(103, "catga.test.stream"))
        .await?;
    let dropped = bounded_receive(&transport, 5_000).await?;
    let envelope_id = dropped.envelope().id();
    drop(dropped);

    // The dropped delivery stays pending and is redelivered to its owner.
    let redelivered = bounded_receive(&transport, 5_000).await?;
    assert_eq!(redelivered.envelope().id(), envelope_id);
    transport.ack(redelivered).await?;
    Ok(())
}

#[tokio::test]
async fn transport_concurrent_receive_skips_owned_pending_and_reclaims_nothing() -> CatgaResult<()>
{
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let label = uuid::Uuid::new_v4().to_string();
    let transport = RedisTransport::connect(stream_config(&url, &label, "worker-a")).await?;

    transport.publish(envelope(111, "catga.test.first")).await?;
    let held = bounded_receive(&transport, 5_000).await?;

    // While one delivery is held in flight, a second receive skips the owned-pending
    // read, finds only its own pending entry during reclaim, and blocks on ">".
    transport
        .publish(envelope(112, "catga.test.second"))
        .await?;
    let second = bounded_receive(&transport, 5_000).await?;
    assert_eq!(second.envelope().id(), 112);

    transport.ack(held).await?;
    transport.ack(second).await?;
    Ok(())
}

#[tokio::test]
async fn transport_reclaims_idle_entries_for_another_consumer() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let label = uuid::Uuid::new_v4().to_string();
    let reclaim = RedisPendingReclaimOptions::new(Duration::from_millis(1), 4)?;
    let stalled = RedisTransport::connect_with_reclaim_options(
        stream_config(&url, &label, "worker-stalled"),
        reclaim.clone(),
    )
    .await?;
    let recoverer = RedisTransport::connect_with_reclaim_options(
        stream_config(&url, &label, "worker-recoverer"),
        reclaim,
    )
    .await?;

    stalled.publish(envelope(121, "catga.test.reclaim")).await?;
    let abandoned = bounded_receive(&stalled, 5_000).await?;
    assert_eq!(abandoned.attempts(), 1);

    // Once the entry idles past the reclaim floor, the other consumer claims it.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let reclaimed = bounded_receive(&recoverer, 5_000).await?;
    assert_eq!(reclaimed.envelope().id(), 121);
    assert!(reclaimed.attempts() >= 2);
    recoverer.ack(reclaimed).await?;

    // The stalled consumer's acknowledgement now fails: ownership moved away.
    let stale_ack = abandoned.acknowledge().await;
    assert!(matches!(stale_ack, Err(error) if error.code() == ErrorCode::Transient));
    Ok(())
}

#[tokio::test]
async fn transport_rejects_a_wrong_type_stream_on_connect() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let label = uuid::Uuid::new_v4().to_string();
    let config = stream_config(&url, &label, "worker-a");
    let mut raw = raw_connection(&url).await?;

    // A plain string where the stream belongs fails group provisioning.
    let _: () = raw
        .set(config.stream.as_ref(), "a-plain-string")
        .await
        .map_err(map_redis_error)?;
    let result = RedisTransport::connect(config).await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Transient));
    Ok(())
}

#[tokio::test]
async fn transport_receive_from_rejects_a_wrong_type_destination_stream() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let label = uuid::Uuid::new_v4().to_string();
    let transport = RedisTransport::connect(stream_config(&url, &label, "worker-a")).await?;
    let destination = Destination::parse(format!("catga-test-wrong-type-{label}"))?;
    let mut raw = raw_connection(&url).await?;

    // A plain string where the destination stream belongs fails group provisioning.
    let _: () = raw
        .set(format!("stream:{destination}"), "a-plain-string")
        .await
        .map_err(map_redis_error)?;
    let result = transport.receive_from(&destination).await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Transient));
    Ok(())
}

#[tokio::test]
async fn transport_leaves_busy_foreign_pending_entries_unclaimed() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let label = uuid::Uuid::new_v4().to_string();
    let owner = RedisTransport::connect(stream_config(&url, &label, "worker-owner")).await?;
    // One scan with a high idle floor: the foreign entry is seen but never claimable,
    // so the scan budget runs out and the receive loop falls back to blocking reads.
    let waiter = std::sync::Arc::new(
        RedisTransport::connect_with_reclaim_options(
            stream_config(&url, &label, "worker-waiter"),
            RedisPendingReclaimOptions::new(Duration::from_secs(60), 1)?,
        )
        .await?,
    );

    owner.publish(envelope(141, "catga.test.busy")).await?;
    let held = bounded_receive(&owner, 5_000).await?;

    let waiting = {
        let waiter = std::sync::Arc::clone(&waiter);
        tokio::spawn(async move { bounded_receive(&*waiter, 5_000).await })
    };
    // Outlast one blocking-read poll so the receive loop repeats its recovery pass.
    tokio::time::sleep(Duration::from_millis(1_300)).await;
    owner.publish(envelope(142, "catga.test.fresh")).await?;

    let delivery = waiting
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))??;
    assert_eq!(delivery.envelope().id(), 142);
    waiter.ack(delivery).await?;
    owner.ack(held).await?;
    Ok(())
}

#[tokio::test]
async fn transport_concurrent_receivers_share_the_recovery_gate() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let label = uuid::Uuid::new_v4().to_string();
    let transport = RedisTransport::connect(stream_config(&url, &label, "worker-a")).await?;

    for id in [151, 152, 153, 154] {
        transport.publish(envelope(id, "catga.test.race")).await?;
    }

    // Concurrent receivers race for one recovery gate; the losers skip recovery and
    // wait on the blocking read instead.
    let (first, second, third, fourth) = tokio::join!(
        bounded_receive(&transport, 5_000),
        bounded_receive(&transport, 5_000),
        bounded_receive(&transport, 5_000),
        bounded_receive(&transport, 5_000),
    );
    let deliveries = vec![first?, second?, third?, fourth?];
    let mut ids: Vec<u64> = deliveries
        .iter()
        .map(|delivery| delivery.envelope().id())
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![151, 152, 153, 154]);
    for delivery in deliveries {
        transport.ack(delivery).await?;
    }
    Ok(())
}

#[tokio::test]
async fn transport_send_to_and_receive_from_a_destination_stream() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let label = uuid::Uuid::new_v4().to_string();
    let transport = RedisTransport::connect(stream_config(&url, &label, "worker-a")).await?;

    let destination = Destination::parse(format!("catga-test-destination-{label}"))?;
    let outbound = envelope(131, "catga.test.destination");
    transport.send_to(&destination, outbound.clone()).await?;

    let delivery = bounded_receive_from(&transport, &destination, 5_000).await?;
    assert_eq!(delivery.envelope(), &outbound);
    transport.ack(delivery).await?;

    // A second receive re-enters group provisioning through the BUSYGROUP path.
    transport
        .send_to(&destination, envelope(132, "catga.test.destination"))
        .await?;
    let again = bounded_receive_from(&transport, &destination, 5_000).await?;
    assert_eq!(again.envelope().id(), 132);
    transport.ack(again).await?;
    Ok(())
}

async fn bounded_receive_from(
    transport: &(impl DestinationTransport + ?Sized),
    destination: &Destination,
    millis: u64,
) -> CatgaResult<catga_core::Delivery> {
    tokio::time::timeout(
        Duration::from_millis(millis),
        transport.receive_from(destination),
    )
    .await
    .map_err(|_| timeout_error("Redis destination receive timed out"))?
}

#[tokio::test]
async fn transport_stop_accepting_blocks_new_publications() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let label = uuid::Uuid::new_v4().to_string();
    let transport = RedisTransport::connect(stream_config(&url, &label, "worker-a")).await?;

    transport.stop_accepting();
    assert!(!transport.is_accepting());

    let publish = transport.publish(envelope(141, "catga.test.stopped")).await;
    assert!(publish.is_err());

    let destination = Destination::parse(format!("catga-test-destination-{label}"))?;
    let send = transport
        .send_to(&destination, envelope(142, "catga.test.stopped"))
        .await;
    assert!(send.is_err());
    Ok(())
}

#[tokio::test]
async fn transport_receive_rejects_entries_without_a_payload() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let label = uuid::Uuid::new_v4().to_string();
    let config = stream_config(&url, &label, "worker-a");
    let transport = RedisTransport::connect(config.clone()).await?;
    let mut raw = raw_connection(&url).await?;

    // An entry with foreign fields reaches receive without a payload field.
    let _: String = raw
        .xadd(
            config.stream.as_ref(),
            "*",
            &[("not-payload", b"x".to_vec())],
        )
        .await
        .map_err(map_redis_error)?;

    let result = bounded_receive(&transport, 5_000).await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Internal));

    // An entry whose payload is not a valid envelope frame fails decoding.
    let _: String = raw
        .xadd(
            config.stream.as_ref(),
            "*",
            &[("payload", b"not-a-frame".to_vec())],
        )
        .await
        .map_err(map_redis_error)?;
    assert!(bounded_receive(&transport, 5_000).await.is_err());
    Ok(())
}

#[tokio::test]
async fn transport_from_client_and_group_provisioning_are_idempotent() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let label = uuid::Uuid::new_v4().to_string();
    let client = redis::Client::open(url.as_str()).map_err(map_redis_error)?;

    let first =
        RedisTransport::from_client(client.clone(), stream_config(&url, &label, "worker-a"))
            .await?;
    // Re-provisioning the same stream and group tolerates BUSYGROUP.
    let second = RedisTransport::connect_with_client(
        client,
        stream_config(&url, &label, "worker-b"),
        RedisPendingReclaimOptions::default(),
    )
    .await?;

    first.publish(envelope(151, "catga.test.stream")).await?;
    let delivery = bounded_receive(&second, 5_000).await?;
    assert_eq!(delivery.envelope().id(), 151);
    second.ack(delivery).await?;
    Ok(())
}

// ==================== pub/sub transport ====================

async fn pubsub_transport(url: &str, channel: &str) -> CatgaResult<RedisPubSubTransport> {
    RedisPubSubTransport::connect(RedisPubSubConfig {
        server: url.into(),
        channel: channel.into(),
    })
    .await
}

#[tokio::test]
async fn pubsub_lifecycle_and_broadcast_roundtrip() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let channel = format!("catga-test-pubsub-{}", uuid::Uuid::new_v4());
    let transport = pubsub_transport(&url, &channel).await?;

    // Lifecycle surface.
    assert!(transport.is_healthy());
    assert_eq!(
        transport.health_status(),
        Some("Redis Pub/Sub transport is ready")
    );
    transport.initialize().await?;
    assert!(transport.is_accepting());
    assert_eq!(transport.pending_operations(), 0);
    transport
        .wait_for_completion(CancellationToken::new())
        .await?;

    let outbound = envelope(201, "catga.test.pubsub");
    transport.publish(outbound.clone()).await?;
    let delivery = bounded_receive(&transport, 5_000).await?;
    assert_eq!(delivery.envelope(), &outbound);
    // Pub/Sub deliveries carry no acknowledger; uniform ack still works.
    transport.ack(delivery).await?;

    transport.stop_accepting();
    assert!(!transport.is_accepting());
    assert!(
        transport
            .publish(envelope(202, "catga.test.stopped"))
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn pubsub_exactly_once_publish_is_deduplicated_broker_side() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let channel = format!("catga-test-pubsub-{}", uuid::Uuid::new_v4());
    let transport = pubsub_transport(&url, &channel).await?;

    let metadata =
        MessageMetadata::new(301, Some(301)).with_quality_of_service(QualityOfService::ExactlyOnce);
    let outbound = catga_core::Envelope::new(301, "catga.test.exactly-once", vec![9], metadata);

    // Both publishes succeed, but the broker-side script emits only the first.
    transport.publish(outbound.clone()).await?;
    transport.publish(outbound.clone()).await?;

    let delivery = bounded_receive(&transport, 5_000).await?;
    assert_eq!(delivery.envelope(), &outbound);

    let duplicate = tokio::time::timeout(Duration::from_millis(400), transport.receive()).await;
    assert!(
        duplicate.is_err(),
        "the duplicate publish must not be redelivered"
    );
    Ok(())
}

#[tokio::test]
async fn pubsub_exactly_once_receive_is_deduplicated_per_subscriber() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let channel = format!("catga-test-pubsub-{}", uuid::Uuid::new_v4());
    let transport = pubsub_transport(&url, &channel).await?;
    let mut raw = raw_connection(&url).await?;

    // A raw republication bypasses the publisher-side dedup script, delivering
    // the same exactly-once identity twice to the subscriber.
    let metadata =
        MessageMetadata::new(302, Some(302)).with_quality_of_service(QualityOfService::ExactlyOnce);
    let outbound = catga_core::Envelope::new(302, "catga.test.exactly-once", vec![7], metadata);
    let payload = MemoryPackCodec::default().encode(&outbound)?;
    for _ in 0..2 {
        let _: usize = raw
            .publish(channel.as_str(), payload.clone())
            .await
            .map_err(map_redis_error)?;
    }

    let delivery = bounded_receive(&transport, 5_000).await?;
    assert_eq!(delivery.envelope(), &outbound);

    // The second copy loses the per-subscriber claim and is skipped inside receive.
    let duplicate = tokio::time::timeout(Duration::from_millis(400), transport.receive()).await;
    assert!(
        duplicate.is_err(),
        "the duplicate copy must be deduplicated"
    );
    Ok(())
}

#[tokio::test]
async fn pubsub_from_client_and_connect_with_client_subscribe() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let channel = format!("catga-test-pubsub-{}", uuid::Uuid::new_v4());
    let client = redis::Client::open(url.as_str()).map_err(map_redis_error)?;

    let config = || RedisPubSubConfig {
        server: url.clone().into(),
        channel: channel.clone().into(),
    };
    let first = RedisPubSubTransport::from_client(client.clone(), config()).await?;
    let second = RedisPubSubTransport::connect_with_client(client, config()).await?;

    // Each subscriber instance observes its own copy of one broadcast.
    let outbound = envelope(401, "catga.test.fanout");
    first.publish(outbound.clone()).await?;
    let received = tokio::try_join!(bounded_receive_arc(&first), bounded_receive_arc(&second),)?;
    assert_eq!(received.0.envelope(), &outbound);
    assert_eq!(received.1.envelope(), &outbound);
    Ok(())
}

async fn bounded_receive_arc(
    transport: &RedisPubSubTransport,
) -> CatgaResult<catga_core::Delivery> {
    tokio::time::timeout(Duration::from_secs(5), transport.receive())
        .await
        .map_err(|_| timeout_error("Redis Pub/Sub receive timed out"))?
}
