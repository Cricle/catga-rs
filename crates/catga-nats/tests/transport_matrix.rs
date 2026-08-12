//! Transport constructor and lifecycle contract tests against a live JetStream server.
//!
//! Every `connect_*`/`from_client_*` constructor alias must provision the configured stream and
//! consumer identically, and the lifecycle traits must gate publication deterministically.

#[path = "support/envelopes.rs"]
mod envelopes;
#[path = "support/names.rs"]
mod names;
#[path = "support/nats_server.rs"]
mod nats_server;

use std::time::Duration;

use catga_core::{
    AsyncInitializable, CatgaResult, Delivery, Destination, DestinationTransport, ErrorCode,
    HealthCheckable, MessageTransport, QualityOfService, Stoppable, Waitable,
    codec::memorypack::MemoryPackCodec,
};
use catga_nats::{
    NatsConfig, NatsConsumerOptions, NatsDestinationConfig, NatsReceiveOptions, NatsTransport,
    NatsTransportOptions,
};
use envelopes::envelope;
use names::unique;
use nats_server::{server_url, test_error};

fn config() -> NatsConfig {
    NatsConfig {
        server: server_url().into(),
        stream: unique("CATGA_TRANSPORT_MATRIX").into(),
        subject: unique("catga.matrix").into(),
        consumer: unique("CATGA_MATRIX_CONSUMER").into(),
    }
}

async fn receive_with_timeout(transport: &NatsTransport<MemoryPackCodec>) -> CatgaResult<Delivery> {
    tokio::time::timeout(Duration::from_secs(3), transport.receive())
        .await
        .map_err(|error| test_error("receive transport delivery", error))?
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn every_connect_constructor_alias_provisions_an_equivalent_transport() -> CatgaResult<()> {
    let receive_options = NatsReceiveOptions::default().with_pull_batch_size(4)?;
    let consumer_options =
        NatsConsumerOptions::durable().with_inactive_threshold(Duration::from_secs(300));
    let options = NatsTransportOptions::default()
        .with_receive(receive_options)
        .with_consumer(consumer_options);

    let via_connect = NatsTransport::connect(config()).await?;
    let via_options = NatsTransport::connect_with_options(config(), options).await?;
    let via_receive_options =
        NatsTransport::connect_with_receive_options(config(), receive_options).await?;
    let via_consumer_options =
        NatsTransport::connect_with_consumer_options(config(), consumer_options).await?;
    let via_codec = NatsTransport::connect_with_codec(config(), MemoryPackCodec::default()).await?;
    let via_codec_receive = NatsTransport::connect_with_codec_and_receive_options(
        config(),
        MemoryPackCodec::default(),
        receive_options,
    )
    .await?;
    let via_codec_options = NatsTransport::connect_with_codec_and_options(
        config(),
        MemoryPackCodec::default(),
        options,
    )
    .await?;

    for transport in [
        via_connect,
        via_options,
        via_receive_options,
        via_consumer_options,
        via_codec,
        via_codec_receive,
        via_codec_options,
    ] {
        assert!(transport.is_accepting());
        assert!(transport.is_healthy());
        assert_eq!(
            transport.health_status(),
            Some("NATS transport is connected")
        );
        transport.initialize().await?;
        assert_eq!(transport.pending_operations(), 0);
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn every_client_constructor_alias_reuses_the_callers_client() -> CatgaResult<()> {
    let receive_options = NatsReceiveOptions::default().with_pull_batch_size(2)?;
    let options = NatsTransportOptions::default().with_receive(receive_options);

    let via_from_client = NatsTransport::from_client(
        async_nats::connect(server_url())
            .await
            .map_err(|e| test_error("connect fixture client", e))?,
        config(),
    )
    .await?;
    let via_from_client_options = NatsTransport::from_client_with_options(
        async_nats::connect(server_url())
            .await
            .map_err(|e| test_error("connect fixture client", e))?,
        config(),
        options,
    )
    .await?;
    let via_from_client_receive = NatsTransport::from_client_with_receive_options(
        async_nats::connect(server_url())
            .await
            .map_err(|e| test_error("connect fixture client", e))?,
        config(),
        receive_options,
    )
    .await?;
    let via_connect_with_client = NatsTransport::connect_with_client(
        async_nats::connect(server_url())
            .await
            .map_err(|e| test_error("connect fixture client", e))?,
        config(),
    )
    .await?;
    let via_connect_with_client_receive = NatsTransport::connect_with_client_and_receive_options(
        async_nats::connect(server_url())
            .await
            .map_err(|e| test_error("connect fixture client", e))?,
        config(),
        receive_options,
    )
    .await?;
    let via_from_client_codec = NatsTransport::from_client_with_codec(
        async_nats::connect(server_url())
            .await
            .map_err(|e| test_error("connect fixture client", e))?,
        config(),
        MemoryPackCodec::default(),
    )
    .await?;
    let via_from_client_codec_receive = NatsTransport::from_client_with_codec_and_receive_options(
        async_nats::connect(server_url())
            .await
            .map_err(|e| test_error("connect fixture client", e))?,
        config(),
        MemoryPackCodec::default(),
        receive_options,
    )
    .await?;
    let via_from_client_codec_options = NatsTransport::from_client_with_codec_and_options(
        async_nats::connect(server_url())
            .await
            .map_err(|e| test_error("connect fixture client", e))?,
        config(),
        MemoryPackCodec::default(),
        options,
    )
    .await?;
    let via_connect_with_client_codec = NatsTransport::connect_with_client_with_codec(
        async_nats::connect(server_url())
            .await
            .map_err(|e| test_error("connect fixture client", e))?,
        config(),
        MemoryPackCodec::default(),
    )
    .await?;
    let via_connect_with_client_codec_receive =
        NatsTransport::connect_with_client_with_codec_and_receive_options(
            async_nats::connect(server_url())
                .await
                .map_err(|e| test_error("connect fixture client", e))?,
            config(),
            MemoryPackCodec::default(),
            receive_options,
        )
        .await?;

    let transports = [
        via_from_client,
        via_from_client_options,
        via_from_client_receive,
        via_connect_with_client,
        via_connect_with_client_receive,
        via_from_client_codec,
        via_from_client_codec_receive,
        via_from_client_codec_options,
        via_connect_with_client_codec,
        via_connect_with_client_codec_receive,
    ];
    for transport in &transports {
        assert!(transport.is_healthy());
    }
    // Prove one of them round-trips; all share the same provisioning path.
    let transport = &transports[0];
    let published = envelope(41, QualityOfService::AtLeastOnce);
    transport.publish(published.clone()).await?;
    let delivery = receive_with_timeout(transport).await?;
    assert_eq!(delivery.envelope(), &published);
    assert_eq!(delivery.attempts(), 1);
    delivery.acknowledge().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn negative_acknowledgement_redelivers_with_an_incremented_attempt_count() -> CatgaResult<()>
{
    let transport = NatsTransport::connect(config()).await?;
    let published = envelope(42, QualityOfService::AtLeastOnce);
    transport.publish(published.clone()).await?;

    let first = receive_with_timeout(&transport).await?;
    assert_eq!(first.envelope(), &published);
    first.negative_acknowledge().await?;

    let redelivered = receive_with_timeout(&transport).await?;
    assert_eq!(redelivered.envelope(), &published);
    assert_eq!(redelivered.attempts(), 2);
    redelivered.acknowledge().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn destinations_round_trip_and_reject_unprovisioned_or_duplicate_names() -> CatgaResult<()> {
    let transport = NatsTransport::connect(config()).await?;
    let destination = Destination::parse(unique("matrix-destination"))?;
    let destination_config = NatsDestinationConfig {
        stream: unique("CATGA_MATRIX_DEST").into(),
        subject: unique("catga.matrix.dest").into(),
        consumer: unique("CATGA_MATRIX_DEST_CONSUMER").into(),
    };

    // Unprovisioned destinations are fenced before any broker I/O.
    assert!(matches!(
        transport.send_to(&destination, envelope(43, QualityOfService::AtLeastOnce)).await,
        Err(error) if error.code() == ErrorCode::NotFound
    ));
    assert!(matches!(
        transport.receive_from(&destination).await,
        Err(error) if error.code() == ErrorCode::NotFound
    ));
    assert!(matches!(
        transport.declare_destination(&destination),
        Err(error) if error.code() == ErrorCode::NotFound
    ));

    transport
        .provision_destination(destination.clone(), destination_config.clone())
        .await?;
    transport.declare_destination(&destination)?;
    assert!(matches!(
        transport
            .provision_destination(destination.clone(), destination_config)
            .await,
        Err(error) if error.code() == ErrorCode::Conflict
    ));

    // Both durable QoS levels flow through the destination consumer.
    let at_least_once = envelope(44, QualityOfService::AtLeastOnce);
    transport
        .send_to(&destination, at_least_once.clone())
        .await?;
    let exactly_once = envelope(45, QualityOfService::ExactlyOnce);
    transport
        .send_to(&destination, exactly_once.clone())
        .await?;
    transport
        .send_to(&destination, exactly_once.clone())
        .await?;

    let first = tokio::time::timeout(Duration::from_secs(3), transport.receive_from(&destination))
        .await
        .map_err(|error| test_error("receive destination delivery", error))??;
    assert_eq!(first.envelope(), &at_least_once);
    first.acknowledge().await?;
    let second = tokio::time::timeout(Duration::from_secs(3), transport.receive_from(&destination))
        .await
        .map_err(|error| test_error("receive deduplicated destination delivery", error))??;
    assert_eq!(second.envelope(), &exactly_once);
    second.acknowledge().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn destination_provisioning_validates_resource_names_before_broker_io() -> CatgaResult<()> {
    let transport = NatsTransport::connect(config()).await?;
    let destination = Destination::parse(unique("matrix-invalid-destination"))?;
    let valid = NatsDestinationConfig {
        stream: unique("CATGA_MATRIX_INVALID").into(),
        subject: unique("catga.matrix.invalid").into(),
        consumer: unique("CATGA_MATRIX_INVALID_CONSUMER").into(),
    };
    for invalid in [
        NatsDestinationConfig {
            stream: " ".into(),
            ..valid.clone()
        },
        NatsDestinationConfig {
            subject: " ".into(),
            ..valid.clone()
        },
        NatsDestinationConfig {
            consumer: " ".into(),
            ..valid.clone()
        },
    ] {
        assert!(matches!(
            transport
                .provision_destination(destination.clone(), invalid)
                .await,
            Err(error) if error.code() == ErrorCode::Validation
        ));
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn stop_accepting_fences_publication_and_wait_for_completion_drains() -> CatgaResult<()> {
    let transport = NatsTransport::connect(config()).await?;
    let destination = Destination::parse(unique("matrix-stop-destination"))?;
    transport
        .provision_destination(
            destination.clone(),
            NatsDestinationConfig {
                stream: unique("CATGA_MATRIX_STOP").into(),
                subject: unique("catga.matrix.stop").into(),
                consumer: unique("CATGA_MATRIX_STOP_CONSUMER").into(),
            },
        )
        .await?;

    transport.stop_accepting();
    assert!(!transport.is_accepting());
    assert!(matches!(
        transport.publish(envelope(46, QualityOfService::AtLeastOnce)).await,
        Err(error) if error.code() == ErrorCode::Unavailable
    ));
    assert!(matches!(
        transport.send_to(&destination, envelope(47, QualityOfService::AtLeastOnce)).await,
        Err(error) if error.code() == ErrorCode::Unavailable
    ));
    assert!(matches!(
        transport.publish(envelope(48, QualityOfService::ExactlyOnce)).await,
        Err(error) if error.code() == ErrorCode::Unavailable
    ));

    // No acknowledgement work is in flight, so draining completes immediately.
    tokio::time::timeout(
        Duration::from_secs(2),
        transport.wait_for_completion(tokio_util::sync::CancellationToken::new()),
    )
    .await
    .map_err(|error| test_error("wait for transport completion", error))??;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn ephemeral_consumers_round_trip_without_a_durable_cursor() -> CatgaResult<()> {
    let transport = NatsTransport::connect_with_consumer_options(
        config(),
        NatsConsumerOptions::ephemeral().with_inactive_threshold(Duration::from_secs(60)),
    )
    .await?;
    let published = envelope(49, QualityOfService::AtLeastOnce);
    transport.publish(published.clone()).await?;
    let delivery = receive_with_timeout(&transport).await?;
    assert_eq!(delivery.envelope(), &published);
    delivery.acknowledge().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn acknowledgements_after_connection_close_fail_transiently() -> CatgaResult<()> {
    let client = async_nats::connect(server_url())
        .await
        .map_err(|error| test_error("connect injected transport client", error))?;
    let transport = NatsTransport::from_client_with_receive_options(
        client.clone(),
        config(),
        NatsReceiveOptions::default().with_pull_batch_size(2)?,
    )
    .await?;
    transport
        .publish(envelope(1, QualityOfService::AtLeastOnce))
        .await?;
    transport
        .publish(envelope(2, QualityOfService::AtLeastOnce))
        .await?;
    let first = receive_with_timeout(&transport).await?;
    let second = receive_with_timeout(&transport).await?;
    // Releasing the transport drops the pull-batch subscription so the drain finishes
    // without waiting for the batch to expire.
    drop(transport);

    // Draining closes the shared connection for every clone; afterwards the broker calls
    // made by the retained acknowledgement tokens fail instead of hanging.
    client
        .drain()
        .await
        .map_err(|error| test_error("drain the injected connection", error))?;
    let mut closed = false;
    for _ in 0..200 {
        if client
            .publish("catga.ack.probe".to_owned(), Vec::new().into())
            .await
            .is_err()
        {
            closed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(closed, "the drained connection must reject broker calls");

    assert!(matches!(
        first.acknowledge().await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    assert!(matches!(
        second.negative_acknowledge().await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    Ok(())
}
