//! Service-free validation contracts for public NATS constructors.

use std::time::Duration;

use catga_core::ErrorCode;
use catga_nats::{
    NatsConfig, NatsConsumerMode, NatsConsumerOptions, NatsEventStore, NatsPubSubConfig,
    NatsPubSubTransport, NatsPublisher, NatsPublisherConfig, NatsReceiveOptions, NatsRequestClient,
    NatsRequestServer, NatsTransport, NatsTransportOptions,
};

const UNREACHABLE_SERVER: &str = "nats://127.0.0.1:1";

fn durable_config() -> NatsConfig {
    NatsConfig {
        server: UNREACHABLE_SERVER.into(),
        stream: "CATGA_ORDERS".into(),
        subject: "orders.created".into(),
        consumer: "catga_orders".into(),
    }
}

#[tokio::test]
async fn publisher_rejects_blank_stream_configuration_before_connecting() {
    let invalid = NatsPublisherConfig {
        server: UNREACHABLE_SERVER.into(),
        stream: " ".into(),
        subject: "orders.created".into(),
    };
    let Err(error) = NatsPublisher::connect(invalid).await else {
        panic!("publisher must reject invalid stream configuration before network I/O");
    };
    assert_eq!(error.code(), ErrorCode::Validation);
}

#[test]
fn nats_receive_options_default_to_a_bounded_pull_batch_and_allow_overrides() {
    let default_options = NatsReceiveOptions::default();
    assert_eq!(default_options.pull_batch_size().get(), 64);

    let configured = default_options
        .with_pull_batch_size(16)
        .expect("positive receive batch size is valid");
    assert_eq!(configured.pull_batch_size().get(), 16);
    assert!(matches!(
        NatsReceiveOptions::default().with_pull_batch_size(0),
        Err(error) if error.code() == ErrorCode::Validation
    ));
}

#[test]
fn nats_transport_options_keep_durable_consumers_by_default_and_allow_ephemeral_cleanup() {
    let defaults = NatsTransportOptions::default();
    assert_eq!(defaults.consumer().mode(), NatsConsumerMode::Durable);
    assert_eq!(defaults.consumer().inactive_threshold(), None);

    let ephemeral =
        NatsConsumerOptions::ephemeral().with_inactive_threshold(Duration::from_secs(90));
    let configured = defaults.with_consumer(ephemeral);
    assert_eq!(configured.consumer().mode(), NatsConsumerMode::Ephemeral);
    assert_eq!(
        configured.consumer().inactive_threshold(),
        Some(Duration::from_secs(90))
    );
}

#[tokio::test]
async fn durable_transport_rejects_blank_resource_names_before_connecting() {
    for invalid in [
        NatsConfig {
            stream: " \t".into(),
            ..durable_config()
        },
        NatsConfig {
            subject: "".into(),
            ..durable_config()
        },
        NatsConfig {
            consumer: "\n".into(),
            ..durable_config()
        },
    ] {
        let Err(error) = NatsTransport::connect(invalid).await else {
            panic!("invalid durable resource names must be rejected before network I/O");
        };
        assert_eq!(error.code(), ErrorCode::Validation);
    }
}

#[tokio::test]
async fn durable_transport_accepts_explicit_ephemeral_consumer_options() {
    let options = NatsTransportOptions::default().with_consumer(
        NatsConsumerOptions::ephemeral().with_inactive_threshold(Duration::from_secs(90)),
    );
    let Err(error) = NatsTransport::connect_with_options(durable_config(), options).await else {
        panic!("an unreachable server must fail after accepting valid consumer options");
    };
    assert_ne!(error.code(), ErrorCode::Validation);
}

#[tokio::test]
async fn core_and_request_connectors_reject_blank_subjects_before_connecting() {
    for subject in ["", " \t\n"] {
        let Err(pubsub_error) = NatsPubSubTransport::connect(NatsPubSubConfig {
            server: UNREACHABLE_SERVER.into(),
            subject: subject.into(),
        })
        .await
        else {
            panic!("Core Pub/Sub subjects must be nonblank");
        };
        assert_eq!(pubsub_error.code(), ErrorCode::Validation);

        let Err(client_error) = NatsRequestClient::connect(UNREACHABLE_SERVER, subject).await
        else {
            panic!("NATS request client subjects must be nonblank");
        };
        assert_eq!(client_error.code(), ErrorCode::Validation);

        let Err(server_error) = NatsRequestServer::connect(UNREACHABLE_SERVER, subject).await
        else {
            panic!("NATS request server subjects must be nonblank");
        };
        assert_eq!(server_error.code(), ErrorCode::Validation);
    }
}

#[tokio::test]
async fn event_store_rejects_invalid_subject_prefixes_before_connecting() {
    for prefix in ["", "catga.*", "catga.>", "catga..events"] {
        let Err(error) = NatsEventStore::connect(UNREACHABLE_SERVER, "CATGA_EVENTS", prefix).await
        else {
            panic!("event-store subject prefixes must contain only literal NATS tokens");
        };
        assert_eq!(error.code(), ErrorCode::Validation);
    }
}
