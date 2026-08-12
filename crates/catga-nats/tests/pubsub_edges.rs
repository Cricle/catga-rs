//! Core Pub/Sub edge contracts: application-owned client constructors subscribe before
//! returning, lifecycle traits report immediate completion, and Core NATS keeps its
//! AtMostOnly-on-the-wire delivery semantics.

#[path = "support/envelopes.rs"]
mod envelopes;
#[path = "support/names.rs"]
mod names;
#[path = "support/nats_server.rs"]
mod nats_server;

use std::time::Duration;

use catga_core::{
    AsyncInitializable, CatgaResult, ErrorCode, MessageTransport, QualityOfService, Stoppable,
    Waitable,
};
use catga_nats::{NatsPubSubConfig, NatsPubSubTransport};
use envelopes::envelope;
use names::unique;
use nats_server::{server_url, test_error};
use tokio_util::sync::CancellationToken;

fn config(subject: &str) -> NatsPubSubConfig {
    NatsPubSubConfig {
        server: server_url().into(),
        subject: subject.into(),
    }
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn application_owned_clients_deliver_before_returning() -> CatgaResult<()> {
    let subject = unique("catga.pubsub.injected");
    let subscriber_client = async_nats::connect(server_url())
        .await
        .map_err(|error| test_error("connect subscriber client", error))?;
    let publisher_client = async_nats::connect(server_url())
        .await
        .map_err(|error| test_error("connect publisher client", error))?;

    // Both application-owned constructors subscribe before returning.
    let subscriber =
        NatsPubSubTransport::from_client(subscriber_client.clone(), config(&subject)).await?;
    let publisher =
        NatsPubSubTransport::connect_with_client(publisher_client, config(&subject)).await?;

    // A flush forces the subscription registration across the wire before the publisher on
    // its own connection sends, so the publish cannot race past it.
    subscriber_client
        .flush()
        .await
        .map_err(|error| test_error("flush subscription registration", error))?;

    // A publish issued immediately after construction cannot race past the subscription.
    let message = envelope(41, QualityOfService::AtMostOnce);
    publisher.publish(message.clone()).await?;
    let delivery = tokio::time::timeout(Duration::from_secs(2), subscriber.receive())
        .await
        .map_err(|error| test_error("receive injected-client delivery", error))??;
    assert_eq!(delivery.envelope(), &message);
    delivery.acknowledge().await
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn lifecycle_traits_report_immediate_completion() -> CatgaResult<()> {
    let transport = NatsPubSubTransport::connect(config(&unique("catga.pubsub.lifecycle"))).await?;

    // Subscription setup finished during connect, and Core NATS retains no ack work.
    AsyncInitializable::initialize(&transport).await?;
    assert_eq!(Waitable::pending_operations(&transport), 0);
    Waitable::wait_for_completion(&transport, CancellationToken::new()).await?;

    // The acceptance gate fences later publications without breaking the traits above.
    Stoppable::stop_accepting(&transport);
    assert!(!Stoppable::is_accepting(&transport));
    assert!(matches!(
        transport.publish(envelope(42, QualityOfService::AtMostOnce)).await,
        Err(error) if error.code() == ErrorCode::Unavailable
    ));
    assert_eq!(Waitable::pending_operations(&transport), 0);
    Waitable::wait_for_completion(&transport, CancellationToken::new()).await?;
    Ok(())
}
