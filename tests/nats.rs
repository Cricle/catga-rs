//! NATS JetStream integration tests.

use catga_core::{Envelope, MessageMetadata, MessageTransport};
use catga_nats::{NatsConfig, NatsTransport};

#[tokio::test]
async fn jetstream_round_trip_and_ack() {
    let Some(server) = std::env::var("CATGA_NATS_URL").ok() else {
        eprintln!("skipping NATS integration test: CATGA_NATS_URL is unset");
        return;
    };
    let suffix = format!("{}", std::process::id());
    let transport = NatsTransport::connect(NatsConfig {
        server: server.into(),
        stream: format!("CATGA_{suffix}").into(),
        subject: format!("catga.{suffix}").into(),
        consumer: format!("catga_{suffix}").into(),
    })
    .await
    .unwrap();

    transport
        .publish(Envelope::new(
            1,
            "order.created",
            vec![1, 2],
            MessageMetadata::new(1, None),
        ))
        .await
        .unwrap();
    let delivery = transport.receive().await.unwrap();
    assert_eq!(delivery.envelope().payload(), [1, 2]);
    transport.ack(delivery).await.unwrap();
}
