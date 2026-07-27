#![allow(missing_docs)]

//! Compile coverage for public NATS client-injection constructors.

use catga_core::{CatgaResult, Envelope, EnvelopeCodec};
use catga_nats::{
    NatsConfig, NatsPubSubConfig, NatsPubSubTransport, NatsRequestClient, NatsRequestServer,
    NatsTransport,
};

#[derive(Default)]
struct MarkerCodec;

impl EnvelopeCodec for MarkerCodec {
    fn encode(&self, _: &Envelope) -> CatgaResult<Vec<u8>> {
        Ok(Vec::new())
    }

    fn decode(&self, _: &[u8]) -> CatgaResult<Envelope> {
        Err(catga_core::CatgaError::new(
            catga_core::ErrorCode::Internal,
            "compile-only codec",
        ))
    }
}

fn transport_config() -> NatsConfig {
    NatsConfig {
        server: "nats://unused.invalid".into(),
        stream: "orders".into(),
        subject: "orders.created".into(),
        consumer: "workers".into(),
    }
}

fn pubsub_config() -> NatsPubSubConfig {
    NatsPubSubConfig {
        server: "nats://unused.invalid".into(),
        subject: "orders.created".into(),
    }
}

fn client_injection_constructors_are_public(client: async_nats::Client) {
    std::mem::drop(NatsTransport::from_client(
        client.clone(),
        transport_config(),
    ));
    std::mem::drop(NatsTransport::connect_with_client(
        client.clone(),
        transport_config(),
    ));
    std::mem::drop(NatsPubSubTransport::from_client(
        client.clone(),
        pubsub_config(),
    ));
    std::mem::drop(NatsPubSubTransport::connect_with_client(
        client.clone(),
        pubsub_config(),
    ));
    let _ = NatsRequestClient::from_client(client.clone(), "orders.create");
    let _ = NatsRequestClient::connect_with_client(client.clone(), "orders.create");
    std::mem::drop(NatsRequestServer::from_client(
        client.clone(),
        "orders.create",
    ));
    std::mem::drop(NatsRequestServer::connect_with_client(
        client,
        "orders.create",
    ));
}

#[test]
fn client_injection_constructor_signatures_compile_without_a_nats_server() {
    let _ = client_injection_constructors_are_public as fn(async_nats::Client);
}

#[test]
fn codec_injection_constructor_signatures_compile_without_a_nats_server() {
    let config = transport_config();
    std::mem::drop(NatsTransport::<MarkerCodec>::connect_with_codec(
        config.clone(),
        MarkerCodec,
    ));
    let from_client = |client: async_nats::Client| {
        std::mem::drop(NatsTransport::<MarkerCodec>::from_client_with_codec(
            client.clone(),
            config.clone(),
            MarkerCodec,
        ));
        std::mem::drop(
            NatsTransport::<MarkerCodec>::connect_with_client_with_codec(
                client,
                config,
                MarkerCodec,
            ),
        );
    };
    let _ = from_client;
}
