#![allow(missing_docs)]

//! Public client-injection constructor coverage.

use catga_redis::{
    RedisConfig, RedisPendingReclaimOptions, RedisPubSubConfig, RedisPubSubTransport,
    RedisTransport,
};

fn redis_client() -> redis::Client {
    redis::Client::open("redis://127.0.0.1:1").expect("test Redis URL must be valid")
}

fn stream_config() -> RedisConfig {
    RedisConfig {
        server: "redis://unused.invalid".into(),
        stream: "orders".into(),
        group: "workers".into(),
        consumer: "worker-1".into(),
    }
}

fn pubsub_config() -> RedisPubSubConfig {
    RedisPubSubConfig {
        server: "redis://unused.invalid".into(),
        channel: "orders".into(),
    }
}

#[test]
fn client_injection_constructors_are_public() {
    let client = redis_client();

    std::mem::drop(RedisTransport::from_client(client.clone(), stream_config()));
    std::mem::drop(RedisTransport::connect_with_client(
        client.clone(),
        stream_config(),
        RedisPendingReclaimOptions::default(),
    ));
    std::mem::drop(RedisPubSubTransport::from_client(
        client.clone(),
        pubsub_config(),
    ));
    std::mem::drop(RedisPubSubTransport::connect_with_client(
        client,
        pubsub_config(),
    ));
}
