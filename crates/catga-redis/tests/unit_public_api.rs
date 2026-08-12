//! Pure-logic unit coverage for public API validation and construction paths.
//!
//! Every test here runs without a Redis server: they cover argument validation that
//! happens before any network I/O and constructor error mapping for invalid URLs.

use std::time::Duration;

use catga_core::{CatgaResult, ErrorCode};
use catga_redis::{
    MAX_REDIS_PENDING_RECLAIM_SCANS, RedisCommandOptions, RedisEventStore,
    RedisPendingReclaimOptions, RedisPubSubConfig, RedisPubSubTransport, RedisRequestClient,
    RedisRequestServer,
};

#[test]
fn command_options_accept_a_nonzero_timeout() -> CatgaResult<()> {
    let options = RedisCommandOptions::new(Duration::from_millis(250))?;
    assert_eq!(options.response_timeout(), Duration::from_millis(250));
    let defaulted = RedisCommandOptions::default();
    assert_eq!(
        defaulted.response_timeout(),
        catga_redis::DEFAULT_REDIS_COMMAND_RESPONSE_TIMEOUT
    );
    assert_eq!(defaulted, defaulted.clone());
    Ok(())
}

#[test]
fn command_options_reject_a_zero_timeout() {
    let result = RedisCommandOptions::new(Duration::ZERO);
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Validation));
}

#[test]
fn pending_reclaim_options_validate_their_bounds() -> CatgaResult<()> {
    let options = RedisPendingReclaimOptions::new(Duration::from_secs(5), 4)?;
    assert_eq!(options.minimum_idle(), Duration::from_secs(5));
    assert_eq!(options.max_scans(), 4);
    assert_eq!(options, options.clone());

    let defaulted = RedisPendingReclaimOptions::default();
    assert_eq!(defaulted.minimum_idle(), Duration::from_secs(30));

    assert!(matches!(
        RedisPendingReclaimOptions::new(Duration::ZERO, 1),
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert!(matches!(
        RedisPendingReclaimOptions::new(Duration::from_secs(1), 0),
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert!(matches!(
        RedisPendingReclaimOptions::new(
            Duration::from_secs(1),
            MAX_REDIS_PENDING_RECLAIM_SCANS + 1
        ),
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert!(matches!(
        RedisPendingReclaimOptions::new(Duration::from_nanos(999_999), 1),
        Err(error) if error.code() == ErrorCode::Validation
    ));
    // An idle duration beyond Redis's millisecond precision cannot be a PX argument.
    assert!(matches!(
        RedisPendingReclaimOptions::new(Duration::from_secs(u64::MAX), 1),
        Err(error) if error.code() == ErrorCode::Validation
    ));
    Ok(())
}

#[tokio::test]
async fn event_store_rejects_an_invalid_server_url() {
    let result = RedisEventStore::connect("not-a-redis-url", "catga-test").await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Transient));
}

#[tokio::test]
async fn event_store_reports_a_transient_error_for_an_unreachable_server() {
    let result = RedisEventStore::connect("redis://127.0.0.1:1/", "catga-test").await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Transient));
}

#[tokio::test]
async fn pubsub_rejects_a_blank_channel_before_any_network_io() {
    for channel in ["", "   "] {
        let result = RedisPubSubTransport::connect(RedisPubSubConfig {
            server: "redis://127.0.0.1:1/".into(),
            channel: channel.into(),
        })
        .await;
        assert!(
            matches!(result, Err(error) if error.code() == ErrorCode::Validation),
            "blank channel must be rejected"
        );
    }
}

#[tokio::test]
async fn pubsub_rejects_an_invalid_server_url() {
    let result = RedisPubSubTransport::connect(RedisPubSubConfig {
        server: "not-a-redis-url".into(),
        channel: "orders".into(),
    })
    .await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Transient));
}

#[test]
fn request_client_rejects_an_invalid_server_url() {
    let result = RedisRequestClient::connect("not-a-redis-url");
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Transient));
}

#[tokio::test]
async fn request_server_rejects_an_empty_destination_before_any_network_io() {
    let result = RedisRequestServer::connect("redis://127.0.0.1:1/", "").await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Validation));
}

#[tokio::test]
async fn request_server_rejects_an_invalid_server_url() {
    let result = RedisRequestServer::connect("not-a-redis-url", "requests").await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Transient));
}

#[tokio::test]
async fn request_to_validates_arguments_before_any_network_io() -> CatgaResult<()> {
    let client = RedisRequestClient::connect("redis://127.0.0.1:1/")?;
    let request = catga_core::Envelope::new(
        1,
        "catga.test.request",
        Vec::new(),
        catga_core::MessageMetadata::new(1, Some(1)),
    );

    let empty_destination = client
        .request_to("", request.clone(), Duration::from_secs(1))
        .await;
    assert!(matches!(empty_destination, Err(error) if error.code() == ErrorCode::Validation));

    let zero_timeout = client.request_to("requests", request, Duration::ZERO).await;
    assert!(matches!(zero_timeout, Err(error) if error.code() == ErrorCode::Validation));
    Ok(())
}

#[cfg(feature = "streams-rpc")]
mod streams_rpc {
    use std::sync::Arc;
    use std::time::Duration;

    use catga_core::{
        CatgaResult, Delivery, Destination, DestinationTransport, Envelope, ErrorCode,
        MessageTransport,
    };
    use catga_redis::{RedisStreamsRequestClient, RedisStreamsRequestServer};

    struct NoopTransport;

    #[async_trait::async_trait]
    impl MessageTransport for NoopTransport {
        async fn publish(&self, _: Envelope) -> CatgaResult<()> {
            Ok(())
        }

        async fn receive(&self) -> CatgaResult<Delivery> {
            std::future::pending().await
        }
    }

    #[async_trait::async_trait]
    impl DestinationTransport for NoopTransport {
        async fn send_to(&self, _: &Destination, _: Envelope) -> CatgaResult<()> {
            Ok(())
        }

        async fn receive_from(&self, _: &Destination) -> CatgaResult<Delivery> {
            std::future::pending().await
        }
    }

    #[test]
    fn streams_rpc_constructors_reject_an_invalid_server_url() -> CatgaResult<()> {
        let transport = Arc::new(NoopTransport);
        let client = RedisStreamsRequestClient::new(transport.clone(), "not-a-redis-url");
        assert!(matches!(client, Err(error) if error.code() == ErrorCode::Transient));

        let client = RedisStreamsRequestClient::new(transport.clone(), "redis://127.0.0.1:1/")?;
        let _ = client;

        let server = RedisStreamsRequestServer::new(
            transport,
            Destination::parse("orders")?,
            "not-a-redis-url",
        );
        assert!(matches!(server, Err(error) if error.code() == ErrorCode::Transient));
        Ok(())
    }

    #[tokio::test]
    async fn streams_rpc_request_rejects_a_zero_timeout_before_any_network_io() -> CatgaResult<()> {
        let client =
            RedisStreamsRequestClient::new(Arc::new(NoopTransport), "redis://127.0.0.1:1/")?;
        let request = Envelope::new(
            1,
            "catga.test.request",
            Vec::new(),
            catga_core::MessageMetadata::new(1, Some(1)),
        );

        let invalid_destination = client
            .request_to("", request.clone(), Duration::from_secs(1))
            .await;
        assert!(matches!(invalid_destination, Err(error) if error.code() == ErrorCode::Validation));

        let zero_timeout = client.request_to("orders", request, Duration::ZERO).await;
        assert!(matches!(zero_timeout, Err(error) if error.code() == ErrorCode::Validation));
        Ok(())
    }

    #[tokio::test]
    async fn streams_rpc_reports_unavailable_when_the_reply_connection_fails() -> CatgaResult<()> {
        let client =
            RedisStreamsRequestClient::new(Arc::new(NoopTransport), "redis://127.0.0.1:1/")?;
        let request = Envelope::new(
            1,
            "catga.test.request",
            Vec::new(),
            catga_core::MessageMetadata::new(1, Some(1)),
        );

        let result = client
            .request_to("orders", request, Duration::from_secs(5))
            .await;
        assert!(
            matches!(&result, Err(error) if error.code() == ErrorCode::Unavailable),
            "a refused reply connection must map to Unavailable, got {result:?}"
        );
        Ok(())
    }
}
