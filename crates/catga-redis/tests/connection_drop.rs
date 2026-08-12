//! Contract coverage for live connection drops and subscribe failures, backed by
//! an in-process scripted RESP2 mock Redis server. These tests need no real service.

use std::time::Duration;

use catga_core::{CatgaError, CatgaResult, Envelope, ErrorCode, MessageMetadata, MessageTransport};
use catga_redis::{
    RedisPubSubConfig, RedisPubSubTransport, RedisRequestClient, RedisRequestServer,
};

#[path = "support/mock_redis.rs"]
mod mock_redis;

use mock_redis::{SubscribeReply, spawn_mock_redis};

fn request(id: u64) -> Envelope {
    Envelope::new(
        id,
        "catga.test.request",
        Vec::new(),
        MessageMetadata::new(id, Some(id)),
    )
}

#[tokio::test]
async fn pubsub_receive_fails_when_the_subscription_connection_drops() -> CatgaResult<()> {
    let (url, server) = spawn_mock_redis(SubscribeReply::ConfirmThenClose).await?;
    let transport = RedisPubSubTransport::connect(RedisPubSubConfig {
        server: url.into(),
        channel: "catga-test-mock-drop".into(),
    })
    .await?;

    let error = transport
        .receive()
        .await
        .expect_err("a dropped subscription connection must fail the receive");
    assert_eq!(error.code(), ErrorCode::Transient);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn request_server_next_fails_when_the_subscription_connection_drops() -> CatgaResult<()> {
    let (url, server) = spawn_mock_redis(SubscribeReply::ConfirmThenClose).await?;
    let mut requests = RedisRequestServer::connect(&url, "catga-test-mock-requests").await?;

    let error = match requests.next().await {
        Ok(_) => panic!("a dropped subscription connection must fail the request wait"),
        Err(error) => error,
    };
    assert_eq!(error.code(), ErrorCode::Transient);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn request_client_fails_when_the_reply_connection_drops() -> CatgaResult<()> {
    let (url, server) = spawn_mock_redis(SubscribeReply::ConfirmThenClose).await?;
    let client = RedisRequestClient::connect(&url)?;

    let error = client
        .request_to(
            "catga-test-mock-requests",
            request(1),
            Duration::from_secs(5),
        )
        .await
        .expect_err("a dropped reply connection must fail the request");
    assert_eq!(error.code(), ErrorCode::Transient);
    server.abort();
    Ok(())
}

#[cfg(feature = "streams-rpc")]
mod streams_rpc {
    use std::{future::pending, sync::Arc};

    use super::*;
    use catga_core::{Delivery, Destination, DestinationTransport};
    use catga_redis::RedisStreamsRequestClient;

    /// A transport whose durable send completes without any broker work.
    struct NoopTransport;

    #[async_trait::async_trait]
    impl MessageTransport for NoopTransport {
        async fn publish(&self, _: Envelope) -> CatgaResult<()> {
            Ok(())
        }

        async fn receive(&self) -> CatgaResult<Delivery> {
            pending().await
        }
    }

    #[async_trait::async_trait]
    impl DestinationTransport for NoopTransport {
        async fn send_to(&self, _: &Destination, _: Envelope) -> CatgaResult<()> {
            Ok(())
        }

        async fn receive_from(&self, _: &Destination) -> CatgaResult<Delivery> {
            pending().await
        }
    }

    /// A transport whose durable send always fails.
    struct FailingTransport;

    #[async_trait::async_trait]
    impl MessageTransport for FailingTransport {
        async fn publish(&self, _: Envelope) -> CatgaResult<()> {
            Err(CatgaError::new(
                ErrorCode::Unavailable,
                "durable send failed by test",
            ))
        }

        async fn receive(&self) -> CatgaResult<Delivery> {
            pending().await
        }
    }

    #[async_trait::async_trait]
    impl DestinationTransport for FailingTransport {
        async fn send_to(&self, _: &Destination, _: Envelope) -> CatgaResult<()> {
            Err(CatgaError::new(
                ErrorCode::Unavailable,
                "durable send failed by test",
            ))
        }

        async fn receive_from(&self, _: &Destination) -> CatgaResult<Delivery> {
            pending().await
        }
    }

    use std::sync::atomic::{AtomicBool, Ordering};

    /// A transport whose durable send starts but never resolves.
    struct HangingTransport {
        send_started: Arc<AtomicBool>,
        send_dropped: Arc<AtomicBool>,
    }

    /// Records when the pending send future is dropped so the test can prove the
    /// cancelled send does not linger past the request budget.
    struct SendProbe(Arc<AtomicBool>);

    impl Drop for SendProbe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl MessageTransport for HangingTransport {
        async fn publish(&self, _: Envelope) -> CatgaResult<()> {
            pending().await
        }

        async fn receive(&self) -> CatgaResult<Delivery> {
            pending().await
        }
    }

    #[async_trait::async_trait]
    impl DestinationTransport for HangingTransport {
        async fn send_to(&self, _: &Destination, _: Envelope) -> CatgaResult<()> {
            self.send_started.store(true, Ordering::SeqCst);
            let _probe = SendProbe(Arc::clone(&self.send_dropped));
            pending().await
        }

        async fn receive_from(&self, _: &Destination) -> CatgaResult<Delivery> {
            pending().await
        }
    }

    #[tokio::test]
    async fn streams_rpc_maps_a_failed_reply_subscription_to_unavailable() -> CatgaResult<()> {
        let (url, server) = spawn_mock_redis(SubscribeReply::Fail).await?;
        let client = RedisStreamsRequestClient::new(Arc::new(NoopTransport), &url)?;

        let error = client
            .request_to("orders", request(1), Duration::from_secs(5))
            .await
            .expect_err("a failed reply subscription must fail the request");
        assert_eq!(error.code(), ErrorCode::Unavailable);
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn streams_rpc_fails_when_the_reply_connection_drops() -> CatgaResult<()> {
        let (url, server) = spawn_mock_redis(SubscribeReply::ConfirmThenClose).await?;
        let client = RedisStreamsRequestClient::new(Arc::new(NoopTransport), &url)?;

        let error = client
            .request_to("orders", request(1), Duration::from_secs(5))
            .await
            .expect_err("a dropped reply connection must fail the request");
        assert_eq!(error.code(), ErrorCode::Transient);
        server.abort();
        Ok(())
    }

    /// A durable send that never resolves is not bounded by the request budget today;
    /// a failing send must still propagate promptly instead of hanging.
    #[tokio::test]
    async fn streams_rpc_propagates_durable_send_failures() -> CatgaResult<()> {
        let (url, server) = spawn_mock_redis(SubscribeReply::Confirm).await?;
        let client = RedisStreamsRequestClient::new(Arc::new(FailingTransport), &url)?;

        let error = client
            .request_to("orders", request(1), Duration::from_secs(5))
            .await
            .expect_err("a failed durable send must propagate");
        assert_eq!(error.code(), ErrorCode::Unavailable);
        server.abort();
        Ok(())
    }

    #[tokio::test]
    async fn streams_rpc_bounds_a_never_resolving_durable_send() -> CatgaResult<()> {
        let (url, server) = spawn_mock_redis(SubscribeReply::Confirm).await?;
        let send_started = Arc::new(AtomicBool::new(false));
        let send_dropped = Arc::new(AtomicBool::new(false));
        let client = RedisStreamsRequestClient::new(
            Arc::new(HangingTransport {
                send_started: Arc::clone(&send_started),
                send_dropped: Arc::clone(&send_dropped),
            }),
            &url,
        )?;

        let result = tokio::time::timeout(
            Duration::from_secs(2),
            client.request_to("orders", request(1), Duration::from_millis(20)),
        )
        .await
        .expect("a never-resolving durable send must stay inside the request budget");

        let error = result.expect_err("a never-resolving durable send must time out");
        assert_eq!(error.code(), ErrorCode::Timeout);
        assert!(send_started.load(Ordering::SeqCst));
        assert!(
            send_dropped.load(Ordering::SeqCst),
            "the cancelled durable send must be dropped instead of lingering"
        );
        server.abort();
        Ok(())
    }
}
