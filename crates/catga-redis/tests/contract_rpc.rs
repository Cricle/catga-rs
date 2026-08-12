//! Service-backed contract coverage for Redis Pub/Sub and Streams request/reply.

use std::sync::Arc;
use std::time::Duration;

use catga_core::codec::memorypack::{MemoryPackCodec, MemoryPackRpcResponse};
use catga_core::{
    CatgaError, CatgaResult, Envelope, EnvelopeCodec, ErrorCode, MessageMetadata, RequestTransport,
    SnowflakeIdGenerator, SnowflakeLayout,
};
use catga_redis::{MemoryPackRequestClient, RedisRequestClient, RedisRequestServer};

#[path = "support/redis_err.rs"]
mod redis_err;
#[path = "support/service_url.rs"]
mod service_url;
#[path = "support/timeout_err.rs"]
mod timeout_err;
#[path = "support/typed_rpc.rs"]
mod typed_rpc;

use redis_err::map_redis_error;
use timeout_err::timeout_error;
use typed_rpc::{DoubleRequest, DoublingHandler, RejectingHandler};

fn request_envelope(id: u64) -> Envelope {
    Envelope::new(
        id,
        "catga.test.request",
        vec![1, 2, 3],
        MessageMetadata::new(id, Some(id)),
    )
}

fn reply_envelope(request: &Envelope) -> Envelope {
    Envelope::new(
        request.id(),
        "catga.test.reply",
        vec![4, 5, 6],
        MessageMetadata::new(request.id(), request.metadata().correlation_id()),
    )
}

#[tokio::test]
async fn request_reply_roundtrip_over_pubsub() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let destination = format!("catga-test-rpc-{}", uuid::Uuid::new_v4());
    let mut server = RedisRequestServer::connect(&url, &destination).await?;
    let client = RedisRequestClient::connect(&url)?;

    let worker = tokio::spawn(async move {
        let request = server.next().await?;
        assert_eq!(request.envelope().message_type(), "catga.test.request");
        let response = reply_envelope(request.envelope());
        request.respond(response).await
    });

    let response = client
        .request_to(&destination, request_envelope(501), Duration::from_secs(5))
        .await?;
    assert_eq!(response.payload(), &[4, 5, 6]);
    worker
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))??;
    Ok(())
}

#[tokio::test]
async fn request_without_reply_to_cannot_be_answered() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let destination = format!("catga-test-rpc-{}", uuid::Uuid::new_v4());
    let mut server = RedisRequestServer::connect(&url, &destination).await?;

    // Publish a bare envelope without a reply inbox.
    let raw_client = redis::Client::open(url.as_str()).map_err(map_redis_error)?;
    let mut raw = raw_client
        .get_multiplexed_async_connection()
        .await
        .map_err(map_redis_error)?;
    let payload = MemoryPackCodec::default().encode(&request_envelope(502))?;
    let _: usize = redis::AsyncCommands::publish(&mut raw, destination.as_str(), payload)
        .await
        .map_err(map_redis_error)?;

    let request = server.next().await?;
    let result = request
        .respond(reply_envelope(&request_envelope(502)))
        .await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Validation));
    Ok(())
}

#[tokio::test]
async fn typed_handle_next_returns_a_doubled_response() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let destination = format!("catga-test-rpc-{}", uuid::Uuid::new_v4());
    let mut server = RedisRequestServer::connect(&url, &destination).await?;
    let transport = Arc::new(RedisRequestClient::connect(&url)?);
    let client = MemoryPackRequestClient::new(
        transport,
        destination.clone(),
        Duration::from_secs(5),
        Arc::new(SnowflakeIdGenerator::new(1, SnowflakeLayout::default())?),
    )?;

    let worker = tokio::spawn(async move { server.handle_next(&DoublingHandler).await });

    let response = client
        .request(&DoubleRequest(21), Duration::from_secs(5))
        .await?;
    assert_eq!(response, 42);
    worker
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))??;
    Ok(())
}

#[tokio::test]
async fn typed_handle_next_propagates_handler_failures() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let destination = format!("catga-test-rpc-{}", uuid::Uuid::new_v4());
    let mut server = RedisRequestServer::connect(&url, &destination).await?;
    let transport = Arc::new(RedisRequestClient::connect(&url)?);
    let client = MemoryPackRequestClient::new(
        transport,
        destination.clone(),
        Duration::from_secs(5),
        Arc::new(SnowflakeIdGenerator::new(2, SnowflakeLayout::default())?),
    )?;

    let worker = tokio::spawn(async move { server.handle_next(&RejectingHandler).await });

    let result = client
        .request(&DoubleRequest(7), Duration::from_secs(5))
        .await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Validation));
    worker
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))??;
    Ok(())
}

#[tokio::test]
async fn typed_handle_next_reports_a_decode_failure() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let destination = format!("catga-test-rpc-{}", uuid::Uuid::new_v4());
    let mut server = RedisRequestServer::connect(&url, &destination).await?;
    let client = RedisRequestClient::connect(&url)?;

    let worker = tokio::spawn(async move {
        server
            .handle_next::<DoubleRequest, _>(&DoublingHandler)
            .await
    });

    // A three-byte payload cannot decode as the typed u64 request.
    let response = client
        .request_to(&destination, request_envelope(503), Duration::from_secs(5))
        .await?;
    let decoded: MemoryPackRpcResponse<()> =
        MemoryPackCodec::default().decode_rpc_response(response.payload())?;
    assert!(matches!(decoded, MemoryPackRpcResponse::Failure(_)));
    worker
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))??;
    Ok(())
}

#[tokio::test]
async fn request_to_times_out_without_a_server() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let destination = format!("catga-test-rpc-{}", uuid::Uuid::new_v4());
    let client = RedisRequestClient::connect(&url)?;

    let result = client
        .request_to(
            &destination,
            request_envelope(504),
            Duration::from_millis(50),
        )
        .await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Timeout));

    // The RequestTransport trait object delegates to the same bounded path.
    let via_trait: &(dyn RequestTransport + Sync) = &client;
    let result = via_trait
        .request(
            &destination,
            request_envelope(505),
            Duration::from_millis(50),
        )
        .await;
    assert!(matches!(result, Err(error) if error.code() == ErrorCode::Timeout));
    Ok(())
}

#[cfg(feature = "streams-rpc")]
mod streams_rpc {
    use super::*;
    use catga_core::{Destination, DestinationTransport};
    use catga_redis::{
        RedisConfig, RedisStreamsRequestClient, RedisStreamsRequestServer, RedisTransport,
    };

    async fn transport(url: &str, label: &str) -> CatgaResult<Arc<RedisTransport>> {
        RedisTransport::connect(RedisConfig {
            server: url.into(),
            stream: format!("catga-test-streams-rpc-{label}").into(),
            group: format!("catga-test-streams-rpc-group-{label}").into(),
            consumer: format!("catga-test-streams-rpc-consumer-{label}").into(),
        })
        .await
        .map(Arc::new)
    }

    #[tokio::test]
    async fn streams_rpc_typed_handle_next_roundtrip() -> CatgaResult<()> {
        let Some(url) = service_url::redis_url()? else {
            return Ok(());
        };
        let label = uuid::Uuid::new_v4().to_string();
        let transport = transport(&url, &label).await?;
        let destination = Destination::parse(format!("catga-test-rpc-{label}"))?;
        let server =
            RedisStreamsRequestServer::new(Arc::clone(&transport), destination.clone(), &url)?;
        assert_eq!(server.destination(), &destination);
        let client = RedisStreamsRequestClient::new(Arc::clone(&transport), &url)?;

        let worker = tokio::spawn(async move { server.handle_next(&DoublingHandler).await });

        let response = client
            .request_to(
                destination.as_str(),
                typed_request_envelope(601, &DoubleRequest(30))?,
                Duration::from_secs(5),
            )
            .await?;
        let decoded: MemoryPackRpcResponse<u64> =
            MemoryPackCodec::default().decode_rpc_response(response.payload())?;
        assert!(matches!(decoded, MemoryPackRpcResponse::Success(60)));
        worker
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))??;
        Ok(())
    }

    #[tokio::test]
    async fn streams_rpc_typed_handler_failure_replies_an_error() -> CatgaResult<()> {
        let Some(url) = service_url::redis_url()? else {
            return Ok(());
        };
        let label = uuid::Uuid::new_v4().to_string();
        let transport = transport(&url, &label).await?;
        let destination = Destination::parse(format!("catga-test-rpc-{label}"))?;
        let server =
            RedisStreamsRequestServer::new(Arc::clone(&transport), destination.clone(), &url)?;
        let client = RedisStreamsRequestClient::new(Arc::clone(&transport), &url)?;

        let worker = tokio::spawn(async move { server.handle_next(&RejectingHandler).await });

        let response = client
            .request_to(
                destination.as_str(),
                typed_request_envelope(602, &DoubleRequest(5))?,
                Duration::from_secs(5),
            )
            .await?;
        let decoded: MemoryPackRpcResponse<u64> =
            MemoryPackCodec::default().decode_rpc_response(response.payload())?;
        assert!(matches!(
            decoded,
            MemoryPackRpcResponse::Failure(error) if error.code() == ErrorCode::Validation
        ));
        worker
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))??;
        Ok(())
    }

    #[tokio::test]
    async fn streams_rpc_typed_decode_failure_replies_an_error() -> CatgaResult<()> {
        let Some(url) = service_url::redis_url()? else {
            return Ok(());
        };
        let label = uuid::Uuid::new_v4().to_string();
        let transport = transport(&url, &label).await?;
        let destination = Destination::parse(format!("catga-test-rpc-{label}"))?;
        let server =
            RedisStreamsRequestServer::new(Arc::clone(&transport), destination.clone(), &url)?;
        let client = RedisStreamsRequestClient::new(Arc::clone(&transport), &url)?;

        let worker = tokio::spawn(async move {
            server
                .handle_next::<DoubleRequest, _>(&DoublingHandler)
                .await
        });

        let response = client
            .request_to(
                destination.as_str(),
                request_envelope(603),
                Duration::from_secs(5),
            )
            .await?;
        let decoded: MemoryPackRpcResponse<u64> =
            MemoryPackCodec::default().decode_rpc_response(response.payload())?;
        assert!(matches!(decoded, MemoryPackRpcResponse::Failure(_)));
        worker
            .await
            .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))??;
        Ok(())
    }

    #[tokio::test]
    async fn streams_rpc_request_exposes_envelope_attempts_and_decode() -> CatgaResult<()> {
        let Some(url) = service_url::redis_url()? else {
            return Ok(());
        };
        let label = uuid::Uuid::new_v4().to_string();
        let transport = transport(&url, &label).await?;
        let destination = Destination::parse(format!("catga-test-rpc-{label}"))?;
        let server =
            RedisStreamsRequestServer::new(Arc::clone(&transport), destination.clone(), &url)?;

        transport
            .send_to(
                &destination,
                typed_request_envelope(604, &DoubleRequest(8))?,
            )
            .await?;
        let request = server.next().await?;
        assert_eq!(request.envelope().id(), 604);
        assert_eq!(request.attempts(), 1);
        let decoded = request.decode::<DoubleRequest>()?;
        assert_eq!(decoded.0, 8);

        // Nacking returns the delivery to the redelivery path without a reply.
        request.nack().await?;
        let redelivered = tokio::time::timeout(Duration::from_secs(5), server.next())
            .await
            .map_err(|_| timeout_error("Redis Streams redelivery timed out"))??;
        assert!(redelivered.attempts() >= 2);

        // A request whose reply inbox subscription is gone still acknowledges.
        redelivered.respond_value(&16_u64).await?;
        Ok(())
    }

    #[tokio::test]
    async fn streams_rpc_request_transport_trait_delegates() -> CatgaResult<()> {
        let Some(url) = service_url::redis_url()? else {
            return Ok(());
        };
        let label = uuid::Uuid::new_v4().to_string();
        let transport = transport(&url, &label).await?;
        let client = RedisStreamsRequestClient::new(transport, &url)?;

        let via_trait: &(dyn RequestTransport + Sync) = &client;
        let result = via_trait
            .request(
                &format!("catga-test-rpc-{label}"),
                request_envelope(605),
                Duration::from_millis(50),
            )
            .await;
        assert!(matches!(result, Err(error) if error.code() == ErrorCode::Timeout));
        Ok(())
    }

    fn typed_request_envelope(id: u64, request: &DoubleRequest) -> CatgaResult<Envelope> {
        Ok(Envelope::new(
            id,
            "catga.test.DoubleRequest",
            MemoryPackCodec::default().encode_value(request)?,
            MessageMetadata::new(id, Some(id)),
        )
        .with_reply_to(format!("catga-test-reply-inbox-{id}")))
    }
}
