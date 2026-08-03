//! Service-gated contract coverage for Redis Streams request/reply.

#![cfg(feature = "streams-rpc")]

use std::{sync::Arc, time::Duration};

use catga_core::codec::memorypack::{MemoryPackCodec, MemoryPackRpcResponse};
use catga_core::{
    CatgaError, CatgaResult, Destination, DestinationTransport, Envelope, ErrorCode,
    MessageMetadata,
};
use catga_redis::{
    RedisConfig, RedisStreamsRequestClient, RedisStreamsRequestServer, RedisTransport,
};

#[path = "support/service_url.rs"]
mod service_url;

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

fn request(id: u64) -> Envelope {
    Envelope::new(
        id,
        "catga.test.request",
        vec![1, 2, 3],
        MessageMetadata::new(id, Some(id)),
    )
}

fn reply(request: &Envelope) -> Envelope {
    Envelope::new(
        request.id(),
        "catga.test.reply",
        vec![4, 5, 6],
        MessageMetadata::new(request.id(), request.metadata().correlation_id()),
    )
}

#[tokio::test]
async fn streams_rpc_returns_a_successful_reply_and_acknowledges_ingress() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let label = uuid::Uuid::new_v4().to_string();
    let transport = transport(&url, &label).await?;
    let destination = Destination::parse(format!("catga-test-rpc-{label}"))?;
    let server = Arc::new(RedisStreamsRequestServer::new(
        Arc::clone(&transport),
        destination.clone(),
        &url,
    )?);
    let client = RedisStreamsRequestClient::new(Arc::clone(&transport), &url)?;
    let worker = {
        let server = Arc::clone(&server);
        tokio::spawn(async move {
            let received = server.next().await?;
            let response = reply(received.envelope());
            received.respond(response).await
        })
    };

    let response = client
        .request_to(destination.as_str(), request(1), Duration::from_secs(2))
        .await?;
    assert_eq!(response.payload(), &[4, 5, 6]);
    worker
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))??;
    Ok(())
}

#[tokio::test]
async fn streams_rpc_returns_structured_remote_errors() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let label = uuid::Uuid::new_v4().to_string();
    let transport = transport(&url, &label).await?;
    let destination = Destination::parse(format!("catga-test-rpc-{label}"))?;
    let server = Arc::new(RedisStreamsRequestServer::new(
        Arc::clone(&transport),
        destination.clone(),
        &url,
    )?);
    let client = RedisStreamsRequestClient::new(Arc::clone(&transport), &url)?;
    let worker = {
        let server = Arc::clone(&server);
        tokio::spawn(async move {
            server
                .next()
                .await?
                .respond_error(CatgaError::new(ErrorCode::Validation, "rejected"))
                .await
        })
    };

    let response = client
        .request_to(destination.as_str(), request(2), Duration::from_secs(2))
        .await?;
    let decoded: MemoryPackRpcResponse<()> =
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
async fn streams_rpc_times_out_when_no_server_replies() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let label = uuid::Uuid::new_v4().to_string();
    let transport = transport(&url, &label).await?;
    let client = RedisStreamsRequestClient::new(transport, &url)?;

    let error = client
        .request_to(
            format!("catga-test-rpc-{label}").as_str(),
            request(3),
            Duration::from_millis(20),
        )
        .await
        .expect_err("a request without a server must time out");
    assert_eq!(error.code(), ErrorCode::Timeout);
    Ok(())
}

#[tokio::test]
async fn streams_rpc_redelivers_when_response_cannot_be_prepared() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let label = uuid::Uuid::new_v4().to_string();
    let transport = transport(&url, &label).await?;
    let destination = Destination::parse(format!("catga-test-rpc-{label}"))?;
    let server = RedisStreamsRequestServer::new(Arc::clone(&transport), destination.clone(), &url)?;
    transport.send_to(&destination, request(4)).await?;

    let failed_response = server
        .next()
        .await?
        .respond(reply(&request(4)))
        .await
        .expect_err("a request without reply_to cannot be answered");
    assert_eq!(failed_response.code(), ErrorCode::Validation);
    let redelivered = server.next().await?;
    assert!(redelivered.attempts() >= 2);
    redelivered.nack().await?;
    Ok(())
}
