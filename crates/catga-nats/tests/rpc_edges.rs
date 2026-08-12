//! Request/reply edge contracts: application-owned client constructors, transport-trait
//! dispatch, request validation, typed handler success/failure/decode-failure routing, and
//! timeout behavior.

#[path = "support/envelopes.rs"]
mod envelopes;
#[path = "support/names.rs"]
mod names;
#[path = "support/nats_server.rs"]
mod nats_server;
#[path = "support/typed_rpc.rs"]
mod typed_rpc;

use std::sync::Arc;
use std::time::Duration;

use catga_core::codec::memorypack::MemoryPackCodec;
use catga_core::codec::memorypack::MemoryPackRpcResponse;
use catga_core::{
    CatgaError, CatgaResult, ErrorCode, QualityOfService, RequestTransport, SnowflakeIdGenerator,
    SnowflakeLayout,
};
use catga_nats::{NatsRequestClient, NatsRequestServer};
use envelopes::envelope;
use futures::StreamExt;
use names::unique;
use nats_server::{server_url, test_error};
use typed_rpc::{DoubleRequest, DoublingHandler, RejectingHandler};

fn id_generator(worker_id: u32) -> CatgaResult<Arc<SnowflakeIdGenerator>> {
    Ok(Arc::new(SnowflakeIdGenerator::new(
        worker_id,
        SnowflakeLayout::default(),
    )?))
}

async fn raw_client() -> CatgaResult<async_nats::Client> {
    async_nats::connect(server_url())
        .await
        .map_err(|error| test_error("connect raw request client", error))
}

async fn join<T>(handle: tokio::task::JoinHandle<CatgaResult<T>>) -> CatgaResult<T> {
    handle
        .await
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn application_owned_clients_roundtrip_through_the_transport_trait() -> CatgaResult<()> {
    let subject = unique("catga.rpc.injected");

    // Both application-owned constructors validate and subscribe before returning.
    let server = NatsRequestServer::connect_with_client(raw_client().await?, &subject).await?;
    let client = NatsRequestClient::connect_with_client(raw_client().await?, &subject)?;

    let request_env = envelope(701, QualityOfService::AtLeastOnce);
    let worker = tokio::spawn(async move {
        let mut server = server;
        let request = server.next().await?;
        request
            .respond(envelope(702, QualityOfService::AtLeastOnce))
            .await
    });

    // The RequestTransport trait entry point dispatches to the same request path.
    let response =
        RequestTransport::request(&client, &subject, request_env, Duration::from_secs(5)).await?;
    assert_eq!(response.id(), 702);
    join(worker).await?;

    // from_client shares the same initializer.
    let second = NatsRequestServer::from_client(raw_client().await?, &subject).await?;
    let second_client = NatsRequestClient::from_client(raw_client().await?, &subject)?;
    let worker = tokio::spawn(async move {
        let mut server = second;
        let request = server.next().await?;
        let echo = request.envelope().clone();
        request.respond(echo).await
    });
    let echoed = second_client
        .request(
            envelope(703, QualityOfService::AtLeastOnce),
            Duration::from_secs(5),
        )
        .await?;
    assert_eq!(echoed.id(), 703);
    join(worker).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn requests_validate_subject_and_timeout_before_sending() -> CatgaResult<()> {
    let client = NatsRequestClient::from_client(raw_client().await?, "catga.rpc.validation")?;
    let request_env = envelope(704, QualityOfService::AtLeastOnce);

    assert!(matches!(
        client.request_to(" ", request_env.clone(), Duration::from_secs(1)).await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert!(matches!(
        client.request_to("catga.rpc.validation", request_env, Duration::ZERO).await,
        Err(error) if error.code() == ErrorCode::Validation
    ));

    // A subject without any responder fails fast through the broker's no-responders signal.
    assert!(matches!(
        client.request(envelope(705, QualityOfService::AtLeastOnce), Duration::from_secs(5)).await,
        Err(error) if error.code() == ErrorCode::Transient
    ));

    // A responder that never replies is reported as a client-side timeout.
    let silent = raw_client().await?;
    let mut silent_subscription = silent
        .subscribe("catga.rpc.silent")
        .await
        .map_err(|error| test_error("subscribe silent responder", error))?;
    silent
        .flush()
        .await
        .map_err(|error| test_error("flush silent responder", error))?;
    let held = tokio::spawn(async move { silent_subscription.next().await });
    assert!(matches!(
        client.request_to("catga.rpc.silent", envelope(706, QualityOfService::AtLeastOnce), Duration::from_millis(100)).await,
        Err(error) if error.code() == ErrorCode::Timeout
    ));
    assert!(
        held.await
            .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?
            .is_some()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn typed_requests_route_success_failure_and_decode_errors() -> CatgaResult<()> {
    let subject = unique("catga.rpc.typed");
    let server = NatsRequestServer::connect(server_url().as_str(), &subject).await?;
    let client = NatsRequestClient::connect(server_url().as_str(), &subject)
        .await?
        .typed(id_generator(1)?)?;

    // One worker answers the three requests in arrival order.
    let worker = tokio::spawn(async move {
        let mut server = server;
        server.handle_next(&DoublingHandler).await?;
        server.handle_next(&RejectingHandler).await?;
        server
            .handle_next::<DoubleRequest, _>(&DoublingHandler)
            .await
    });

    // A doubling handler answers through the typed success envelope.
    assert_eq!(
        client
            .request(&DoubleRequest(21), Duration::from_secs(5))
            .await?,
        42
    );

    // A rejecting handler propagates its structured failure to the caller.
    assert!(matches!(
        client.request(&DoubleRequest(7), Duration::from_secs(5)).await,
        Err(error) if error.code() == ErrorCode::Validation
    ));

    // An undecodable payload is answered with a structured failure, not a hang.
    let raw = NatsRequestClient::from_client(raw_client().await?, &subject)?;
    let response = raw
        .request(
            envelope(706, QualityOfService::AtLeastOnce),
            Duration::from_secs(5),
        )
        .await?;
    let decoded: MemoryPackRpcResponse<()> =
        MemoryPackCodec::default().decode_rpc_response(response.payload())?;
    assert!(matches!(decoded, MemoryPackRpcResponse::Failure(_)));
    join(worker).await?;
    Ok(())
}
