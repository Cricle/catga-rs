//! Native NATS request/reply integration tests.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, Envelope, ErrorCode, Handler, MessageMetadata, Request,
    SnowflakeIdGenerator, SnowflakeLayout,
};
use catga_nats::{NatsRequestClient, NatsRequestServer};
use serde::{Deserialize, Serialize};

#[path = "support/nats_e2e.rs"]
mod nats_e2e;

#[derive(Deserialize, Serialize, catga_core::Message)]
struct CreateOrder {
    order_id: u64,
}

impl Request for CreateOrder {
    type Response = OrderCreated;
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct OrderCreated {
    order_id: u64,
}

struct CreateOrderHandler;

#[async_trait]
impl Handler<CreateOrder> for CreateOrderHandler {
    async fn handle(&self, request: CreateOrder) -> CatgaResult<OrderCreated> {
        if request.order_id == 0 {
            return Err(CatgaError::new(ErrorCode::Conflict, "order already exists"));
        }
        Ok(OrderCreated {
            order_id: request.order_id,
        })
    }
}

#[test]
fn request_e2e_subjects_are_unique() {
    assert_ne!(subject("unique"), subject("unique"));
}

#[tokio::test]
async fn concurrent_requests_keep_responses_on_their_private_nats_inboxes() {
    let server = server().await;
    let subject = subject("raw");
    let mut responder = NatsRequestServer::connect(&server, &subject).await.unwrap();
    let client = Arc::new(NatsRequestClient::connect(&server, &subject).await.unwrap());

    let responder_task = tokio::spawn(async move {
        for _ in 0..8 {
            let request = responder.next().await.unwrap();
            let response = {
                let envelope = request.envelope();
                Envelope::new(
                    envelope.id(),
                    "order.created.response",
                    envelope.payload().to_vec(),
                    MessageMetadata::new(
                        envelope.metadata().message_id(),
                        envelope.metadata().correlation_id(),
                    ),
                )
            };
            request.respond(response).await.unwrap();
        }
    });

    let requests = (1_u64..=8).map(|id| {
        let client = Arc::clone(&client);
        async move {
            client
                .request(
                    Envelope::new(
                        id,
                        "order.created",
                        vec![id as u8],
                        MessageMetadata::new(id, Some(id)),
                    ),
                    Duration::from_secs(2),
                )
                .await
        }
    });
    let responses = futures::future::try_join_all(requests).await.unwrap();
    responder_task.await.unwrap();

    for (id, response) in (1_u64..=8).zip(responses) {
        assert_eq!(response.id(), id);
        assert_eq!(response.payload(), [id as u8]);
        assert_eq!(response.metadata().correlation_id(), Some(id));
    }
    server
        .close()
        .await
        .expect("the managed NATS test container is removed");
}

#[tokio::test]
async fn raw_request_rejects_a_zero_timeout_before_publishing() {
    let server = server().await;
    let client = NatsRequestClient::connect(&server, &subject("timeout"))
        .await
        .unwrap();

    let error = client
        .request(
            Envelope::new(1, "order.created", vec![1], MessageMetadata::new(1, None)),
            Duration::ZERO,
        )
        .await
        .expect_err("zero timeout must not publish a request");

    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(
        error.message(),
        "NATS request timeout must be greater than zero"
    );
    server
        .close()
        .await
        .expect("the managed NATS test container is removed");
}

#[tokio::test]
async fn typed_client_round_trips_without_manual_envelopes() {
    let server = server().await;
    let subject = subject("typed");
    let mut responder = NatsRequestServer::connect(&server, &subject).await.unwrap();
    let client = typed_client(&server, &subject).await;

    let responder_task = tokio::spawn(async move {
        let request = responder.next().await.unwrap();
        let order: CreateOrder = request.decode().unwrap();
        request
            .respond_value(&OrderCreated {
                order_id: order.order_id,
            })
            .await
            .unwrap();
    });

    let response = client
        .request(&CreateOrder { order_id: 42 }, Duration::from_secs(2))
        .await
        .unwrap();
    responder_task.await.unwrap();

    assert_eq!(response, OrderCreated { order_id: 42 });
    server
        .close()
        .await
        .expect("the managed NATS test container is removed");
}

#[tokio::test]
async fn typed_client_returns_the_remote_catga_error() {
    let server = server().await;
    let subject = subject("remote-error");
    let mut responder = NatsRequestServer::connect(&server, &subject).await.unwrap();
    let client = typed_client(&server, &subject).await;

    let responder_task = tokio::spawn(async move {
        responder
            .next()
            .await
            .unwrap()
            .respond_error(CatgaError::new(ErrorCode::Conflict, "order already exists"))
            .await
            .unwrap();
    });

    let error = client
        .request(&CreateOrder { order_id: 42 }, Duration::from_secs(2))
        .await
        .expect_err("remote failure must be returned to the caller");
    responder_task.await.unwrap();

    assert_eq!(error.code(), ErrorCode::Conflict);
    assert_eq!(error.message(), "order already exists");
    server
        .close()
        .await
        .expect("the managed NATS test container is removed");
}

#[tokio::test]
async fn request_server_adapts_existing_handlers_and_preserves_handler_errors() {
    let server = server().await;
    let subject = subject("handler");
    let mut responder = NatsRequestServer::connect(&server, &subject).await.unwrap();
    let client = typed_client(&server, &subject).await;

    let responder_task = tokio::spawn(async move {
        let handler = CreateOrderHandler;
        responder
            .handle_next::<CreateOrder, _>(&handler)
            .await
            .unwrap();
    });

    let error = client
        .request(&CreateOrder { order_id: 0 }, Duration::from_secs(2))
        .await
        .expect_err("handler errors must cross the NATS request boundary");
    responder_task.await.unwrap();

    assert_eq!(error.code(), ErrorCode::Conflict);
    assert_eq!(error.message(), "order already exists");
    server
        .close()
        .await
        .expect("the managed NATS test container is removed");
}

async fn server() -> nats_e2e::NatsTestLease {
    nats_e2e::server_url().await
}

fn subject(suffix: &str) -> String {
    static NEXT_SUBJECT: AtomicU64 = AtomicU64::new(0);

    format!(
        "catga.rpc.{suffix}.{}.{}",
        std::process::id(),
        NEXT_SUBJECT.fetch_add(1, Ordering::Relaxed)
    )
}

async fn typed_client(server: &str, subject: &str) -> catga_nats::NatsTypedRequestClient {
    let generator = Arc::new(SnowflakeIdGenerator::new(1, SnowflakeLayout::default()).unwrap());
    NatsRequestClient::connect(server, subject)
        .await
        .unwrap()
        .typed(generator)
        .unwrap()
}
