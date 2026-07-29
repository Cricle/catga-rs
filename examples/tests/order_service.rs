//! End-to-end contracts for the modular order-service example.

use std::time::Duration;

use catga_examples::order_service::{OrderService, OrderServiceOptions};
use tokio::{net::TcpListener, task::JoinHandle, time::timeout};

async fn start(service: OrderService) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a loopback test listener");
    let endpoint = format!(
        "http://{}",
        listener.local_addr().expect("read listener address")
    );
    let app = service.router().expect("build order-service routes");
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve the order-service test application");
    });
    (endpoint, server)
}

#[test]
fn order_service_exposes_a_concise_default_server_entrypoint() {
    let service = OrderService::in_memory(OrderServiceOptions::default())
        .expect("construct the in-memory order service");
    let server = service.serve("127.0.0.1:0");
    drop(server);
}

#[tokio::test]
async fn order_service_e2e_records_cqrs_flow_event_and_acknowledged_delivery() {
    let service = OrderService::in_memory(OrderServiceOptions::default())
        .expect("construct the in-memory order service");
    let (endpoint, server) = start(service.clone()).await;

    let response = reqwest::Client::new()
        .post(format!("{endpoint}/orders"))
        .json(&serde_json::json!({"quantity": 2, "unit_price_cents": 1299}))
        .send()
        .await
        .expect("submit an order over HTTP");
    assert!(response.status().is_success());
    let order = response
        .json::<catga_examples::order_service::OrderAccepted>()
        .await
        .expect("decode the accepted order");
    assert_eq!(order.total_cents, 2_598);

    let delivery = timeout(Duration::from_secs(1), service.receive_completed_order())
        .await
        .expect("completed event must arrive")
        .expect("decode and acknowledge the completed event");
    assert_eq!(delivery.order_id, order.order_id);
    assert_eq!(
        service
            .completed_event_count()
            .await
            .expect("read event store"),
        1
    );
    assert_eq!(service.handled_completion_count(), 1);

    let health = reqwest::Client::new()
        .get(format!("{endpoint}/healthz"))
        .send()
        .await
        .expect("read health endpoint")
        .json::<catga_examples::order_service::OrderServiceHealth>()
        .await
        .expect("decode health document");
    assert!(health.is_leader);
    assert_eq!(health.cluster_size, 1);
    server.abort();
}

#[tokio::test]
async fn order_service_e2e_compensates_a_declined_payment_without_publishing_success() {
    let service = OrderService::in_memory(OrderServiceOptions::with_declined_payments())
        .expect("construct the in-memory order service");
    let (endpoint, server) = start(service.clone()).await;

    let response = reqwest::Client::new()
        .post(format!("{endpoint}/orders"))
        .json(&serde_json::json!({"quantity": 1, "unit_price_cents": 500}))
        .send()
        .await
        .expect("submit a declined order over HTTP");
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        service
            .completed_event_count()
            .await
            .expect("read event store"),
        0
    );
    assert_eq!(service.reserved_inventory_count(), 0);
    assert_eq!(service.captured_payment_count(), 0);
    server.abort();
}

#[tokio::test]
async fn order_service_e2e_rejects_writes_after_leadership_moves_to_another_member() {
    let service = OrderService::in_memory(OrderServiceOptions::with_members([
        "http://cluster/node-a",
        "http://cluster/node-b",
    ]))
    .expect("construct a two-member order service");
    service
        .elect_leader("node-b")
        .expect("move leadership to the peer");
    let (endpoint, server) = start(service).await;

    let response = reqwest::Client::new()
        .post(format!("{endpoint}/orders"))
        .json(&serde_json::json!({"quantity": 1, "unit_price_cents": 500}))
        .send()
        .await
        .expect("submit to a follower over HTTP");
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    server.abort();
}
