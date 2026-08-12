//! Contract tests for the [`MediatorState`] extractor: state wrapping, conversion,
//! deref dispatch, and extraction through a running router.

#[path = "common/server.rs"]
mod server;

use std::sync::Arc;

use axum::{Router, extract::Path, routing::get};
use catga_axum::MediatorState;
use catga_core::{Mediator, Message, MessageTypeId, Registry, Request, request_handler};
use http::StatusCode;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GetBalance {
    account: u64,
}
impl Message for GetBalance {}
struct GetBalanceTypeId;
impl MessageTypeId for GetBalanceTypeId {
    const NAME: &'static str = "GetBalance";
}
impl Request for GetBalance {
    type Response = u64;
    type TypeId = GetBalanceTypeId;
}

fn mediator() -> Arc<Mediator> {
    let mut registry = Registry::new();
    registry
        .register_request::<GetBalance, _>(request_handler(|query: GetBalance| async move {
            Ok(query.account * 10)
        }))
        .expect("handler registers");
    Arc::new(Mediator::new(registry))
}

#[test]
fn mediator_state_wraps_converts_and_derefs() {
    let mediator = mediator();
    let state = MediatorState::new(Arc::clone(&mediator));
    assert!(Arc::ptr_eq(state.mediator(), &mediator));

    let from_arc: MediatorState = Arc::clone(&mediator).into();
    let back: Arc<Mediator> = from_arc.into();
    assert!(Arc::ptr_eq(&back, &mediator));

    // Deref exposes mediator methods without unwrapping.
    let state = MediatorState::new(Arc::clone(&mediator));
    let _bound: &Mediator = &state;
}

#[tokio::test]
async fn mediator_state_extracts_and_dispatches_through_a_router() {
    async fn get_balance(state: MediatorState, Path(account): Path<u64>) -> String {
        state
            .send(GetBalance { account })
            .await
            .expect("dispatch succeeds")
            .to_string()
    }

    let app: Router<MediatorState> = Router::new().route("/balance/{id}", get(get_balance));
    let app: Router = app.with_state(MediatorState::new(mediator()));
    let (base, server) = server::spawn_app(app).await;

    let response = reqwest::Client::new()
        .get(format!("{base}/balance/4"))
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.expect("body"), "40");

    server.abort();
}
