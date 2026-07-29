//! Startup contracts for the typed Axum application composition helper.

use std::sync::Arc;

use async_trait::async_trait;
use axum::{body::Body, http::Request as AxumRequest};
use catga_core::{CatgaResult, Handler, MediatorHandle, Message, Request};
use serde::{Deserialize, Serialize};
use tower::ServiceExt;

#[derive(Deserialize)]
struct Double {
    value: u64,
}

impl Message for Double {}

impl Request for Double {
    type Response = Doubled;
}

#[derive(Deserialize, Serialize)]
struct Doubled {
    value: u64,
}

struct DoubleHandler;

#[async_trait]
impl Handler<Double> for DoubleHandler {
    async fn handle(&self, request: Double) -> CatgaResult<Doubled> {
        Ok(Doubled {
            value: request.value.saturating_mul(2),
        })
    }
}

#[tokio::test]
async fn application_macro_binds_the_explicit_handle_and_builds_typed_routes() {
    let handle = MediatorHandle::new();
    let application = catga_axum::catga_application! {
        mediator_handle = handle;
        handlers {
            request Double => DoubleHandler;
        }
        routes {
            requests { @post "/double" => Double }
            events {}
        }
    }
    .expect("compose the application");

    assert!(handle.is_bound());
    assert_eq!(
        application
            .mediator()
            .send(Double { value: 21 })
            .await
            .expect("dispatch through the bound mediator")
            .value,
        42
    );
    let response = application
        .router()
        .oneshot(
            AxumRequest::post("/double")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"value":21}"#))
                .expect("build HTTP request"),
        )
        .await
        .expect("route request");
    assert!(response.status().is_success());
    let _: Arc<_> = application.mediator();
}

#[tokio::test]
async fn application_macro_does_not_require_a_handle_without_nested_dispatch() {
    let application = catga_axum::catga_application! {
        handlers {
            request Double => DoubleHandler;
        }
        routes {
            requests { @post "/double" => Double }
            events {}
        }
    }
    .expect("compose the application without nested dispatch");

    assert_eq!(
        application
            .mediator()
            .send(Double { value: 7 })
            .await
            .expect("dispatch through the application mediator")
            .value,
        14
    );
}

#[test]
fn application_macro_binds_a_handle_only_after_route_validation_succeeds() {
    let handle = MediatorHandle::new();
    let error = match catga_axum::catga_application! {
        mediator_handle = handle;
        handlers {
            request Double => DoubleHandler;
        }
        routes {
            requests { @post "double" => Double }
            events {}
        }
    } {
        Err(error) => error,
        Ok(_) => panic!("an invalid route must reject application startup"),
    };
    assert_eq!(error.code(), catga_core::ErrorCode::Validation);
    assert!(!handle.is_bound());

    let application = catga_axum::catga_application! {
        mediator_handle = handle;
        handlers {
            request Double => DoubleHandler;
        }
        routes {
            requests { @post "/double" => Double }
            events {}
        }
    }
    .expect("a corrected route can reuse the unbound handle");
    assert!(handle.is_bound());
    let _: Arc<_> = application.mediator();
}
