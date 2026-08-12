//! Contract tests for the composable tower layers: correlation and trace-context
//! propagation through real HTTP requests.

#[path = "common/server.rs"]
mod server;

use axum::{Router, routing::get};
use catga_axum::{CORRELATION_ID_HEADER, CorrelationLayer, TraceContextLayer};
use catga_core::{current_correlation_id, current_correlation_value, current_transport_context};
use http::StatusCode;

#[tokio::test]
async fn correlation_layer_preserves_an_inbound_value_and_echoes_it() {
    let app = Router::new()
        .route(
            "/probe",
            get(|| async {
                format!(
                    "{}|{:?}",
                    current_correlation_value().unwrap_or_default(),
                    current_correlation_id()
                )
            }),
        )
        .layer(CorrelationLayer::new());
    let (base, server) = server::spawn_app(app).await;
    let client = reqwest::Client::new();

    // A numeric inbound value drives both the opaque value and the numeric scope.
    let response = client
        .get(format!("{base}/probe"))
        .header(CORRELATION_ID_HEADER, "77")
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[CORRELATION_ID_HEADER], "77");
    assert_eq!(response.text().await.expect("body"), "77|Some(77)");

    // A non-numeric inbound value is preserved opaquely; the numeric scope falls
    // back to a generated identifier.
    let response = client
        .get(format!("{base}/probe"))
        .header(CORRELATION_ID_HEADER, "corr-abc")
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.headers()[CORRELATION_ID_HEADER], "corr-abc");
    let body = response.text().await.expect("body");
    assert!(
        body.starts_with("corr-abc|Some("),
        "numeric scope must still populate, got {body}"
    );

    // Without a header a monotonic numeric value is generated, scoped, and echoed.
    let response = client
        .get(format!("{base}/probe"))
        .send()
        .await
        .expect("request succeeds");
    let echoed = response.headers()[CORRELATION_ID_HEADER]
        .to_str()
        .expect("echo header")
        .to_string();
    let body = response.text().await.expect("body");
    assert_eq!(body, format!("{echoed}|Some({echoed})"));

    server.abort();
}

#[tokio::test]
async fn trace_context_layer_scopes_only_valid_inbound_contexts() {
    let app = Router::new()
        .route(
            "/probe",
            get(|| async {
                current_transport_context()
                    .and_then(|context| {
                        context.headers().and_then(|headers| {
                            headers
                                .iter()
                                .find(|(key, _)| key.eq_ignore_ascii_case("traceparent"))
                                .map(|(_, value)| value.to_string())
                        })
                    })
                    .unwrap_or_else(|| "unscoped".to_string())
            }),
        )
        .layer(TraceContextLayer::new());
    let (base, server) = server::spawn_app(app).await;
    let client = reqwest::Client::new();

    let parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let response = client
        .get(format!("{base}/probe"))
        .header("traceparent", parent)
        .header("tracestate", "congo=t61rcWkgMzE")
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.text().await.expect("body"), parent);

    // An invalid traceparent leaves the handler unscoped.
    let response = client
        .get(format!("{base}/probe"))
        .header("traceparent", "not-a-traceparent")
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.text().await.expect("body"), "unscoped");

    // No headers at all also stays unscoped.
    let response = client
        .get(format!("{base}/probe"))
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.text().await.expect("body"), "unscoped");

    server.abort();
}
