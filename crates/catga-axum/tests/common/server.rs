//! Shared loopback HTTP server fixture for catga-axum contract tests.

use axum::Router;
use tokio::{net::TcpListener, task::JoinHandle};

/// Serves `app` on an ephemeral loopback port and returns its base URL.
///
/// The returned handle is aborted by the test when the server must stop;
/// dropping it leaves the server running until the test process exits.
pub async fn spawn_app(app: Router) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener must bind");
    let address = format!(
        "http://{}",
        listener.local_addr().expect("listener address")
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("loopback server runs");
    });
    (address, server)
}
