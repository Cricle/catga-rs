//! Shared harness for the catga-sorock integration tests: loopback node
//! configs and a bounded polling helper for eventually-consistent assertions.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use catga_sorock::SorockNodeConfig;

/// Upper bound for eventually-consistent assertions (elections, replication).
pub(crate) const WAIT_TIMEOUT: Duration = Duration::from_secs(25);

/// Delay between condition polls inside [`wait_until`].
pub(crate) const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Builds a config bound to loopback with an OS-assigned port and a client
/// deadline comfortably above local election latency.
pub(crate) fn loopback_config(node_id: &str) -> SorockNodeConfig {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let mut config = SorockNodeConfig::new(node_id, bind_addr);
    config.request_timeout = Duration::from_secs(10);
    config
}

/// Polls `condition` until it holds or the wait budget expires.
pub(crate) async fn wait_until(description: &str, condition: impl Fn() -> bool) {
    let deadline = std::time::Instant::now() + WAIT_TIMEOUT;
    while !condition() {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {description}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}
