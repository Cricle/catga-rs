//! Integration tests for `catga_raft::transport::connection` (ConnectionPool).
//!
//! The `ConnectionPool` type and its associated constants are part of the
//! crate's public API (re-exported at the crate root and under
//! `catga_raft::transport`), so they are exercised directly here.
//!
//! Network tests bind ephemeral TCP listeners on `127.0.0.1:0` to avoid
//! fixed ports. A bare TCP listener is sufficient because tonic's
//! `Endpoint::connect` only requires the TCP connection to be established;
//! the HTTP/2 handshake is deferred until the first request.

use std::net::SocketAddr;

use tokio::net::TcpListener;

use catga_raft::transport::connection::{DEFAULT_POOL_SIZE, MAX_POOL_SIZE};
use catga_raft::{CatgaRaftError, ConnectionPool};

/// Bind an ephemeral TCP listener and spawn an accept loop that drops
/// accepted sockets. Returns the endpoint URL and the accept task handle.
async fn start_dummy_listener() -> (String, SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("local addr");
    let handle = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((_socket, _peer)) => {
                    // Drop the socket immediately; tests never send requests.
                }
                Err(_) => break,
            }
        }
    });
    (format!("http://{}", addr), addr, handle)
}

// ==================== Construction / defaults ====================

#[test]
fn conn_pool_new_stores_endpoint_and_defaults() {
    let pool = ConnectionPool::new("http://127.0.0.1:9999".to_string(), DEFAULT_POOL_SIZE);

    assert_eq!(pool.endpoint(), "http://127.0.0.1:9999");
    assert_eq!(pool.pool_size(), DEFAULT_POOL_SIZE);
    assert_eq!(pool.connection_count(), 0);
    assert!(pool.is_empty());
    assert!(!pool.is_full());
}

#[test]
fn conn_pool_constants_have_expected_values() {
    assert_eq!(DEFAULT_POOL_SIZE, 4);
    assert_eq!(MAX_POOL_SIZE, 16);
}

#[test]
fn conn_pool_size_is_clamped_to_bounds() {
    let below = ConnectionPool::new("http://127.0.0.1:1".to_string(), 0);
    assert_eq!(below.pool_size(), 1);

    let above = ConnectionPool::new("http://127.0.0.1:1".to_string(), MAX_POOL_SIZE + 1024);
    assert_eq!(above.pool_size(), MAX_POOL_SIZE);

    let in_range = ConnectionPool::new("http://127.0.0.1:1".to_string(), 7);
    assert_eq!(in_range.pool_size(), 7);
}

#[test]
fn conn_pool_debug_includes_state_summary() {
    let pool = ConnectionPool::new("http://127.0.0.1:7777".to_string(), 2);
    let debug = format!("{:?}", pool);

    assert!(debug.contains("ConnectionPool"));
    assert!(debug.contains("http://127.0.0.1:7777"));
    assert!(debug.contains("pool_size"));
    assert!(debug.contains("active_connections"));
}

#[test]
fn conn_pool_return_channel_is_noop_on_empty_pool() {
    // Build a channel endpoint string; return_channel on an empty pool must
    // not panic and must not change observable pool state.
    let pool = ConnectionPool::new("http://127.0.0.1:0".to_string(), 2);
    assert!(pool.is_empty());

    // We cannot easily fabricate a tonic::Channel without connecting, so
    // verify the no-op contract through state only after a real connection
    // in the async tests below. Here we just assert the initial state.
    assert_eq!(pool.connection_count(), 0);
    assert_eq!(pool.pool_size(), 2);
}

// ==================== Cloning / shared state ====================

#[tokio::test]
async fn conn_pool_clone_shares_underlying_state() {
    let (endpoint, _addr, handle) = start_dummy_listener().await;

    let pool = ConnectionPool::new(endpoint, 4);
    let cloned = pool.clone();
    assert_eq!(cloned.endpoint(), pool.endpoint());
    assert_eq!(cloned.pool_size(), pool.pool_size());

    // A channel acquired through the clone is visible via the original.
    let _channel = cloned.get_channel().await.expect("get_channel via clone");
    assert_eq!(pool.connection_count(), 1);
    assert_eq!(cloned.connection_count(), 1);

    handle.abort();
}

// ==================== Happy paths (real connections) ====================

#[tokio::test]
async fn conn_pool_get_channel_connects_to_listener() {
    let (endpoint, _addr, handle) = start_dummy_listener().await;

    let pool = ConnectionPool::new(endpoint.clone(), 4);
    let _channel = pool.get_channel().await.expect("get_channel succeeds");
    assert_eq!(pool.endpoint(), endpoint);

    assert_eq!(pool.connection_count(), 1);
    assert!(!pool.is_empty());
    assert!(!pool.is_full());

    handle.abort();
}

#[tokio::test]
async fn conn_pool_get_channel_reuses_cached_channel_round_robin() {
    let (endpoint, _addr, handle) = start_dummy_listener().await;

    let pool = ConnectionPool::new(endpoint, 4);

    // First call creates and caches a channel; subsequent calls must serve
    // it from the cache via round-robin instead of opening new connections.
    let c1 = pool.get_channel().await.expect("first get_channel");
    let c2 = pool.get_channel().await.expect("second get_channel");
    let c3 = pool.get_channel().await.expect("third get_channel");

    assert_eq!(pool.connection_count(), 1, "cached channel must be reused");

    // return_channel is a documented no-op today; it must not disturb state.
    pool.return_channel(c1);
    pool.return_channel(c2);
    pool.return_channel(c3);
    assert_eq!(pool.connection_count(), 1);

    handle.abort();
}

#[tokio::test]
async fn conn_pool_is_full_with_pool_size_one() {
    let (endpoint, _addr, handle) = start_dummy_listener().await;

    let pool = ConnectionPool::new(endpoint, 1);
    assert!(!pool.is_full());

    let _channel = pool.get_channel().await.expect("get_channel");
    assert_eq!(pool.connection_count(), 1);
    assert!(pool.is_full());
    assert!(!pool.is_empty());

    handle.abort();
}

#[tokio::test]
async fn conn_pool_clear_drops_connections_and_allows_reconnect() {
    let (endpoint, _addr, handle) = start_dummy_listener().await;

    let pool = ConnectionPool::new(endpoint, 4);
    let _channel = pool.get_channel().await.expect("get_channel");
    assert_eq!(pool.connection_count(), 1);

    pool.clear();
    assert_eq!(pool.connection_count(), 0);
    assert!(pool.is_empty());
    assert!(!pool.is_full());

    // Pool is reusable after clear: a fresh connection must succeed.
    let _channel = pool.get_channel().await.expect("get_channel after clear");
    assert_eq!(pool.connection_count(), 1);

    handle.abort();
}

#[tokio::test]
async fn conn_pool_concurrent_get_channel_stays_within_pool_size() {
    let (endpoint, _addr, handle) = start_dummy_listener().await;

    let pool = ConnectionPool::new(endpoint, 4);

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let p = pool.clone();
        tasks.push(tokio::spawn(async move { p.get_channel().await }));
    }

    let mut ok = 0;
    for t in tasks {
        let result = t.await.expect("task join");
        assert!(
            result.is_ok(),
            "concurrent get_channel failed: {:?}",
            result.err()
        );
        ok += 1;
    }
    assert_eq!(ok, 8);

    // The push path checks capacity under the write lock, so the cache can
    // never exceed pool_size even under concurrent creation.
    assert!(pool.connection_count() >= 1);
    assert!(pool.connection_count() <= pool.pool_size());

    handle.abort();
}

// ==================== Error cases ====================

#[tokio::test]
async fn conn_pool_get_channel_rejects_invalid_endpoint() {
    // Whitespace makes the URI unparseable by tonic's Endpoint::from_shared.
    let pool = ConnectionPool::new("not a valid uri".to_string(), 2);

    let err = pool
        .get_channel()
        .await
        .expect_err("must fail on invalid endpoint");
    match err {
        CatgaRaftError::Transport(msg) => {
            assert!(
                msg.contains("invalid endpoint"),
                "unexpected message: {}",
                msg
            );
        }
        other => panic!("expected Transport error, got {:?}", other),
    }
    assert_eq!(pool.connection_count(), 0);
    assert!(pool.is_empty());
}

#[tokio::test]
async fn conn_pool_get_channel_fails_when_nothing_is_listening() {
    // Bind an ephemeral port, capture it, then drop the listener so the
    // subsequent connect attempt is refused on loopback (no long timeout).
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("local addr");
    drop(listener);

    let pool = ConnectionPool::new(format!("http://{}", addr), 2);
    let err = pool
        .get_channel()
        .await
        .expect_err("must fail on refused connection");

    match err {
        CatgaRaftError::Transport(msg) => {
            assert!(
                msg.contains("failed to connect to"),
                "unexpected message: {}",
                msg
            );
            assert!(
                msg.contains(&addr.to_string()),
                "message should name the endpoint"
            );
        }
        other => panic!("expected Transport error, got {:?}", other),
    }
    assert!(pool.is_empty());
}
