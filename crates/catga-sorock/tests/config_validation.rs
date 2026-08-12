//! Contract tests for [`SorockNodeConfig`]: documented defaults and the
//! startup validation of fields that cannot be checked lazily, exercised
//! through the public `SorockRuntime::start` entry point.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use catga_core::{CatgaResult, ConsensusRuntime, ConsensusStateMachine, ErrorCode};
use catga_sorock::{
    DEFAULT_PROPOSE_MAX_ATTEMPTS, DEFAULT_PROPOSE_RETRY_BACKOFF, DEFAULT_REQUEST_TIMEOUT,
    DEFAULT_SHARD_INDEX, SorockNodeConfig, SorockProposeRetry, SorockRuntime, SorockStorage,
};

fn loopback_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
}

/// Minimal machine; validation fails before the machine is ever used.
struct NoopMachine;

impl ConsensusStateMachine for NoopMachine {
    fn apply(&mut self, _index: u64, _data: &[u8]) -> CatgaResult<()> {
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        Ok(Vec::new())
    }

    fn restore(&mut self, _data: &[u8]) -> CatgaResult<()> {
        Ok(())
    }
}

#[test]
fn new_applies_documented_defaults() {
    let addr = loopback_addr();
    let config = SorockNodeConfig::new("node-1", addr);

    assert_eq!(config.node_id, "node-1");
    assert_eq!(config.bind_addr, addr);
    assert!(config.public_uri.is_none());
    assert!(config.members.is_empty());
    assert_eq!(config.shard, DEFAULT_SHARD_INDEX);
    assert_eq!(DEFAULT_SHARD_INDEX, 0);
    assert!(matches!(config.storage, SorockStorage::InMemory));
    assert_eq!(config.snapshot_interval, 0);
    assert_eq!(config.request_timeout, DEFAULT_REQUEST_TIMEOUT);
    assert_eq!(DEFAULT_REQUEST_TIMEOUT, Duration::from_secs(2));
    assert_eq!(config.propose_retry, SorockProposeRetry::default());
    assert_eq!(DEFAULT_PROPOSE_MAX_ATTEMPTS, 10);
    assert_eq!(DEFAULT_PROPOSE_RETRY_BACKOFF, Duration::from_millis(200));
    assert_eq!(
        config.propose_retry.max_attempts,
        DEFAULT_PROPOSE_MAX_ATTEMPTS
    );
    assert_eq!(config.propose_retry.backoff, DEFAULT_PROPOSE_RETRY_BACKOFF);
    assert!(
        !config.failover_watchdog,
        "the failover watchdog is opt-in because of the term-churn caveat"
    );

    assert!(
        matches!(SorockStorage::default(), SorockStorage::InMemory),
        "in-memory storage is the documented default"
    );

    let cloned = config.clone();
    assert_eq!(cloned.node_id, config.node_id);
    assert_eq!(cloned.bind_addr, config.bind_addr);
    assert!(
        format!("{config:?}").contains("node-1"),
        "Debug output must identify the node"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_node_id_is_rejected_at_startup() {
    let config = SorockNodeConfig::new("", loopback_addr());

    let error = SorockRuntime::start(config, NoopMachine)
        .await
        .err()
        .expect("an empty node id must be rejected");

    assert_eq!(error.code(), ErrorCode::Validation);
    assert!(
        error.message().contains("node id must not be empty"),
        "unexpected message: {}",
        error.message()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_public_uri_is_rejected_at_startup() {
    let mut config = SorockNodeConfig::new("node-1", loopback_addr());
    config.public_uri = Some("not a uri".to_owned());

    let error = SorockRuntime::start(config, NoopMachine)
        .await
        .err()
        .expect("an unparseable public uri must be rejected");

    assert_eq!(error.code(), ErrorCode::Validation);
    assert!(
        error.message().contains("not a valid URI"),
        "unexpected message: {}",
        error.message()
    );
    assert!(
        error.message().contains("not a uri"),
        "the offending value must be reported: {}",
        error.message()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_propose_max_attempts_is_rejected_at_startup() {
    let mut config = SorockNodeConfig::new("node-1", loopback_addr());
    config.propose_retry.max_attempts = 0;

    let error = SorockRuntime::start(config, NoopMachine)
        .await
        .err()
        .expect("a zero propose attempt budget must be rejected");

    assert_eq!(error.code(), ErrorCode::Validation);
    assert!(
        error.message().contains("max_attempts must be at least 1"),
        "unexpected message: {}",
        error.message()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn valid_public_uri_is_accepted_and_advertised() {
    let mut config = SorockNodeConfig::new("node-1", loopback_addr());
    config.public_uri = Some("http://raft-1.internal:7000".to_owned());

    let runtime = SorockRuntime::start(config, NoopMachine)
        .await
        .expect("a valid public uri must be accepted");

    assert_eq!(runtime.advertised_uri(), "http://raft-1.internal:7000");

    runtime.shutdown();
    runtime
        .join()
        .await
        .expect("graceful shutdown must succeed");
}
