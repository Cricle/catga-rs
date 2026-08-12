//! Connect-path edge contracts: unreachable servers fail transiently before any
//! provisioning, and broker-rejected names surface as transient provisioning errors.
//!
//! Every store maps connection and provisioning failures to `ErrorCode::Transient` so
//! operators can retry startup; these tests pin that mapping for each public connector.

#[path = "support/names.rs"]
mod names;
#[path = "support/nats_server.rs"]
mod nats_server;

use catga_core::{CatgaResult, ErrorCode};
use catga_nats::{
    NatsDeadLetters, NatsDslStepProgress, NatsEnhancedSnapshots, NatsFlowScheduler, NatsFlows,
    NatsIdempotency, NatsInbox, NatsOutbox, NatsProjectionCheckpoints, NatsSnapshotStore,
    NatsSubscriptions, NatsSuspendedFlows,
};
use names::unique;
use nats_server::{server_url, test_error};

/// Nothing listens on port 1, so the initial connection is refused immediately.
const UNREACHABLE: &str = "nats://127.0.0.1:1";

fn assert_transient<T>(result: CatgaResult<T>, case: &str) {
    assert!(
        matches!(result, Err(error) if error.code() == ErrorCode::Transient),
        "case {case} must fail transiently"
    );
}

#[tokio::test]
async fn unreachable_servers_fail_transiently_before_provisioning() {
    let bucket = unique("CATGA_CONN_DOWN");
    assert_transient(
        NatsFlowScheduler::connect(UNREACHABLE, bucket.as_str()).await,
        "scheduler",
    );
    assert_transient(
        NatsFlows::connect(UNREACHABLE, bucket.as_str()).await,
        "flows",
    );
    assert_transient(
        NatsSuspendedFlows::connect(UNREACHABLE, bucket.as_str()).await,
        "suspended flows",
    );
    assert_transient(
        NatsSubscriptions::connect(UNREACHABLE, bucket.as_str()).await,
        "subscriptions",
    );
    assert_transient(
        NatsDslStepProgress::connect(UNREACHABLE, bucket.as_str()).await,
        "DSL progress",
    );
    assert_transient(
        NatsProjectionCheckpoints::connect(UNREACHABLE, bucket.as_str()).await,
        "projection checkpoints",
    );
    assert_transient(
        NatsSnapshotStore::<u64>::connect(UNREACHABLE, bucket.as_str()).await,
        "snapshots",
    );
    assert_transient(
        NatsEnhancedSnapshots::<u64>::connect(UNREACHABLE, bucket.as_str()).await,
        "enhanced snapshots",
    );
    assert_transient(
        NatsIdempotency::connect(UNREACHABLE, bucket.as_str()).await,
        "idempotency",
    );
    assert_transient(
        NatsInbox::connect(UNREACHABLE, bucket.as_str()).await,
        "inbox",
    );
    assert_transient(
        NatsOutbox::connect(UNREACHABLE, bucket.as_str()).await,
        "outbox",
    );
    assert_transient(
        NatsDeadLetters::connect(UNREACHABLE, "CATGA_CONN_DOWN_STREAM", "catga.conn.down").await,
        "dead letters",
    );
}

#[tokio::test]
#[ignore = "requires a real JetStream server; run in the E2E job"]
async fn broker_rejected_names_fail_transiently_after_bounded_retries() -> CatgaResult<()> {
    // Spaces and punctuation are illegal in stream and bucket names, so provisioning can
    // never succeed; the store must give up after its bounded polling instead of hanging.
    let server = server_url();
    // Preflight: the broker must accept a raw connection before the rejection checks.
    async_nats::connect(&server)
        .await
        .map_err(|error| test_error("connect to the test broker", error))?;
    assert_transient(
        NatsFlowScheduler::connect(&server, "bad bucket!").await,
        "scheduler",
    );
    assert_transient(NatsFlows::connect(&server, "bad bucket!").await, "flows");
    assert_transient(
        NatsSuspendedFlows::connect(&server, "bad bucket!").await,
        "suspended flows",
    );
    assert_transient(
        NatsSubscriptions::connect(&server, "bad bucket!").await,
        "subscriptions",
    );
    assert_transient(
        NatsDslStepProgress::connect(&server, "bad bucket!").await,
        "DSL progress",
    );
    assert_transient(
        NatsProjectionCheckpoints::connect(&server, "bad bucket!").await,
        "projection checkpoints",
    );
    assert_transient(
        NatsSnapshotStore::<u64>::connect(&server, "bad bucket!").await,
        "snapshots",
    );
    assert_transient(
        NatsEnhancedSnapshots::<u64>::connect(&server, "bad bucket!").await,
        "enhanced snapshots",
    );
    assert_transient(
        NatsIdempotency::connect(&server, "bad bucket!").await,
        "idempotency",
    );
    assert_transient(NatsInbox::connect(&server, "bad bucket!").await, "inbox");
    assert_transient(NatsOutbox::connect(&server, "bad bucket!").await, "outbox");
    assert_transient(
        NatsDeadLetters::connect(&server, "bad stream!", "catga.conn.bad").await,
        "dead letters",
    );
    Ok(())
}
