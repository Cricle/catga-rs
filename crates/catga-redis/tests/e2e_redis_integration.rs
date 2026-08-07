//! E2E tests for catga-redis with real Redis backend.
//!
//! These tests verify Redis connection, pub/sub, scheduler, and flow store
//! functionality using a real Redis instance (either from podman-compose or
//! a configured CATGA_REDIS_URL).

use std::time::Duration;

use catga_core::flow::{
    DueFlowScheduler, FlowContinuation, FlowQuery, FlowScheduler, FlowState, FlowStore,
    SuspendedFlowStore, TimedOutFlowPoll, TimedOutFlowStore, WaitCondition, WaitPolicy,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode, MessageTransport, Stoppable};
use catga_redis::{
    RedisFlowScheduler, RedisFlows, RedisPubSubConfig, RedisPubSubTransport,
    RedisSuspendedFlows,
};
use redis::AsyncCommands;

#[path = "support/service_url.rs"]
mod service_url;

fn map_redis_error(error: redis::RedisError) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}

/// Tests basic Redis connection and flow creation.
#[tokio::test]
async fn e2e_redis_connection_and_flow_creation() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };

    let prefix = format!("catga-test-e2e-{}", uuid::Uuid::new_v4());
    let store = RedisFlows::connect(&url, prefix.clone()).await?;

    // Create a flow
    let state = FlowState::new("test-flow-1", "payment", Vec::new(), "node-a");
    assert!(store.create(state.clone()).await?);

    // Retrieve it
    let retrieved = store.get("test-flow-1").await?;
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.id(), "test-flow-1");
    assert_eq!(retrieved.version(), 0);

    // Verify keys exist in Redis
    let client = redis::Client::open(url.clone()).map_err(map_redis_error)?;
    let mut conn = client.get_multiplexed_async_connection().await.map_err(map_redis_error)?;
    let keys: Vec<String> = conn.keys(format!("{prefix}:*")).await.map_err(map_redis_error)?;
    assert!(!keys.is_empty(), "Redis must contain keys for the flow");

    Ok(())
}

/// Tests flow persistence across reconnection to Redis.
#[tokio::test]
async fn e2e_redis_flow_persists_across_reconnections() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };

    let prefix = format!("catga-test-persist-{}", uuid::Uuid::new_v4());
    let flow_id = "persistent-flow";

    // First connection - create flow
    {
        let store = RedisFlows::connect(&url, prefix.clone()).await?;
        let state = FlowState::new(flow_id, "payment", Vec::new(), "node-a");
        assert!(store.create(state).await?);
    }

    // Simulate reconnect - same flow should still exist
    {
        let store = RedisFlows::connect(&url, prefix.clone()).await?;
        let recovered = store.get(flow_id).await?;
        assert!(recovered.is_some(), "flow must persist across reconnection");
        assert_eq!(recovered.unwrap().version(), 0);
    }

    // Cleanup
    let client = redis::Client::open(url.clone()).map_err(map_redis_error)?;
    let mut conn = client.get_multiplexed_async_connection().await.map_err(map_redis_error)?;
    let _: () = conn.del(format!("{prefix}:*")).await.map_err(map_redis_error)?;

    Ok(())
}

/// Tests the Redis pub/sub transport for message broadcasting.
#[tokio::test]
async fn e2e_redis_pubsub_message_broadcast_and_receive() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };

    let channel = format!("test-channel-{}", uuid::Uuid::new_v4());
    let config = RedisPubSubConfig {
        server: url.clone().into(),
        channel: channel.clone().into(),
    };

    let transport = RedisPubSubTransport::connect(config).await?;

    // Publish a message
    let envelope = catga_core::Envelope::new(
        1,
        "test.message",
        b"hello from e2e test".to_vec(),
        catga_core::MessageMetadata::new(1, None),
    );
    transport.publish(envelope.clone()).await?;

    // Receive it back
    let delivery = tokio::time::timeout(Duration::from_secs(5), transport.receive())
        .await
        .map_err(|_| CatgaError::new(ErrorCode::Timeout, "timeout waiting for pub/sub message"))??;
    assert_eq!(delivery.envelope().message_type(), "test.message");

    transport.stop_accepting();
    Ok(())
}

/// Tests pub/sub with exactly-once delivery semantics.
#[tokio::test]
async fn e2e_redis_pubsub_exactly_once_deduplication() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };

    let channel = format!("test-exactly-once-{}", uuid::Uuid::new_v4());
    let config = RedisPubSubConfig {
        server: url.clone().into(),
        channel: channel.clone().into(),
    };

    let transport = RedisPubSubTransport::connect(config).await?;

    // Create message with exactly-once QoS
    let metadata = catga_core::MessageMetadata::new(42, None);
    let envelope = catga_core::Envelope::new(
        1,
        "exactly.once.message",
        b"test".to_vec(),
        metadata,
    );

    // Publish twice (simulating retry)
    transport.publish(envelope.clone()).await?;
    transport.publish(envelope.clone()).await?;

    // Should only receive once
    let delivery = tokio::time::timeout(Duration::from_secs(5), transport.receive())
        .await
        .map_err(|_| CatgaError::new(ErrorCode::Timeout, "timeout waiting for first pub/sub message"))??;
    let second_result = tokio::time::timeout(
        Duration::from_millis(500),
        transport.receive(),
    )
    .await;

    assert!(second_result.is_err(), "duplicate should be deduplicated");

    transport.stop_accepting();
    Ok(())
}

/// Tests Redis flow scheduler with due work.
#[tokio::test]
async fn e2e_redis_scheduler_due_work_and_acknowledgement() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };

    let prefix = format!("catga-test-scheduler-{}", uuid::Uuid::new_v4());
    let scheduler = RedisFlowScheduler::connect(&url, prefix.clone()).await?;

    // Schedule work for the future
    let due = std::time::SystemTime::now() + Duration::from_secs(5);
    let schedule_id = scheduler
        .schedule_resume("scheduled-flow", "process-step", due)
        .await?;
    assert!(!schedule_id.is_empty());

    // Nothing is due yet
    let now_claims = scheduler
        .claim_due(
            "worker-1",
            std::time::SystemTime::now(),
            Duration::from_secs(30),
            10,
        )
        .await?;
    assert!(now_claims.is_empty(), "future work should not be claimable");

    // After time passes, claim the work
    let later = std::time::SystemTime::now() + Duration::from_secs(10);
    let claims = scheduler
        .claim_due("worker-1", later, Duration::from_secs(30), 10)
        .await?;
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].flow_id(), "scheduled-flow");
    assert_eq!(claims[0].state_id(), "process-step");

    // Acknowledge completion
    let ack_result = scheduler.ack_due("worker-1", &claims[0].schedule_id()).await?;
    assert!(ack_result, "acknowledgement should succeed");

    // Verify work is no longer claimable
    let after_ack = scheduler
        .claim_due("worker-2", later, Duration::from_secs(30), 10)
        .await?;
    assert!(after_ack.is_empty(), "acknowledged work should be gone");

    Ok(())
}

/// Tests Redis suspended flows with wait conditions.
#[tokio::test]
async fn e2e_redis_suspended_flows_with_wait_correlation() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };

    let prefix = format!("catga-test-suspended-{}", uuid::Uuid::new_v4());
    let store = RedisSuspendedFlows::connect(&url, prefix.clone()).await?;

    let flow_id = "wait-flow";
    let correlation = "payment-callback-456";
    let now = std::time::SystemTime::now();

    // Create a waiting continuation
    let waiting = FlowContinuation::waiting(
        FlowState::new(flow_id, "payment", Vec::new(), "node-a").suspended(),
        "complete",
        WaitCondition::new(
            correlation,
            WaitPolicy::All,
            1,
            now,
            Duration::from_secs(60),
        ),
    );
    assert!(store.create(waiting.clone()).await?);

    // Query by wait correlation
    let by_correlation = store.get_by_wait_correlation(correlation).await?;
    assert!(by_correlation.is_some());
    assert_eq!(by_correlation.unwrap().state().id(), flow_id);

    // Query all suspended flows
    let all_suspended = store
        .query(&FlowQuery::new(10, 10).unwrap())
        .await?;
    assert_eq!(all_suspended.len(), 1);

    // Record wait success
    assert!(
        store
            .record_wait_success(flow_id, 0, "callback-1", b"confirmed".to_vec())
            .await?
    );

    // Verify wait result was recorded
    let updated = store.get(flow_id).await?.unwrap();
    let wait = updated.wait().expect("wait must exist");
    assert_eq!(wait.results().len(), 1);

    Ok(())
}

/// Tests timeout poll and receipt handling in Redis.
#[tokio::test]
async fn e2e_redis_timeout_poll_and_receipt_handling() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };

    let prefix = format!("catga-test-timeout-{}", uuid::Uuid::new_v4());
    let store = RedisSuspendedFlows::connect(&url, prefix.clone()).await?;

    let flow_id = "timeout-test-flow";
    let now = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(3000);

    // Create a waiting continuation with expired timeout
    let waiting = FlowContinuation::waiting(
        FlowState::new(flow_id, "payment", Vec::new(), "node-a").suspended(),
        "resume",
        WaitCondition::new(
            "timeout-correlation",
            WaitPolicy::All,
            1,
            now - Duration::from_secs(10),
            Duration::from_secs(5),
        ),
    );
    assert!(store.create(waiting).await?);

    // Poll for timed out flows
    let poll = TimedOutFlowPoll::new(now, 1, 10)?;
    let receipts = store.poll_timed_out(&poll).await?;
    assert_eq!(receipts.len(), 1);
    let receipt = receipts.into_iter().next().unwrap();
    assert_eq!(receipt.flow_id(), flow_id);

    // Poll again - should be empty (receipt is leased)
    let empty_poll = store.poll_timed_out(&poll).await?;
    assert!(empty_poll.is_empty());

    // Release the receipt
    store.release_timed_out(&receipt).await?;

    // Poll again - should get the same flow back
    let re_polled = store.poll_timed_out(&poll).await?;
    assert_eq!(re_polled.len(), 1);

    // Acknowledge to complete
    let re_receipt = re_polled.into_iter().next().unwrap();
    store.ack_timed_out(&re_receipt).await?;

    // Final poll should be empty
    let final_poll = store.poll_timed_out(&poll).await?;
    assert!(final_poll.is_empty());

    Ok(())
}

/// Tests concurrent flow updates in Redis (CAS semantics).
#[tokio::test]
async fn e2e_redis_concurrent_updates_are_atomically_serialized() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };

    let prefix = format!("catga-test-cas-{}", uuid::Uuid::new_v4());
    let flow_id = "cas-flow";

    // Create initial flow
    {
        let store = RedisFlows::connect(&url, prefix.clone()).await?;
        let state = FlowState::new(flow_id, "payment", Vec::new(), "node-a");
        store.create(state).await?;
    }

    // Concurrent updates from multiple "workers"
    let (result1, result2) = tokio::join!(
        async {
            let store = RedisFlows::connect(&url, prefix.clone()).await?;
            let current = store.get(flow_id).await?.unwrap();
            let version = current.version();
            let next = current.next_version()?;
            store.update(version, next).await
        },
        async {
            let store = RedisFlows::connect(&url, prefix.clone()).await?;
            let current = store.get(flow_id).await?.unwrap();
            let version = current.version();
            let next = current.next_version()?;
            store.update(version, next).await
        }
    );

    // Exactly one should succeed (atomic CAS)
    let success_count = usize::from(result1?) + usize::from(result2?);
    assert_eq!(success_count, 1, "exactly one concurrent update must succeed");

    // Verify final state
    let store = RedisFlows::connect(&url, prefix.clone()).await?;
    let final_state = store.get(flow_id).await?.unwrap();
    assert_eq!(final_state.version(), 1);

    Ok(())
}

/// Tests Redis flow claiming with stale heartbeat detection.
#[tokio::test]
async fn e2e_redis_flow_claiming_with_heartbeat_staleness() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };

    let prefix = format!("catga-test-claim-{}", uuid::Uuid::new_v4());
    let store = RedisFlows::connect(&url, prefix.clone()).await?;

    let flow_id = "stale-claim-flow";
    let stale_time = std::time::SystemTime::UNIX_EPOCH;

    // Create a stale flow (heartbeat in the past)
    let stale_state = FlowState::new(flow_id, "payment", Vec::new(), "node-a")
        .heartbeated_at(stale_time);
    assert!(store.create(stale_state).await?);

    // Claim should succeed
    let claimed = store
        .try_claim("payment", "new-worker", Duration::from_secs(86400))
        .await?;
    assert!(claimed.is_some());
    assert_eq!(claimed.unwrap().id(), flow_id);

    // Second claim should fail (already claimed by new-worker)
    let not_claimed = store
        .try_claim("payment", "another-worker", Duration::from_secs(86400))
        .await?;
    assert!(not_claimed.is_none());

    Ok(())
}

/// Tests Redis scheduler idempotency - same flow/schedule returns same ID.
#[tokio::test]
async fn e2e_redis_scheduler_idempotent_scheduling() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };

    let prefix = format!("catga-test-idempotent-{}", uuid::Uuid::new_v4());
    let scheduler = RedisFlowScheduler::connect(&url, prefix.clone()).await?;

    let due = std::time::SystemTime::now() + Duration::from_secs(100);

    // Schedule the same flow/state twice
    let id1 = scheduler
        .schedule_resume("idempotent-flow", "step", due)
        .await?;
    let id2 = scheduler
        .schedule_resume("idempotent-flow", "step", due)
        .await?;

    assert_eq!(id1, id2, "duplicate schedules must return same ID");

    // Only one claim should be available
    let later = std::time::SystemTime::now() + Duration::from_secs(200);
    let claims = scheduler
        .claim_due("worker", later, Duration::from_secs(30), 10)
        .await?;
    assert_eq!(claims.len(), 1);

    Ok(())
}
