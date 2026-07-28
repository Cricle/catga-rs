#![allow(missing_docs)]

//! Service-gated contracts for Redis-backed durable flow scheduling.

use std::time::{Duration, UNIX_EPOCH};

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{DueFlowScheduler, FlowScheduler};
use catga_redis::RedisFlowScheduler;
use redis::AsyncCommands;

#[path = "support/service_url.rs"]
mod service_url;

fn map_redis_error(error: redis::RedisError) -> CatgaError {
    CatgaError::new(ErrorCode::Transient, error.to_string())
}

fn target_key(flow_id: &str, state_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(16 + flow_id.len() + state_id.len());
    key.extend_from_slice(&(flow_id.len() as u64).to_be_bytes());
    key.extend_from_slice(flow_id.as_bytes());
    key.extend_from_slice(&(state_id.len() as u64).to_be_bytes());
    key.extend_from_slice(state_id.as_bytes());
    key
}

#[tokio::test]
async fn scheduler_returns_a_stable_id_only_for_its_own_target() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = format!("catga-test-scheduler-{}", uuid::Uuid::new_v4());
    let scheduler = RedisFlowScheduler::connect(&url, prefix.clone()).await?;
    let due_at = UNIX_EPOCH + Duration::from_secs(100);

    let first = scheduler
        .schedule_resume("flow-a", "state-a", due_at)
        .await?;
    let repeated = scheduler
        .schedule_resume("flow-a", "state-a", due_at)
        .await?;
    assert_eq!(first, repeated);

    let client = redis::Client::open(url).map_err(map_redis_error)?;
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .map_err(map_redis_error)?;
    let _: () = connection
        .hset(
            format!("{prefix}:targets"),
            target_key("flow-b", "state-b"),
            first.as_ref(),
        )
        .await
        .map_err(map_redis_error)?;

    let second = scheduler
        .schedule_resume("flow-b", "state-b", due_at)
        .await?;
    assert_ne!(first, second);
    assert_eq!(
        second,
        scheduler
            .schedule_resume("flow-b", "state-b", due_at)
            .await?
    );
    Ok(())
}

#[tokio::test]
async fn scheduler_does_not_renew_an_expired_lease() -> CatgaResult<()> {
    let Some(url) = service_url::redis_url()? else {
        return Ok(());
    };
    let prefix = format!("catga-test-scheduler-{}", uuid::Uuid::new_v4());
    let scheduler = RedisFlowScheduler::connect(&url, prefix).await?;
    let due_at = UNIX_EPOCH + Duration::from_secs(100);
    let schedule_id = scheduler.schedule_resume("flow", "state", due_at).await?;

    let claimed = scheduler
        .claim_due("worker", due_at, Duration::from_secs(1), 1)
        .await?;
    assert_eq!(claimed.len(), 1);
    assert!(
        !scheduler
            .renew_due(
                "worker",
                &schedule_id,
                due_at + Duration::from_secs(1),
                Duration::from_secs(1),
            )
            .await?
    );
    Ok(())
}
