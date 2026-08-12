//! Strict scenario contracts for the in-memory [`MemoryFlowScheduler`].
//!
//! Covers idempotent scheduling, due-time ordering, cancellation fencing, and
//! the claim / acknowledge / release / renew lease lifecycle. All schedules
//! use explicit [`SystemTime`] values so the tests never sleep.

use std::time::{Duration, SystemTime};

use catga_core::flow::{DueFlowScheduler, FlowScheduler, MemoryFlowScheduler};
use catga_core::{CatgaResult, ErrorCode};

const T0: SystemTime = SystemTime::UNIX_EPOCH;

fn at(secs: u64) -> SystemTime {
    T0 + Duration::from_secs(secs)
}

#[tokio::test]
async fn schedule_resume_is_idempotent_per_target_and_retains_its_due_time() -> CatgaResult<()> {
    let scheduler = MemoryFlowScheduler::default();

    let first = scheduler
        .schedule_resume("flow", "step-a", at(1_000))
        .await?;
    let second = scheduler
        .schedule_resume("flow", "step-a", at(9_999))
        .await?;
    assert_eq!(
        first, second,
        "the same target returns the registered identity"
    );

    assert!(
        scheduler.take_due(at(999)).is_empty(),
        "the original due time is retained"
    );
    let due = scheduler.take_due(at(1_000));
    assert_eq!(due.len(), 1);
    let resume = &due[0];
    assert_eq!(resume.schedule_id(), first.as_ref());
    assert_eq!(resume.flow_id(), "flow");
    assert_eq!(resume.state_id(), "step-a");
    assert_eq!(resume.due_at(), at(1_000));
    Ok(())
}

#[tokio::test]
async fn take_due_returns_only_due_schedules_and_removes_them() -> CatgaResult<()> {
    let scheduler = MemoryFlowScheduler::default();
    scheduler.schedule_resume("flow-a", "step", at(100)).await?;
    scheduler.schedule_resume("flow-b", "step", at(200)).await?;

    let due = scheduler.take_due(at(150));
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].flow_id(), "flow-a");

    let due = scheduler.take_due(at(200));
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].flow_id(), "flow-b");

    assert!(scheduler.take_due(at(200)).is_empty(), "taken work is gone");
    Ok(())
}

#[tokio::test]
async fn cancel_resume_removes_schedules_and_reports_missing_identities() -> CatgaResult<()> {
    let scheduler = MemoryFlowScheduler::default();
    let schedule_id = scheduler.schedule_resume("flow", "step", at(100)).await?;

    assert!(scheduler.cancel_resume(&schedule_id).await?);
    assert!(
        scheduler.take_due(at(100)).is_empty(),
        "a cancelled schedule never becomes due"
    );
    assert!(
        !scheduler.cancel_resume(&schedule_id).await?,
        "cancelling twice reports false"
    );
    assert!(!scheduler.cancel_resume("never-scheduled").await?);

    let schedule_id = scheduler.schedule_resume("flow", "step", at(100)).await?;
    let taken = scheduler.take_due(at(100));
    assert_eq!(taken.len(), 1);
    assert!(
        !scheduler.cancel_resume(&schedule_id).await?,
        "a taken schedule can no longer be cancelled"
    );
    Ok(())
}

#[tokio::test]
async fn claim_due_validates_leases_and_fences_ownership() -> CatgaResult<()> {
    let scheduler = MemoryFlowScheduler::default();
    scheduler.schedule_resume("flow", "step", at(1_000)).await?;

    let error = scheduler
        .claim_due("worker-a", at(1_000), Duration::ZERO, 1)
        .await
        .expect_err("a zero lease cannot establish ownership");
    assert_eq!(error.code(), ErrorCode::Validation);

    let error = scheduler
        .claim_due("worker-a", at(1_000), Duration::MAX, 1)
        .await
        .expect_err("an unrepresentable lease deadline is rejected");
    assert_eq!(error.code(), ErrorCode::Validation);

    assert!(
        scheduler
            .claim_due("worker-a", at(999), Duration::from_secs(60), 1)
            .await?
            .is_empty(),
        "future schedules are not claimable"
    );

    let claimed = scheduler
        .claim_due("worker-a", at(1_000), Duration::from_secs(60), 1)
        .await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].flow_id(), "flow");

    assert!(
        scheduler
            .claim_due("worker-b", at(1_030), Duration::from_secs(60), 1)
            .await?
            .is_empty(),
        "a live foreign claim blocks other owners"
    );

    assert!(
        !scheduler
            .ack_due("worker-b", claimed[0].schedule_id())
            .await?,
        "only the claiming owner may acknowledge"
    );
    assert!(
        scheduler
            .ack_due("worker-a", claimed[0].schedule_id())
            .await?
    );
    assert!(
        scheduler
            .claim_due("worker-b", at(2_000), Duration::from_secs(60), 1)
            .await?
            .is_empty(),
        "acknowledged work is removed"
    );
    assert!(
        !scheduler
            .ack_due("worker-a", claimed[0].schedule_id())
            .await?,
        "acknowledging twice reports false"
    );
    Ok(())
}

#[tokio::test]
async fn claim_due_respects_the_batch_limit() -> CatgaResult<()> {
    let scheduler = MemoryFlowScheduler::default();
    for index in 0..3_u8 {
        scheduler
            .schedule_resume(format!("flow-{index}").as_str(), "step", at(100))
            .await?;
    }

    let claimed = scheduler
        .claim_due("worker-a", at(100), Duration::from_secs(60), 2)
        .await?;
    assert_eq!(claimed.len(), 2, "one call claims at most the limit");

    let mut seen: Vec<&str> = claimed.iter().map(|resume| resume.flow_id()).collect();
    seen.sort_unstable();
    assert_eq!(seen, ["flow-0", "flow-1"]);

    let remaining = scheduler
        .claim_due("worker-b", at(100), Duration::from_secs(60), 2)
        .await?;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].flow_id(), "flow-2");
    Ok(())
}

#[tokio::test]
async fn release_due_returns_a_claim_to_the_due_set() -> CatgaResult<()> {
    let scheduler = MemoryFlowScheduler::default();
    scheduler.schedule_resume("flow", "step", at(100)).await?;

    let claimed = scheduler
        .claim_due("worker-a", at(100), Duration::from_secs(3_600), 1)
        .await?;
    assert_eq!(claimed.len(), 1);
    let schedule_id = claimed[0].schedule_id();

    assert!(
        !scheduler.release_due("worker-b", schedule_id).await?,
        "only the claiming owner may release"
    );
    assert!(scheduler.release_due("worker-a", schedule_id).await?);
    assert!(
        !scheduler.release_due("worker-a", schedule_id).await?,
        "releasing twice reports false"
    );

    let reclaimed = scheduler
        .claim_due("worker-b", at(100), Duration::from_secs(60), 1)
        .await?;
    assert_eq!(
        reclaimed.len(),
        1,
        "a released claim is immediately claimable"
    );
    assert_eq!(reclaimed[0].schedule_id(), schedule_id);
    assert_eq!(
        reclaimed[0].due_at(),
        at(100),
        "release retains the due time"
    );
    Ok(())
}

#[tokio::test]
async fn renew_due_extends_only_the_owners_live_lease() -> CatgaResult<()> {
    let scheduler = MemoryFlowScheduler::default();
    scheduler.schedule_resume("flow", "step", at(100)).await?;
    let claimed = scheduler
        .claim_due("worker-a", at(100), Duration::from_secs(100), 1)
        .await?;
    let schedule_id = claimed[0].schedule_id().to_owned();

    let error = scheduler
        .renew_due("worker-a", &schedule_id, at(100), Duration::ZERO)
        .await
        .expect_err("a zero renewal is invalid");
    assert_eq!(error.code(), ErrorCode::Validation);
    assert!(
        !scheduler
            .renew_due(
                "worker-b",
                &schedule_id,
                at(100),
                Duration::from_secs(1_000)
            )
            .await?,
        "only the claiming owner may renew"
    );

    assert!(
        scheduler
            .renew_due(
                "worker-a",
                &schedule_id,
                at(100),
                Duration::from_secs(1_000)
            )
            .await?
    );
    assert!(
        scheduler
            .claim_due("worker-b", at(500), Duration::from_secs(60), 1)
            .await?
            .is_empty(),
        "the renewed lease still fences other owners"
    );
    let reclaimed = scheduler
        .claim_due("worker-b", at(1_101), Duration::from_secs(60), 1)
        .await?;
    assert_eq!(reclaimed.len(), 1, "an expired lease can be reclaimed");
    Ok(())
}

#[tokio::test]
async fn take_due_never_consumes_a_live_claim() -> CatgaResult<()> {
    let scheduler = MemoryFlowScheduler::default();
    scheduler.schedule_resume("flow", "step", at(100)).await?;
    let claimed = scheduler
        .claim_due("worker-a", at(100), Duration::from_secs(100), 1)
        .await?;
    assert_eq!(claimed.len(), 1);

    assert!(
        scheduler.take_due(at(150)).is_empty(),
        "a live claim is not consumed by the compatibility path"
    );
    let due = scheduler.take_due(at(201));
    assert_eq!(due.len(), 1, "the schedule returns once the lease expires");
    assert_eq!(due[0].flow_id(), "flow");
    Ok(())
}

#[tokio::test]
async fn cancel_resume_refuses_a_live_claim() -> CatgaResult<()> {
    let scheduler = MemoryFlowScheduler::default();
    let now = SystemTime::now();
    scheduler.schedule_resume("flow", "step", now).await?;
    let claimed = scheduler
        .claim_due("worker-a", now, Duration::from_secs(3_600), 1)
        .await?;
    assert_eq!(claimed.len(), 1);

    assert!(
        !scheduler.cancel_resume(claimed[0].schedule_id()).await?,
        "a live claim cannot be cancelled out from under its owner"
    );
    assert!(
        scheduler
            .ack_due("worker-a", claimed[0].schedule_id())
            .await?
    );
    Ok(())
}
