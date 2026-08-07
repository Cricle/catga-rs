//! Inbox claim fencing conformance tests for the in-memory backend.

use std::{sync::Arc, time::Duration};

use catga_core::memory::MemoryInbox;
use catga_core::{ErrorCode, InboxStore, ProcessingState};

#[tokio::test]
async fn failed_claim_is_reclaimed_with_a_new_generation_and_fences_the_former_owner() {
    let inbox = MemoryInbox::default();
    let first = inbox
        .try_claim(701)
        .await
        .expect("initial claim operation succeeds")
        .expect("initial owner acquires the message");

    assert_ne!(
        first.generation(),
        0,
        "store-issued generations are never zero"
    );
    inbox
        .fail(first)
        .await
        .expect("current owner can release a failed claim");

    let second = inbox
        .try_claim(701)
        .await
        .expect("reclaim operation succeeds")
        .expect("failed message is reclaimable");
    assert!(
        second.generation() > first.generation(),
        "each reclaim advances the fencing generation"
    );

    assert!(matches!(
        inbox.complete(first, Some(Arc::from([1_u8]))).await,
        Err(error) if error.code() == ErrorCode::Conflict
    ));
    assert!(matches!(
        inbox.fail(first).await,
        Err(error) if error.code() == ErrorCode::Conflict
    ));
    assert_eq!(
        inbox.state(701).await.expect("state lookup succeeds"),
        Some(ProcessingState::Claimed),
        "a stale owner cannot release the reclaimed claim"
    );

    inbox
        .fail(second)
        .await
        .expect("current reclaimed owner can release a failed claim");
    let third = inbox
        .try_claim(701)
        .await
        .expect("second reclaim operation succeeds")
        .expect("failed reclaimed message is reclaimable");
    assert!(
        third.generation() > second.generation(),
        "every successive reclaim advances the fencing generation"
    );
    assert!(matches!(
        inbox.complete(second, Some(Arc::from([3_u8]))).await,
        Err(error) if error.code() == ErrorCode::Conflict
    ));
    assert!(matches!(
        inbox.fail(second).await,
        Err(error) if error.code() == ErrorCode::Conflict
    ));
    assert_eq!(
        inbox.state(701).await.expect("state lookup succeeds"),
        Some(ProcessingState::Claimed),
        "the second stale owner cannot release the third claim"
    );

    let expected = Arc::<[u8]>::from([2_u8, 7]);
    inbox
        .complete(third, Some(Arc::clone(&expected)))
        .await
        .expect("current owner can complete the message");
    assert_eq!(
        inbox.result(701).await.expect("result lookup succeeds"),
        Some(expected),
        "the stale completion cannot overwrite the current owner's result"
    );
}

#[tokio::test]
async fn zero_lease_is_rejected_without_retaining_or_consuming_an_inbox_record() {
    let inbox = MemoryInbox::new(1).expect("positive capacity is valid");

    let error = inbox
        .try_claim_for(702, Duration::ZERO)
        .await
        .expect_err("zero-length leases are invalid");
    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(
        inbox.state(702).await.expect("state lookup succeeds"),
        None,
        "invalid input must not retain a partial inbox record"
    );

    assert!(
        inbox
            .try_claim(703)
            .await
            .expect("valid claim succeeds after rejected input")
            .is_some(),
        "rejected input must not consume bounded record capacity"
    );
}
