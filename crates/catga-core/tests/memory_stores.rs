//! Strict contract tests for the process-local memory store implementations:
//! event store paging and optimistic concurrency, inbox claim fencing and
//! retention cleanup, owner-conditional leases, and read-model tracking.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use catga_core::memory::{
    MemoryChangeTracker, MemoryEventStore, MemoryInbox, MemoryLeases, MemoryReadModels,
};
use catga_core::{
    ChangeKind, ChangeRecord, ChangeTracker, Envelope, ErrorCode, EventStore, InboxStore,
    LeaseStore, MAX_EVENT_STORE_PAGE_SIZE, MAX_READ_MODEL_PAGE_SIZE, MessageMetadata,
    ProcessingState, ReadModelStore,
};

fn envelope(id: u64, message_type: &'static str, payload: u8) -> Envelope {
    Envelope::new(
        id,
        message_type,
        vec![payload],
        MessageMetadata::new(id, None),
    )
}

// ---------------------------------------------------------------------------
// MemoryEventStore
// ---------------------------------------------------------------------------

#[tokio::test]
async fn event_store_append_enforces_optimistic_concurrency() {
    let store = MemoryEventStore::default();

    // An empty batch never creates a stream and reports the current version.
    assert_eq!(
        store
            .append("missing", Vec::new(), None)
            .await
            .expect("empty append succeeds"),
        -1
    );

    let first = store
        .append(
            "s",
            vec![envelope(1, "Tick", 1), envelope(2, "Tick", 2)],
            None,
        )
        .await
        .expect("append succeeds");
    assert_eq!(first, 1, "the version of the last appended event");

    // An empty batch on an existing stream reports its current version.
    assert_eq!(
        store
            .append("s", Vec::new(), None)
            .await
            .expect("empty append succeeds"),
        1
    );

    // A matching expected version appends; a stale one conflicts.
    let version = store
        .append("s", vec![envelope(3, "Tick", 3)], Some(1))
        .await
        .expect("matching version appends");
    assert_eq!(version, 2);
    let error = store
        .append("s", vec![envelope(4, "Tick", 4)], Some(1))
        .await
        .expect_err("a stale expected version conflicts");
    assert_eq!(error.code(), ErrorCode::Conflict);

    assert_eq!(store.version("s").await.expect("version succeeds"), 2);
    assert_eq!(
        store.version("missing").await.expect("version succeeds"),
        -1
    );
}

#[tokio::test]
async fn event_store_read_page_paginates_with_a_version_cursor() {
    let store = MemoryEventStore::default();
    let events: Vec<_> = (0..5).map(|i| envelope(i, "Tick", i as u8)).collect();
    store
        .append("s", events, None)
        .await
        .expect("append succeeds");

    let first = store.read_page("s", 0, 2).await.expect("page succeeds");
    assert_eq!(first.stream().version(), 4);
    assert_eq!(first.stream().events().len(), 2);
    assert_eq!(first.stream().events()[0].version(), 0);
    assert_eq!(first.stream().events()[0].envelope().payload(), &[0]);
    assert_eq!(first.next_version(), Some(2));

    let second = store.read_page("s", 2, 2).await.expect("page succeeds");
    assert_eq!(second.stream().events().len(), 2);
    assert_eq!(second.next_version(), Some(4));

    let last = store.read_page("s", 4, 2).await.expect("page succeeds");
    assert_eq!(last.stream().events().len(), 1);
    assert_eq!(last.next_version(), None, "the tip ends the cursor");

    // Reading past the tip yields an empty page with the stream version.
    let beyond = store.read_page("s", 9, 2).await.expect("page succeeds");
    assert!(beyond.stream().events().is_empty());
    assert_eq!(beyond.stream().version(), 4);
    assert_eq!(beyond.next_version(), None);

    // A missing stream reads as empty with version -1.
    let missing = store
        .read_page("missing", 0, 2)
        .await
        .expect("page succeeds");
    assert_eq!(missing.stream().version(), -1);
    assert!(missing.stream().events().is_empty());

    // Page sizes are bounded by the shared contract.
    for max_count in [0, MAX_EVENT_STORE_PAGE_SIZE + 1] {
        let error = store
            .read_page("s", 0, max_count)
            .await
            .expect_err("invalid page size must fail");
        assert_eq!(error.code(), ErrorCode::Validation);
    }
}

#[tokio::test]
async fn event_store_read_to_version_page_bounds_inclusively() {
    let store = MemoryEventStore::default();
    let events: Vec<_> = (0..5).map(|i| envelope(i, "Tick", i as u8)).collect();
    store
        .append("s", events, None)
        .await
        .expect("append succeeds");

    // A negative upper bound reads nothing and reports no stream progress.
    let empty = store
        .read_to_version_page("s", 0, -1, 10)
        .await
        .expect("page succeeds");
    assert!(empty.stream().events().is_empty());
    assert_eq!(empty.stream().version(), -1);
    assert_eq!(empty.next_version(), None);

    let page = store
        .read_to_version_page("s", 0, 2, 10)
        .await
        .expect("page succeeds");
    assert_eq!(page.stream().events().len(), 3);
    assert_eq!(page.stream().version(), 2);
    assert_eq!(page.next_version(), None, "the bound precedes the tip");

    // A page that stops before the bound resumes after the last read event;
    // a page that reaches the bound ends the cursor.
    let page = store
        .read_to_version_page("s", 0, 1, 1)
        .await
        .expect("page succeeds");
    assert_eq!(page.stream().events().len(), 1);
    assert_eq!(page.next_version(), Some(1));
    let page = store
        .read_to_version_page("s", 1, 1, 2)
        .await
        .expect("page succeeds");
    assert_eq!(page.stream().events().len(), 1);
    assert_eq!(
        page.next_version(),
        None,
        "the inclusive bound ends the cursor"
    );

    let error = store
        .read_to_version_page("s", 0, 4, 0)
        .await
        .expect_err("invalid page size must fail");
    assert_eq!(error.code(), ErrorCode::Validation);
}

#[tokio::test]
async fn event_store_read_to_time_page_filters_by_timestamp() {
    let store = MemoryEventStore::default();
    let before = SystemTime::now();
    store
        .append("s", vec![envelope(1, "Tick", 1)], None)
        .await
        .expect("append succeeds");

    // A bound before the append matches nothing but still reports progress.
    let page = store
        .read_to_time_page("s", 0, before, 10)
        .await
        .expect("page succeeds");
    assert!(page.stream().events().is_empty());
    assert_eq!(page.stream().version(), -1);
    assert_eq!(page.next_version(), None, "the whole stream was scanned");

    let after = SystemTime::now();
    let page = store
        .read_to_time_page("s", 0, after, 10)
        .await
        .expect("page succeeds");
    assert_eq!(page.stream().events().len(), 1);
    assert_eq!(page.stream().version(), 0);

    let error = store
        .read_to_time_page("s", 0, after, MAX_EVENT_STORE_PAGE_SIZE + 1)
        .await
        .expect_err("invalid page size must fail");
    assert_eq!(error.code(), ErrorCode::Validation);
}

#[tokio::test]
async fn event_store_version_history_and_stream_ids_page_lexically() {
    let store = MemoryEventStore::default();
    for stream in ["alpha", "beta", "gamma"] {
        store
            .append(
                stream,
                vec![envelope(1, "Tick", 1), envelope(2, "Tock", 2)],
                None,
            )
            .await
            .expect("append succeeds");
    }

    let history = store
        .version_history_page("alpha", 0, 1)
        .await
        .expect("history succeeds");
    assert_eq!(history.entries().len(), 1);
    assert_eq!(history.entries()[0].version(), 0);
    assert_eq!(history.entries()[0].event_type(), "Tick");
    assert_eq!(history.next_version(), Some(1));
    let history = store
        .version_history_page("alpha", 1, 5)
        .await
        .expect("history succeeds");
    assert_eq!(history.entries()[0].event_type(), "Tock");
    assert_eq!(history.next_version(), None);

    let error = store
        .version_history_page("alpha", 0, 0)
        .await
        .expect_err("invalid page size must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    // Stream identifiers page in lexical order with an exclusive cursor.
    let page = store
        .stream_ids_page(None, 2)
        .await
        .expect("ids page succeeds");
    assert_eq!(page.ids(), &["alpha".to_string(), "beta".to_string()]);
    assert_eq!(page.next_stream_id(), Some("beta"));
    let page = store
        .stream_ids_page(page.next_stream_id(), 2)
        .await
        .expect("ids page succeeds");
    assert_eq!(page.ids(), &["gamma".to_string()]);
    assert_eq!(page.next_stream_id(), None);

    let error = store
        .stream_ids_page(None, 0)
        .await
        .expect_err("invalid page size must fail");
    assert_eq!(error.code(), ErrorCode::Validation);
}

// ---------------------------------------------------------------------------
// MemoryInbox
// ---------------------------------------------------------------------------

#[tokio::test]
async fn inbox_claim_lifecycle_fences_by_generation() {
    let inbox = MemoryInbox::default();

    let claim = inbox
        .try_claim(7)
        .await
        .expect("claim succeeds")
        .expect("first claim wins");
    assert_eq!(claim.message_id(), 7);
    assert_eq!(
        inbox.state(7).await.expect("state succeeds"),
        Some(ProcessingState::Claimed)
    );

    // A claimed message cannot be claimed again.
    assert_eq!(inbox.try_claim(7).await.expect("claim succeeds"), None);

    // Failing releases the message for a fresh claim with a new generation.
    inbox.fail(claim).await.expect("fail succeeds");
    assert_eq!(
        inbox.state(7).await.expect("state succeeds"),
        Some(ProcessingState::Failed)
    );
    let reclaimed = inbox
        .try_claim(7)
        .await
        .expect("claim succeeds")
        .expect("a failed message reclaims");
    assert_ne!(reclaimed.generation(), claim.generation());

    // The stale claim can no longer complete the message.
    let error = inbox
        .complete(claim, None)
        .await
        .expect_err("a fenced claim conflicts");
    assert_eq!(error.code(), ErrorCode::Conflict);
    let error = inbox
        .fail(claim)
        .await
        .expect_err("a fenced claim conflicts");
    assert_eq!(error.code(), ErrorCode::Conflict);

    // The current owner completes with a cached result.
    inbox
        .complete(reclaimed, Some(Arc::from([9_u8, 8])))
        .await
        .expect("complete succeeds");
    assert_eq!(
        inbox.state(7).await.expect("state succeeds"),
        Some(ProcessingState::Completed)
    );
    let result = inbox.result(7).await.expect("result succeeds");
    assert_eq!(result.as_deref(), Some(&[9_u8, 8][..]));

    // A completed message is never handed out again.
    assert_eq!(inbox.try_claim(7).await.expect("claim succeeds"), None);

    // Unknown messages report not-found for owner operations.
    let unknown = catga_core::InboxClaim::new(42, 1).expect("non-zero generation");
    let error = inbox
        .complete(unknown, None)
        .await
        .expect_err("unknown message is not found");
    assert_eq!(error.code(), ErrorCode::NotFound);
    let error = inbox
        .fail(unknown)
        .await
        .expect_err("unknown message is not found");
    assert_eq!(error.code(), ErrorCode::NotFound);
    assert_eq!(inbox.state(42).await.expect("state succeeds"), None);
    assert_eq!(inbox.result(42).await.expect("result succeeds"), None);
}

#[tokio::test]
async fn inbox_validates_leases_and_reports_capacity_exhaustion() {
    // A zero lease is rejected before any record transition.
    let inbox = MemoryInbox::default();
    let error = inbox
        .try_claim_for(1, Duration::ZERO)
        .await
        .expect_err("zero lease must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    // Zero retention is rejected at construction.
    let error = MemoryInbox::with_retention_and_capacity(Duration::ZERO, 4)
        .map(|_| ())
        .expect_err("zero retention must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    // A full inbox with nothing cleanable is routine backpressure.
    let inbox = MemoryInbox::new(1).expect("capacity builds");
    assert!(inbox.try_claim(1).await.expect("claim succeeds").is_some());
    let error = inbox
        .try_claim(2)
        .await
        .expect_err("a full inbox without cleanup headroom fails");
    assert_eq!(error.code(), ErrorCode::Unavailable);
}

#[tokio::test]
async fn inbox_cleanup_removes_only_retention_expired_completions() {
    let inbox = MemoryInbox::with_retention_and_capacity(Duration::from_millis(1), 4)
        .expect("inbox builds");

    let fresh = inbox
        .try_claim(1)
        .await
        .expect("claim succeeds")
        .expect("claim wins");
    inbox
        .complete(fresh, None)
        .await
        .expect("complete succeeds");

    // Records younger than the retention survive cleanup.
    let removed = inbox
        .cleanup_completed(Duration::from_secs(60), 10)
        .await
        .expect("cleanup succeeds");
    assert_eq!(removed, 0);
    assert_eq!(
        inbox.state(1).await.expect("state succeeds"),
        Some(ProcessingState::Completed)
    );

    // Once expired, the completed record is removed and capacity released.
    tokio::time::sleep(Duration::from_millis(5)).await;
    let removed = inbox
        .cleanup_completed(Duration::from_millis(1), 10)
        .await
        .expect("cleanup succeeds");
    assert_eq!(removed, 1);
    assert_eq!(inbox.state(1).await.expect("state succeeds"), None);

    // The released slot admits new claims again.
    assert!(inbox.try_claim(2).await.expect("claim succeeds").is_some());

    let error = inbox
        .cleanup_completed(Duration::from_millis(1), 1_025)
        .await
        .expect_err("an over-budget limit must fail");
    assert_eq!(error.code(), ErrorCode::Validation);
}

// ---------------------------------------------------------------------------
// MemoryLeases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn leases_are_owner_conditional_and_expire() {
    let leases = MemoryLeases::default();

    assert!(
        leases
            .try_acquire("res", "owner-a", Duration::from_secs(30))
            .await
            .expect("acquire succeeds")
    );
    // The owner re-acquiring renews the lease.
    assert!(
        leases
            .try_acquire("res", "owner-a", Duration::from_secs(30))
            .await
            .expect("acquire succeeds")
    );
    // A contender is refused while the lease is held.
    assert!(
        !leases
            .try_acquire("res", "owner-b", Duration::from_secs(30))
            .await
            .expect("acquire succeeds")
    );

    // Renew requires the current owner and a live lease.
    assert!(
        leases
            .renew("res", "owner-a", Duration::from_secs(30))
            .await
            .expect("renew succeeds")
    );
    assert!(
        !leases
            .renew("res", "owner-b", Duration::from_secs(30))
            .await
            .expect("renew succeeds")
    );
    assert!(
        !leases
            .renew("missing", "owner-a", Duration::from_secs(30))
            .await
            .expect("renew succeeds")
    );

    // Release is owner-conditional.
    assert!(
        !leases
            .release("res", "owner-b")
            .await
            .expect("release succeeds")
    );
    assert!(
        leases
            .release("res", "owner-a")
            .await
            .expect("release succeeds")
    );
    assert!(
        !leases
            .release("res", "owner-a")
            .await
            .expect("release succeeds")
    );

    // An expired lease passes to the next contender.
    assert!(
        leases
            .try_acquire("ttl", "owner-a", Duration::from_millis(1))
            .await
            .expect("acquire succeeds")
    );
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(
        !leases
            .renew("ttl", "owner-a", Duration::from_secs(30))
            .await
            .expect("an expired lease cannot renew")
    );
    assert!(
        leases
            .try_acquire("ttl", "owner-b", Duration::from_secs(30))
            .await
            .expect("an expired lease transfers")
    );
}

// ---------------------------------------------------------------------------
// Memory read models
// ---------------------------------------------------------------------------

fn change(id: &str, kind: ChangeKind) -> ChangeRecord {
    ChangeRecord::new(
        id,
        "account",
        "acct-1",
        kind,
        envelope(1, "AccountChanged", 1),
    )
}

#[tokio::test]
async fn change_tracker_releases_acknowledged_changes() {
    let tracker = MemoryChangeTracker::default();
    tracker.track(change("c-1", ChangeKind::Created));
    tracker.track(change("c-2", ChangeKind::Updated));

    let pending = tracker.pending_page(10).await.expect("page succeeds");
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].entity_type(), "account");
    assert_eq!(pending[0].entity_id(), "acct-1");

    // Tracking replaces a pending change with the same identifier.
    tracker.track(change("c-1", ChangeKind::Deleted));
    let pending = tracker.pending_page(10).await.expect("page succeeds");
    assert_eq!(pending.len(), 2);
    let c1 = pending
        .iter()
        .find(|record| record.id() == "c-1")
        .expect("c-1 retained");
    assert_eq!(c1.kind(), ChangeKind::Deleted);

    // Page size is bounded and enforced.
    let page = tracker.pending_page(1).await.expect("page succeeds");
    assert_eq!(page.len(), 1);
    let error = tracker
        .pending_page(0)
        .await
        .expect_err("zero page size must fail");
    assert_eq!(error.code(), ErrorCode::Validation);
    let error = tracker
        .pending_page(MAX_READ_MODEL_PAGE_SIZE + 1)
        .await
        .expect_err("over-budget page size must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    // Acknowledged changes leave the pending set.
    tracker
        .mark_synced(&["c-1".into(), "c-2".into()])
        .await
        .expect("sync succeeds");
    assert!(
        tracker
            .pending_page(10)
            .await
            .expect("page succeeds")
            .is_empty()
    );
    // Re-acknowledging is a no-op.
    tracker
        .mark_synced(&["c-1".into()])
        .await
        .expect("sync succeeds");
}

#[tokio::test]
async fn read_models_round_trip_shared_values() {
    let models = MemoryReadModels::<String>::default();
    assert_eq!(models.get("a").await.expect("get succeeds"), None);

    models
        .save("a", Arc::new("alpha".to_string()))
        .await
        .expect("save succeeds");
    let model = models.get("a").await.expect("get succeeds");
    assert_eq!(model.as_ref().map(|m| m.as_str()), Some("alpha"));

    models
        .save("a", Arc::new("beta".to_string()))
        .await
        .expect("overwrite succeeds");
    let model = models.get("a").await.expect("get succeeds");
    assert_eq!(model.as_ref().map(|m| m.as_str()), Some("beta"));

    models.delete("a").await.expect("delete succeeds");
    assert_eq!(models.get("a").await.expect("get succeeds"), None);
    models.delete("a").await.expect("repeated delete succeeds");
}
