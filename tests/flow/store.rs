//! In-memory flow store integration helpers.

use std::time::{Duration, SystemTime};

use catga_core::flow::{FlowContinuation, FlowState, FlowStore, SuspendedFlowStore};
use catga_core::memory::{MemoryFlows, MemorySuspendedFlows};
use serde_json::json;

fn state(id: &str, owner: &str) -> FlowState {
    FlowState::new(id, "payment", b"input".to_vec(), owner)
}

#[tokio::test]
async fn flow_store_uses_versions_for_updates_and_claims_stale_work() {
    let store = MemoryFlows::default();
    let initial = state("a", "node-a");

    assert!(store.create(initial.clone()).await.unwrap());
    assert!(!store.create(initial.clone()).await.unwrap());
    assert!(
        store
            .update(initial.version(), initial.clone().next_version().unwrap())
            .await
            .unwrap()
    );
    assert!(
        !store
            .update(initial.version(), initial.next_version().unwrap())
            .await
            .unwrap()
    );

    let stale = state("stale", "node-a").heartbeated_at(SystemTime::UNIX_EPOCH);
    assert!(store.create(stale).await.unwrap());
    let claimed = store
        .try_claim("payment", "node-b", Duration::from_secs(86_400))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.id(), "stale");
    assert_eq!(claimed.owner(), Some("node-b"));
    assert_eq!(claimed.version(), 1);
}

#[tokio::test]
async fn flow_store_recovers_stale_work_for_a_restarted_owner_with_the_same_id() {
    let store = MemoryFlows::default();
    store
        .create(
            FlowState::new("stale", "payment", b"input".to_vec(), "node-a")
                .heartbeated_at(SystemTime::UNIX_EPOCH),
        )
        .await
        .unwrap();

    let claimed = store
        .try_claim("payment", "node-a", Duration::from_secs(86_400))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(claimed.id(), "stale");
    assert_eq!(claimed.owner(), Some("node-a"));
    assert_eq!(claimed.version(), 1);
}

#[tokio::test]
async fn flow_stores_reject_version_saturation_as_a_successful_transition() {
    let state = state_at_version("version-limit", "node-a", i64::MAX);
    let error = state
        .clone()
        .next_version()
        .expect_err("the maximum flow version cannot advance");
    assert_eq!(error.code(), catga_core::ErrorCode::Conflict);

    let flows = MemoryFlows::default();
    assert!(flows.create(state.clone()).await.unwrap());
    assert!(
        !flows
            .update(i64::MAX, state.clone().at_step(1))
            .await
            .unwrap()
    );

    let continuation = FlowContinuation::new(state, "work");
    let suspended = MemorySuspendedFlows::default();
    assert!(suspended.create(continuation.clone()).await.unwrap());
    assert!(
        !suspended
            .update(
                i64::MAX,
                continuation
                    .clone()
                    .with_state(continuation.state().clone().at_step(1)),
            )
            .await
            .unwrap()
    );
}

fn state_at_version(id: &str, owner: &str, version: i64) -> FlowState {
    let mut encoded = serde_json::to_value(state(id, owner)).expect("serialize flow state");
    encoded["version"] = json!(version);
    serde_json::from_value(encoded).expect("restore flow state with a durable test version")
}
