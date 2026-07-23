use std::time::{Duration, SystemTime};

use catga_flow::{FlowState, FlowStore};
use catga_memory::MemoryFlows;

fn state(id: &str, owner: &str) -> FlowState {
    FlowState::new(id, "payment", b"input".to_vec(), owner)
}

#[tokio::test]
async fn flow_store_uses_versions_for_updates_and_claims_stale_work() {
    let store = MemoryFlows::default();
    let initial = state("a", "node-a");

    assert!(store.create(initial.clone()).await.unwrap());
    assert!(!store.create(initial.clone()).await.unwrap());
    assert!(store
        .update(initial.version(), initial.clone().next_version())
        .await
        .unwrap());
    assert!(!store
        .update(initial.version(), initial.next_version())
        .await
        .unwrap());

    let stale = state("stale", "node-a").heartbeated_at(SystemTime::UNIX_EPOCH);
    assert!(store.create(stale).await.unwrap());
    let claimed = store
        .try_claim("payment", "node-b", Duration::ZERO)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.id(), "stale");
    assert_eq!(claimed.owner(), Some("node-b"));
    assert_eq!(claimed.version(), 1);
}
