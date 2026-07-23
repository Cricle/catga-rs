use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use catga_core::{CatgaError, ErrorCode};
use catga_flow::{FlowExecutor, FlowResult, FlowState, FlowStatus, FlowStore};
use catga_memory::MemoryFlows;

#[tokio::test]
async fn executor_persists_terminal_results_and_deduplicates_completed_ids() {
    let store = Arc::new(MemoryFlows::default());
    let executor = FlowExecutor::new(Arc::clone(&store), "node-a", Duration::from_secs(30));
    let invocations = Arc::new(AtomicUsize::new(0));

    let first_count = Arc::clone(&invocations);
    let first = executor
        .execute("flow-7", "payment", b"input".to_vec(), move |state| {
            let invocations = Arc::clone(&first_count);
            async move {
                invocations.fetch_add(1, Ordering::Relaxed);
                Ok(FlowResult::success(state.step() + 2))
            }
        })
        .await
        .unwrap();
    assert!(first.is_success());

    let second_count = Arc::clone(&invocations);
    let second = executor
        .execute("flow-7", "payment", b"input".to_vec(), move |_| {
            let invocations = Arc::clone(&second_count);
            async move {
                invocations.fetch_add(1, Ordering::Relaxed);
                Ok(FlowResult::success(99))
            }
        })
        .await
        .unwrap();

    assert!(second.is_success());
    assert_eq!(second.completed_steps(), 2);
    assert_eq!(invocations.load(Ordering::Relaxed), 1);
    assert_eq!(
        store.get("flow-7").await.unwrap().unwrap().status(),
        FlowStatus::Done
    );
}

#[tokio::test]
async fn executor_claims_stale_work_and_persists_action_failure() {
    let store = Arc::new(MemoryFlows::default());
    store
        .create(
            FlowState::new("flow-8", "payment", b"input".to_vec(), "dead-node")
                .heartbeated_at(SystemTime::UNIX_EPOCH),
        )
        .await
        .unwrap();
    let executor = FlowExecutor::new(Arc::clone(&store), "node-b", Duration::ZERO);

    let result = executor
        .execute("flow-8", "payment", b"input".to_vec(), |_| async {
            Ok(FlowResult::failure(
                1,
                CatgaError::new(ErrorCode::Transient, "charge failed"),
            ))
        })
        .await
        .unwrap();

    assert!(!result.is_success());
    let persisted = store.get("flow-8").await.unwrap().unwrap();
    assert_eq!(persisted.status(), FlowStatus::Failed);
    assert_eq!(persisted.error().unwrap().message(), "charge failed");
}

#[tokio::test]
async fn executor_heartbeat_advances_version_only_for_its_current_owner() {
    let store = Arc::new(MemoryFlows::default());
    store
        .create(FlowState::new(
            "flow-9",
            "payment",
            b"input".to_vec(),
            "node-a",
        ))
        .await
        .unwrap();
    let executor = FlowExecutor::new(Arc::clone(&store), "node-a", Duration::from_secs(30));

    assert!(executor.heartbeat("flow-9", 0).await.unwrap());
    assert!(!executor.heartbeat("flow-9", 0).await.unwrap());
    assert_eq!(store.get("flow-9").await.unwrap().unwrap().version(), 1);
}
