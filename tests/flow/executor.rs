use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{FlowExecutor, FlowResult, FlowState, FlowStatus, FlowStore};
use catga_memory::MemoryFlows;

struct HeartbeatDuringTerminalUpdateStore {
    inner: Arc<MemoryFlows>,
    heartbeat_injected: AtomicUsize,
}

#[async_trait]
impl FlowStore for HeartbeatDuringTerminalUpdateStore {
    async fn create(&self, state: FlowState) -> CatgaResult<bool> {
        self.inner.create(state).await
    }

    async fn update(&self, expected_version: i64, next: FlowState) -> CatgaResult<bool> {
        if next.status().is_terminal()
            && self.heartbeat_injected.fetch_add(1, Ordering::SeqCst) == 0
        {
            assert!(
                self.inner
                    .heartbeat(next.id(), "node-a", expected_version)
                    .await?
            );
            return Ok(false);
        }
        self.inner.update(expected_version, next).await
    }

    async fn get(&self, id: &str) -> CatgaResult<Option<FlowState>> {
        self.inner.get(id).await
    }

    async fn try_claim(
        &self,
        flow_type: &str,
        owner: &str,
        stale_after: Duration,
    ) -> CatgaResult<Option<FlowState>> {
        self.inner.try_claim(flow_type, owner, stale_after).await
    }

    async fn heartbeat(&self, id: &str, owner: &str, version: i64) -> CatgaResult<bool> {
        self.inner.heartbeat(id, owner, version).await
    }
}

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
    assert!(executor.heartbeat("flow-9", 0).await.unwrap());
    assert_eq!(store.get("flow-9").await.unwrap().unwrap().version(), 0);
}

#[tokio::test]
async fn executor_completes_after_an_inflight_heartbeat() {
    let store = Arc::new(MemoryFlows::default());
    let executor = Arc::new(FlowExecutor::new(
        Arc::clone(&store),
        "node-a",
        Duration::from_secs(30),
    ));
    let heartbeater = Arc::clone(&executor);

    let result = executor
        .execute("flow-10", "payment", b"input".to_vec(), move |state| {
            let executor = Arc::clone(&heartbeater);
            async move {
                assert!(executor.heartbeat("flow-10", state.version()).await?);
                Ok(FlowResult::success(1))
            }
        })
        .await
        .unwrap();

    assert!(result.is_success());
    assert_eq!(
        store.get("flow-10").await.unwrap().unwrap().status(),
        FlowStatus::Done
    );
}

#[tokio::test]
async fn executor_retries_terminal_persistence_after_a_same_version_heartbeat_race() {
    let inner = Arc::new(MemoryFlows::default());
    let store = Arc::new(HeartbeatDuringTerminalUpdateStore {
        inner: Arc::clone(&inner),
        heartbeat_injected: AtomicUsize::new(0),
    });
    let executor = FlowExecutor::new(store, "node-a", Duration::from_secs(30));
    let invocations = Arc::new(AtomicUsize::new(0));

    let run_count = Arc::clone(&invocations);
    let result = executor
        .execute(
            "flow-terminal-heartbeat-race",
            "payment",
            b"input".to_vec(),
            move |state| {
                let invocations = Arc::clone(&run_count);
                async move {
                    invocations.fetch_add(1, Ordering::Relaxed);
                    Ok(FlowResult::success(state.step() + 1))
                }
            },
        )
        .await
        .unwrap();

    assert!(result.is_success());
    assert_eq!(invocations.load(Ordering::Relaxed), 1);
    let persisted = inner
        .get("flow-terminal-heartbeat-race")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(persisted.status(), FlowStatus::Done);
    assert_eq!(persisted.step(), 1);
}

#[tokio::test]
async fn executor_replays_failed_completed_steps_from_persistent_state() {
    let store = Arc::new(MemoryFlows::default());
    let executor = FlowExecutor::new(Arc::clone(&store), "node-a", Duration::from_secs(30));

    let first = executor
        .execute("flow-11", "payment", b"input".to_vec(), |_| async {
            Ok(FlowResult::failure(
                2,
                CatgaError::new(ErrorCode::Transient, "charge failed"),
            ))
        })
        .await
        .unwrap();
    let replayed = executor
        .execute("flow-11", "payment", b"input".to_vec(), |_| async {
            Ok(FlowResult::success(99))
        })
        .await
        .unwrap();

    assert_eq!(first.completed_steps(), 2);
    assert_eq!(replayed.completed_steps(), 2);
    assert_eq!(replayed.error().unwrap().message(), "charge failed");
}
