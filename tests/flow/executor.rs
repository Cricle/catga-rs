//! Shared flow executor integration helpers.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_core::flow::{
    FlowExecutor, FlowHeartbeatOptions, FlowRecoveryOptions, FlowResult, FlowState, FlowStatus,
    FlowStore,
};
use catga_memory::MemoryFlows;
use tokio::sync::Notify;

struct HeartbeatDuringTerminalUpdateStore {
    inner: Arc<MemoryFlows>,
    heartbeat_injected: AtomicUsize,
}

struct CountingHeartbeatStore {
    inner: Arc<MemoryFlows>,
    heartbeats: AtomicUsize,
    accept_heartbeats: bool,
}

#[async_trait]
impl FlowStore for CountingHeartbeatStore {
    async fn create(&self, state: FlowState) -> CatgaResult<bool> {
        self.inner.create(state).await
    }

    async fn update(&self, expected_version: i64, next: FlowState) -> CatgaResult<bool> {
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
        self.heartbeats.fetch_add(1, Ordering::SeqCst);
        if !self.accept_heartbeats {
            return Ok(false);
        }
        self.inner.heartbeat(id, owner, version).await
    }
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

#[test]
fn executor_policies_reject_zero_bounds_before_any_store_operation() {
    let heartbeat = FlowHeartbeatOptions::new(Duration::ZERO)
        .expect_err("a zero heartbeat interval is invalid");
    let claim_limit = FlowRecoveryOptions::new(0, Duration::from_millis(1))
        .expect_err("a zero recovery claim limit is invalid");
    let poll_interval = FlowRecoveryOptions::new(1, Duration::ZERO)
        .expect_err("a zero recovery poll interval is invalid");

    for error in [heartbeat, claim_limit, poll_interval] {
        assert_eq!(error.code(), ErrorCode::Validation);
    }
}

#[tokio::test]
async fn executor_rejects_a_live_flow_owned_by_another_node_without_running_work() {
    let store = Arc::new(MemoryFlows::default());
    store
        .create(FlowState::new(
            "live-other-owner",
            "payment",
            b"input".to_vec(),
            "node-a",
        ))
        .await
        .expect("seed a running flow");
    let executor = FlowExecutor::new(store, "node-b", Duration::from_secs(60));
    let invocations = Arc::new(AtomicUsize::new(0));
    let invocation_count = Arc::clone(&invocations);

    let error = executor
        .execute(
            "live-other-owner",
            "payment",
            b"input".to_vec(),
            move |_| {
                let invocation_count = Arc::clone(&invocation_count);
                async move {
                    invocation_count.fetch_add(1, Ordering::SeqCst);
                    Ok(FlowResult::success(1))
                }
            },
        )
        .await
        .expect_err("a live owner must retain its lease");

    assert_eq!(error.code(), ErrorCode::Transient);
    assert_eq!(invocations.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn executor_rejects_reusing_a_flow_id_for_a_different_type() {
    let store = Arc::new(MemoryFlows::default());
    store
        .create(FlowState::new(
            "type-conflict",
            "payment",
            b"input".to_vec(),
            "node-a",
        ))
        .await
        .expect("seed a running flow");
    let executor = FlowExecutor::new(store, "node-b", Duration::ZERO);

    let error = executor
        .execute("type-conflict", "refund", b"input".to_vec(), |_| async {
            Ok(FlowResult::success(1))
        })
        .await
        .expect_err("one flow id must keep its durable type");

    assert_eq!(error.code(), ErrorCode::Conflict);
}

#[tokio::test(start_paused = true)]
async fn supervised_executor_heartbeats_pending_work_before_persisting_its_result() {
    let store = Arc::new(CountingHeartbeatStore {
        inner: Arc::new(MemoryFlows::default()),
        heartbeats: AtomicUsize::new(0),
        accept_heartbeats: true,
    });
    let executor = Arc::new(FlowExecutor::new(
        Arc::clone(&store),
        "node-a",
        Duration::from_secs(30),
    ));
    let release = Arc::new(Notify::new());
    let release_work = Arc::clone(&release);
    let task = tokio::spawn({
        let executor = Arc::clone(&executor);
        async move {
            executor
                .execute_with_heartbeat(
                    "heartbeating-work",
                    "payment",
                    b"input".to_vec(),
                    FlowHeartbeatOptions::new(Duration::from_secs(1))?,
                    tokio_util::sync::CancellationToken::new(),
                    move |_| async move {
                        release_work.notified().await;
                        Ok(FlowResult::success(1))
                    },
                )
                .await
        }
    });

    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(store.heartbeats.load(Ordering::SeqCst), 1);

    release.notify_one();
    let result = task
        .await
        .expect("supervised flow task must not panic")
        .expect("the heartbeat owner must retain its flow");
    assert!(result.is_success());
    assert_eq!(
        store
            .inner
            .get("heartbeating-work")
            .await
            .expect("load completed flow")
            .expect("completed flow is retained")
            .status(),
        FlowStatus::Done
    );
}

#[tokio::test(start_paused = true)]
async fn supervised_executor_stops_when_its_heartbeat_loses_ownership() {
    let store = Arc::new(CountingHeartbeatStore {
        inner: Arc::new(MemoryFlows::default()),
        heartbeats: AtomicUsize::new(0),
        accept_heartbeats: false,
    });
    let executor = Arc::new(FlowExecutor::new(
        Arc::clone(&store),
        "node-a",
        Duration::from_secs(30),
    ));
    let task = tokio::spawn({
        let executor = Arc::clone(&executor);
        async move {
            executor
                .execute_with_heartbeat(
                    "lost-heartbeat",
                    "payment",
                    b"input".to_vec(),
                    FlowHeartbeatOptions::new(Duration::from_secs(1))?,
                    tokio_util::sync::CancellationToken::new(),
                    |_| async { std::future::pending::<CatgaResult<FlowResult>>().await },
                )
                .await
        }
    });

    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    let error = task
        .await
        .expect("supervised flow task must not panic")
        .expect_err("a rejected heartbeat must stop the caller-owned work future");

    assert_eq!(error.code(), ErrorCode::Conflict);
    assert_eq!(store.heartbeats.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn executor_recovers_only_the_requested_bounded_number_of_stale_flows() {
    let store = Arc::new(MemoryFlows::default());
    for id in ["recover-one", "recover-two", "other-type"] {
        let flow_type = if id == "other-type" {
            "refund"
        } else {
            "payment"
        };
        store
            .create(
                FlowState::new(id, flow_type, b"input".to_vec(), "dead-node")
                    .heartbeated_at(SystemTime::UNIX_EPOCH),
            )
            .await
            .expect("seed stale flow");
    }
    let executor = FlowExecutor::new(Arc::clone(&store), "node-b", Duration::ZERO);
    let recovered = executor
        .recover_stale(
            "payment",
            FlowRecoveryOptions::new(1, Duration::from_secs(1))
                .expect("nonzero recovery bounds are valid"),
            |state| async move { Ok(FlowResult::success(state.step() + 1)) },
        )
        .await
        .expect("recovery sweep completes");

    assert_eq!(recovered, 1);
    let first = store
        .get("recover-one")
        .await
        .expect("load first seeded flow")
        .expect("first seeded flow retained");
    let second = store
        .get("recover-two")
        .await
        .expect("load second seeded flow")
        .expect("second seeded flow retained");
    let completed = usize::from(first.status() == FlowStatus::Done)
        + usize::from(second.status() == FlowStatus::Done);
    assert_eq!(completed, 1);
    assert_eq!(
        store
            .get("other-type")
            .await
            .expect("load nonmatching flow")
            .expect("nonmatching flow retained")
            .status(),
        FlowStatus::Running
    );
}
