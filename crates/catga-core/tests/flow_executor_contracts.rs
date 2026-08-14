//! Contract coverage for durable flow execution: optimistic ownership,
//! heartbeats, stale-flow recovery, and supervised execution loops.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::{Duration, SystemTime},
};

use catga_core::{
    CatgaError, ErrorCode, assert_error_code, assert_failure, assert_success,
    flow::{
        FlowExecutor, FlowHeartbeatOptions, FlowRecoveryOptions, FlowResult, FlowState, FlowStatus,
        store::FlowStore,
    },
    memory::MemoryFlows,
};
use tokio_util::sync::CancellationToken;

async fn create_ghost(store: &MemoryFlows, id: &str, flow_type: &str, age: Duration) {
    let ghost =
        FlowState::new(id, flow_type, Vec::new(), "ghost").heartbeated_at(SystemTime::now() - age);
    assert!(assert_success(store.create(ghost).await));
}

#[test]
fn executor_options_validate_bounds() {
    assert_error_code(
        FlowHeartbeatOptions::new(Duration::ZERO),
        ErrorCode::Validation,
    );
    let heartbeat = assert_success(FlowHeartbeatOptions::new(Duration::from_millis(5)));
    assert_eq!(heartbeat.interval, Duration::from_millis(5));

    assert_error_code(
        FlowRecoveryOptions::new(0, Duration::from_millis(1)),
        ErrorCode::Validation,
    );
    assert_error_code(
        FlowRecoveryOptions::new(1, Duration::ZERO),
        ErrorCode::Validation,
    );
    let recovery = assert_success(FlowRecoveryOptions::new(2, Duration::from_millis(1)));
    assert_eq!(recovery.max_claims, 2);
}

#[tokio::test]
async fn execute_creates_completes_and_replays_terminal_result() {
    let store = Arc::new(MemoryFlows::default());
    let executor = FlowExecutor::new(store.clone(), "worker-a", Duration::from_secs(60));
    let runs = Arc::new(AtomicU32::new(0));

    let result = assert_success(
        executor
            .execute("flow-1", "orders", vec![1u8, 2], {
                let runs = runs.clone();
                move |state: FlowState| async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(state.owner(), Some("worker-a"));
                    assert_eq!(state.data(), &[1, 2]);
                    Ok(FlowResult::success(3))
                }
            })
            .await,
    );
    assert!(result.is_ok());
    assert_eq!(result.completed_steps(), 3);
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    let stored = assert_success(store.get("flow-1").await).expect("flow persisted");
    assert_eq!(stored.status(), FlowStatus::Done);
    assert_eq!(stored.step(), 3);
    // Terminal flows release their ownership lease.
    assert_eq!(stored.owner(), None);

    // A completed flow replays its stored result without running the closure.
    let replay = assert_success(
        executor
            .execute("flow-1", "orders", Vec::new(), move |_state| async move {
                Ok(FlowResult::success(9))
            })
            .await,
    );
    assert!(replay.is_ok());
    assert_eq!(replay.completed_steps(), 3);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn execute_failure_persists_and_replays_the_error() {
    let store = Arc::new(MemoryFlows::default());
    let executor = FlowExecutor::new(store.clone(), "worker-a", Duration::from_secs(60));

    let failed = assert_success(
        executor
            .execute("flow-2", "orders", Vec::new(), |_state| async move {
                Err(CatgaError::new(ErrorCode::Internal, "boom"))
            })
            .await,
    );
    assert!(!failed.is_ok());
    let error = failed.error().expect("failure retains its error");
    assert_eq!(error.code(), ErrorCode::Internal);

    let stored = assert_success(store.get("flow-2").await).expect("flow persisted");
    assert_eq!(stored.status(), FlowStatus::Failed);
    assert_eq!(
        stored.error().map(CatgaError::code),
        Some(ErrorCode::Internal)
    );

    // Re-executing a failed flow replays the stored failure.
    let replay = assert_success(
        executor
            .execute("flow-2", "orders", Vec::new(), |_state| async move {
                Ok(FlowResult::success(1))
            })
            .await,
    );
    assert_eq!(
        replay.error().map(CatgaError::code),
        Some(ErrorCode::Internal)
    );
}

#[tokio::test]
async fn fresh_flow_owned_elsewhere_is_transient_and_type_mismatch_conflicts() {
    let store = Arc::new(MemoryFlows::default());
    // A fresh heartbeat keeps the ghost flow unclaimable.
    let fresh = FlowState::new("flow-3", "orders", Vec::new(), "ghost");
    assert!(assert_success(store.create(fresh).await));

    let executor = FlowExecutor::new(store.clone(), "worker-b", Duration::from_secs(60));
    assert_error_code(
        executor
            .execute("flow-3", "orders", Vec::new(), |_state| async move {
                Ok(FlowResult::success(1))
            })
            .await,
        ErrorCode::Transient,
    );

    // The same identity with a different flow type is a conflict.
    assert_error_code(
        executor
            .execute("flow-3", "payments", Vec::new(), |_state| async move {
                Ok(FlowResult::success(1))
            })
            .await,
        ErrorCode::Conflict,
    );
}

#[tokio::test]
async fn stale_flow_is_claimed_and_completed() {
    let store = Arc::new(MemoryFlows::default());
    create_ghost(&store, "flow-4", "orders", Duration::from_secs(3600)).await;

    let executor = FlowExecutor::new(store.clone(), "worker-a", Duration::from_secs(60));
    let result = assert_success(
        executor
            .execute(
                "flow-4",
                "orders",
                Vec::new(),
                |state: FlowState| async move {
                    assert_eq!(state.owner(), Some("worker-a"));
                    assert_eq!(state.version(), 1);
                    Ok(FlowResult::success(2))
                },
            )
            .await,
    );
    assert_eq!(result.completed_steps(), 2);
    let stored = assert_success(store.get("flow-4").await).expect("flow persisted");
    assert_eq!(stored.status(), FlowStatus::Done);
    assert_eq!(stored.owner(), None);
}

#[tokio::test]
async fn heartbeat_guards_owner_and_version() {
    let store = Arc::new(MemoryFlows::default());
    let fresh = FlowState::new("flow-5", "orders", Vec::new(), "worker-a");
    assert!(assert_success(store.create(fresh).await));

    let owner = FlowExecutor::new(store.clone(), "worker-a", Duration::from_secs(60));
    let stranger = FlowExecutor::new(store.clone(), "worker-b", Duration::from_secs(60));

    assert!(assert_success(owner.heartbeat("flow-5", 0).await));
    // Wrong owner, wrong version, and unknown ids are all refused.
    assert!(!assert_success(stranger.heartbeat("flow-5", 0).await));
    assert!(!assert_success(owner.heartbeat("flow-5", 99).await));
    assert!(!assert_success(owner.heartbeat("missing", 0).await));
}

#[tokio::test]
async fn execute_with_heartbeat_renews_ownership_and_completes() {
    let store = Arc::new(MemoryFlows::default());
    let executor = FlowExecutor::new(store.clone(), "worker-a", Duration::from_secs(60));
    let options = assert_success(FlowHeartbeatOptions::new(Duration::from_millis(5)));
    let before = SystemTime::now();

    let result = assert_success(
        executor
            .execute_with_heartbeat(
                "flow-6",
                "orders",
                Vec::new(),
                options,
                CancellationToken::new(),
                |_state| async move {
                    tokio::time::sleep(Duration::from_millis(40)).await;
                    Ok(FlowResult::success(1))
                },
            )
            .await,
    );
    assert!(result.is_ok());
    let stored = assert_success(store.get("flow-6").await).expect("flow persisted");
    assert_eq!(stored.status(), FlowStatus::Done);
    // Periodic heartbeats renewed the lease while the work was pending.
    assert!(stored.heartbeat() > before);
}

#[tokio::test]
async fn execute_with_heartbeat_cancellation_keeps_flow_running() {
    let store = Arc::new(MemoryFlows::default());
    let executor = Arc::new(FlowExecutor::new(
        store.clone(),
        "worker-a",
        Duration::from_secs(60),
    ));
    let options = assert_success(FlowHeartbeatOptions::new(Duration::from_millis(50)));

    // An already-cancelled token aborts before any work can resolve; the work
    // future stays pending so only the cancellation branch can win the race.
    let pre_cancelled = CancellationToken::new();
    pre_cancelled.cancel();
    assert_error_code(
        executor
            .execute_with_heartbeat(
                "flow-7",
                "orders",
                Vec::new(),
                options,
                pre_cancelled,
                |_state| async move {
                    tokio::time::sleep(Duration::from_secs(600)).await;
                    Ok(FlowResult::success(1))
                },
            )
            .await,
        ErrorCode::Cancelled,
    );
    let stored = assert_success(store.get("flow-7").await).expect("flow persisted");
    assert_eq!(stored.status(), FlowStatus::Running);

    // Cancelling mid-work drops the work future and leaves the flow running,
    // so a later recovery sweep can claim it after the lease goes stale.
    let token = CancellationToken::new();
    let task = {
        let executor = Arc::clone(&executor);
        let token = token.clone();
        tokio::spawn(async move {
            executor
                .execute_with_heartbeat(
                    "flow-8",
                    "orders",
                    Vec::new(),
                    options,
                    token,
                    |_state| async move {
                        tokio::time::sleep(Duration::from_secs(600)).await;
                        Ok(FlowResult::success(1))
                    },
                )
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    token.cancel();
    let error = assert_failure(task.await.expect("executor task panicked"));
    assert_eq!(error.code(), ErrorCode::Cancelled);
    let stored = assert_success(store.get("flow-8").await).expect("flow persisted");
    assert_eq!(stored.status(), FlowStatus::Running);
}

#[tokio::test]
async fn execute_with_heartbeat_reports_lost_ownership() {
    let store = Arc::new(MemoryFlows::default());
    let executor = Arc::new(FlowExecutor::new(
        store.clone(),
        "worker-a",
        Duration::from_secs(60),
    ));
    let options = assert_success(FlowHeartbeatOptions::new(Duration::from_millis(5)));

    let task = {
        let executor = Arc::clone(&executor);
        tokio::spawn(async move {
            executor
                .execute_with_heartbeat(
                    "flow-9",
                    "orders",
                    Vec::new(),
                    options,
                    CancellationToken::new(),
                    |_state| async move {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        Ok(FlowResult::success(1))
                    },
                )
                .await
        })
    };

    // A rival executor claims the flow (zero staleness threshold forces the
    // claim); the next heartbeat must then report lost ownership.
    let claimed = loop {
        if let Some(state) =
            assert_success(store.try_claim("orders", "worker-b", Duration::ZERO).await)
        {
            break state;
        }
        tokio::task::yield_now().await;
    };
    assert_eq!(claimed.owner(), Some("worker-b"));

    let error = assert_failure(task.await.expect("executor task panicked"));
    assert_eq!(error.code(), ErrorCode::Conflict);
    let stored = assert_success(store.get("flow-9").await).expect("flow persisted");
    assert_eq!(stored.status(), FlowStatus::Running);
    assert_eq!(stored.owner(), Some("worker-b"));
}

#[tokio::test]
async fn execute_with_heartbeat_persists_work_errors() {
    let store = Arc::new(MemoryFlows::default());
    let executor = FlowExecutor::new(store.clone(), "worker-a", Duration::from_secs(60));
    let options = assert_success(FlowHeartbeatOptions::new(Duration::from_millis(50)));

    let failed = assert_success(
        executor
            .execute_with_heartbeat(
                "flow-10",
                "orders",
                Vec::new(),
                options,
                CancellationToken::new(),
                |_state| async move { Err(CatgaError::new(ErrorCode::Unavailable, "down")) },
            )
            .await,
    );
    assert_eq!(
        failed.error().map(CatgaError::code),
        Some(ErrorCode::Unavailable)
    );
    let stored = assert_success(store.get("flow-10").await).expect("flow persisted");
    assert_eq!(stored.status(), FlowStatus::Failed);
}

#[tokio::test]
async fn recover_stale_claims_only_matching_stale_flows() {
    let store = Arc::new(MemoryFlows::default());
    create_ghost(&store, "stale-1", "orders", Duration::from_secs(3600)).await;
    create_ghost(&store, "stale-2", "orders", Duration::from_secs(3600)).await;
    // A different flow type and a fresh flow are never claimed.
    create_ghost(&store, "stale-3", "payments", Duration::from_secs(3600)).await;
    let fresh = FlowState::new("fresh-1", "orders", Vec::new(), "ghost");
    assert!(assert_success(store.create(fresh).await));

    let executor = FlowExecutor::new(store.clone(), "recoverer", Duration::from_secs(60));
    let options = assert_success(FlowRecoveryOptions::new(5, Duration::from_millis(1)));
    let runs = Arc::new(AtomicU32::new(0));

    let recovered = assert_success(
        executor
            .recover_stale("orders", options, {
                let runs = runs.clone();
                move |state: FlowState| {
                    let runs = runs.clone();
                    async move {
                        runs.fetch_add(1, Ordering::SeqCst);
                        assert_eq!(state.owner(), Some("recoverer"));
                        Ok(FlowResult::success(1))
                    }
                }
            })
            .await,
    );
    assert_eq!(recovered, 2);
    assert_eq!(runs.load(Ordering::SeqCst), 2);
    assert_eq!(
        assert_success(store.get("stale-1").await)
            .expect("flow persisted")
            .status(),
        FlowStatus::Done
    );

    // A follow-up sweep finds nothing stale of that type.
    let again = assert_success(
        executor
            .recover_stale("orders", options, |_state| async move {
                Ok(FlowResult::success(1))
            })
            .await,
    );
    assert_eq!(again, 0);
}

#[tokio::test]
async fn recover_stale_honors_the_claim_budget() {
    let store = Arc::new(MemoryFlows::default());
    create_ghost(&store, "budget-1", "orders", Duration::from_secs(3600)).await;
    create_ghost(&store, "budget-2", "orders", Duration::from_secs(3600)).await;

    let executor = FlowExecutor::new(store.clone(), "recoverer", Duration::from_secs(60));
    let options = assert_success(FlowRecoveryOptions::new(1, Duration::from_millis(1)));
    let recovered = assert_success(
        executor
            .recover_stale("orders", options, |_state| async move {
                Ok(FlowResult::success(1))
            })
            .await,
    );
    assert_eq!(recovered, 1);
    // Exactly one flow stays running for the next sweep.
    let statuses = [
        assert_success(store.get("budget-1").await)
            .expect("flow persisted")
            .status(),
        assert_success(store.get("budget-2").await)
            .expect("flow persisted")
            .status(),
    ];
    assert_eq!(
        statuses
            .iter()
            .filter(|status| **status == FlowStatus::Running)
            .count(),
        1
    );
}

#[tokio::test]
async fn run_recovery_loop_stops_on_cancellation() {
    let store = Arc::new(MemoryFlows::default());
    create_ghost(&store, "loop-1", "orders", Duration::from_secs(3600)).await;
    let executor = Arc::new(FlowExecutor::new(
        store.clone(),
        "recoverer",
        Duration::from_secs(60),
    ));
    let options = assert_success(FlowRecoveryOptions::new(4, Duration::from_millis(5)));

    // An already-cancelled token stops before any sweep.
    let pre_cancelled = CancellationToken::new();
    pre_cancelled.cancel();
    assert_success(
        executor
            .run_recovery_loop("orders", options, pre_cancelled, |_state| async move {
                Ok(FlowResult::success(1))
            })
            .await,
    );

    // Cancelling during the poll interval ends the loop after the sweeps done so far.
    let token = CancellationToken::new();
    let task = {
        let executor = Arc::clone(&executor);
        let token = token.clone();
        tokio::spawn(async move {
            executor
                .run_recovery_loop("orders", options, token, |_state| async move {
                    Ok(FlowResult::success(1))
                })
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    token.cancel();
    assert_success(task.await.expect("recovery task panicked"));
    assert_eq!(
        assert_success(store.get("loop-1").await)
            .expect("flow persisted")
            .status(),
        FlowStatus::Done
    );
}
