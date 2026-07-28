//! Flow executor CAS, lease, and terminal-result contracts.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, UNIX_EPOCH},
};

use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{FlowExecutor, FlowResult, FlowState, FlowStatus, FlowStore};
use tokio::sync::Mutex;

#[derive(Default)]
struct Store {
    states: Mutex<HashMap<Box<str>, FlowState>>,
}

impl Store {
    async fn insert(&self, state: FlowState) {
        self.states.lock().await.insert(state.id().into(), state);
    }
}

#[async_trait]
impl FlowStore for Store {
    async fn create(&self, state: FlowState) -> CatgaResult<bool> {
        let mut states = self.states.lock().await;
        if states.contains_key(state.id()) {
            return Ok(false);
        }
        states.insert(state.id().into(), state);
        Ok(true)
    }

    async fn update(&self, expected_version: i64, next: FlowState) -> CatgaResult<bool> {
        let mut states = self.states.lock().await;
        let Some(current) = states.get(next.id()) else {
            return Ok(false);
        };
        if current.version() != expected_version
            || !FlowState::is_next_version(expected_version, next.version())
        {
            return Ok(false);
        }
        states.insert(next.id().into(), next);
        Ok(true)
    }

    async fn get(&self, id: &str) -> CatgaResult<Option<FlowState>> {
        Ok(self.states.lock().await.get(id).cloned())
    }

    async fn try_claim(
        &self,
        flow_type: &str,
        owner: &str,
        stale_after: Duration,
    ) -> CatgaResult<Option<FlowState>> {
        let mut states = self.states.lock().await;
        let Some((id, state)) = states
            .iter()
            .find(|(_, state)| {
                state.flow_type() == flow_type
                    && state.status() == FlowStatus::Running
                    && state
                        .heartbeat()
                        .elapsed()
                        .is_ok_and(|elapsed| elapsed >= stale_after)
            })
            .map(|(id, state)| (id.clone(), state.clone()))
        else {
            return Ok(None);
        };
        let claimed = state.claimed_by(owner).next_version()?;
        states.insert(id, claimed.clone());
        Ok(Some(claimed))
    }

    async fn heartbeat(&self, id: &str, owner: &str, version: i64) -> CatgaResult<bool> {
        let mut states = self.states.lock().await;
        let Some(current) = states.get(id).cloned() else {
            return Ok(false);
        };
        if current.owner() != Some(owner) || current.version() != version {
            return Ok(false);
        }
        states.insert(
            id.into(),
            current.heartbeated_at(std::time::SystemTime::now()),
        );
        Ok(true)
    }
}

#[tokio::test]
async fn executor_persists_terminal_result_and_does_not_replay_completed_work() -> CatgaResult<()> {
    let store = Arc::new(Store::default());
    let executor = FlowExecutor::new(Arc::clone(&store), "worker-a", Duration::from_secs(1));
    let invocations = Arc::new(AtomicUsize::new(0));
    let first_calls = Arc::clone(&invocations);

    let first = executor
        .execute("once", "checkout", [], move |_| {
            let first_calls = Arc::clone(&first_calls);
            async move {
                first_calls.fetch_add(1, Ordering::SeqCst);
                Ok(FlowResult::success(2))
            }
        })
        .await?;
    assert!(first.is_success());
    assert_eq!(first.completed_steps(), 2);
    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    let stored = store.get("once").await?.expect("terminal flow persists");
    assert_eq!(stored.status(), FlowStatus::Done);
    assert_eq!(stored.version(), 1);

    let replay_calls = Arc::clone(&invocations);
    let replay = executor
        .execute("once", "checkout", [], move |_| {
            let replay_calls = Arc::clone(&replay_calls);
            async move {
                replay_calls.fetch_add(1, Ordering::SeqCst);
                Ok(FlowResult::success(99))
            }
        })
        .await?;
    assert_eq!(replay.completed_steps(), 2);
    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn executor_claims_only_stale_owners_and_preserves_lease_fencing() -> CatgaResult<()> {
    let store = Arc::new(Store::default());
    store
        .insert(FlowState::new("stale", "checkout", [], "old").heartbeated_at(UNIX_EPOCH))
        .await;
    store
        .insert(FlowState::new("active", "checkout", [], "other"))
        .await;
    let executor = FlowExecutor::new(Arc::clone(&store), "worker-b", Duration::from_secs(1));

    assert!(!executor.heartbeat("stale", 0).await?);
    let stale = executor
        .execute("stale", "checkout", [], |_| async {
            Ok(FlowResult::success(1))
        })
        .await?;
    assert!(stale.is_success());
    let stored = store
        .get("stale")
        .await?
        .expect("stale flow survives claim");
    assert_eq!(stored.status(), FlowStatus::Done);
    assert_eq!(stored.version(), 2);

    let active = executor
        .execute("active", "checkout", [], |_| async {
            Ok(FlowResult::success(1))
        })
        .await
        .expect_err("a fresh foreign lease must not be stolen");
    assert_eq!(active.code(), ErrorCode::Transient);
    let conflicting_type = executor
        .execute("active", "refund", [], |_| async {
            Ok(FlowResult::success(1))
        })
        .await
        .expect_err("flow identities cannot change type");
    assert_eq!(conflicting_type.code(), ErrorCode::Conflict);
    Ok(())
}

#[tokio::test]
async fn executor_converts_work_errors_to_terminal_failures() -> CatgaResult<()> {
    let store = Arc::new(Store::default());
    let executor = FlowExecutor::new(store.clone(), "worker-c", Duration::from_secs(1));
    let result = executor
        .execute("fails", "checkout", [], |_| async {
            Err(CatgaError::new(
                ErrorCode::Transient,
                "downstream unavailable",
            ))
        })
        .await?;

    assert!(!result.is_success());
    assert_eq!(
        result.error().map(CatgaError::code),
        Some(ErrorCode::Transient)
    );
    let stored = store.get("fails").await?.expect("failure persists");
    assert_eq!(stored.status(), FlowStatus::Failed);
    assert_eq!(
        stored.error().map(CatgaError::code),
        Some(ErrorCode::Transient)
    );
    Ok(())
}
