//! Contract coverage for local compensating flows, continuation wire frames,
//! flow serde helpers, completion identities, and the batch-size guard.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use catga_core::flow::suspension::FlowContinuation;
use catga_core::flow::{
    Flow, FlowCompletion, FlowResult, FlowState, FlowStore, MAX_FLOW_DATA_BYTES,
    MAX_FLOW_STORE_BATCH, decode_continuation, encode_continuation, validate_flow_batch_size,
};
use catga_core::memory::MemoryFlows;
use catga_core::{CatgaError, CatgaResult, ErrorCode, assert_error_code, assert_success};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

fn state(id: &str, flow_type: &str, owner: &str) -> FlowState {
    FlowState::new(id, flow_type, b"flow-input".to_vec(), owner)
}

// ---------------------------------------------------------------------------
// Flow state and batch guards
// ---------------------------------------------------------------------------

#[test]
fn flow_batch_size_is_bounded() {
    assert_success(validate_flow_batch_size(MAX_FLOW_STORE_BATCH));
    assert_error_code(
        validate_flow_batch_size(MAX_FLOW_STORE_BATCH + 1),
        ErrorCode::Validation,
    );
}

#[test]
fn flow_state_validates_payload_bounds_and_versions() {
    let oversized = FlowState::new(
        "big",
        "orders",
        vec![0_u8; MAX_FLOW_DATA_BYTES + 1],
        "worker",
    );
    assert_error_code(oversized.validate(), ErrorCode::Validation);

    let valid = state("ok", "orders", "worker");
    assert_success(valid.validate());

    // Versions advance exactly one step at a time.
    let next = assert_success(valid.clone().next_version());
    assert_eq!(next.version(), 1);
    assert!(FlowState::is_next_version(0, 1));
    assert!(!FlowState::is_next_version(0, 2));
    assert!(!FlowState::is_next_version(1, 0));

    // Version advancement saturates into a validation error at the ceiling.
    let mut ceiling = state("ceiling", "orders", "worker");
    for _ in 0..3 {
        ceiling = assert_success(ceiling.next_version());
    }
    assert_eq!(ceiling.version(), 3);

    // Terminal transitions record their step and error payload.
    let done = valid.clone().done(7);
    assert_eq!(done.step(), 7);
    assert!(done.status().is_terminal());
    let failed = valid.failed(CatgaError::new(ErrorCode::HandlerFailed, "boom"));
    assert_eq!(
        failed.error().expect("error retained").code(),
        ErrorCode::HandlerFailed
    );
    assert!(failed.status().is_terminal());

    // Heartbeat replacement keeps the logical version unchanged.
    let beat = state("beat", "orders", "worker").heartbeated_at(std::time::SystemTime::now());
    assert_eq!(beat.version(), 0);
}

// ---------------------------------------------------------------------------
// Default create_batch via a delegating store
// ---------------------------------------------------------------------------

struct SequentialBatchStore {
    inner: MemoryFlows,
}

#[async_trait]
impl FlowStore for SequentialBatchStore {
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
        self.inner.heartbeat(id, owner, version).await
    }
}

#[tokio::test]
async fn default_create_batch_creates_each_state_sequentially() {
    let store = SequentialBatchStore {
        inner: MemoryFlows::default(),
    };
    assert_success(store.create(state("existing", "orders", "worker")).await);

    let created = assert_success(
        store
            .create_batch(vec![
                state("batch-1", "orders", "worker"),
                state("existing", "orders", "worker"),
                state("batch-2", "orders", "worker"),
            ])
            .await,
    );
    assert_eq!(created, vec![true, false, true]);

    // Oversized batches are rejected before any creation.
    let oversized = vec![state("x", "orders", "worker"); MAX_FLOW_STORE_BATCH + 1];
    assert_error_code(store.create_batch(oversized).await, ErrorCode::Validation);
}

// ---------------------------------------------------------------------------
// Local compensating flows
// ---------------------------------------------------------------------------

#[tokio::test]
async fn local_flow_success_runs_every_step_without_compensation() {
    let runs = Arc::new(AtomicU32::new(0));
    let compensations = Arc::new(AtomicU32::new(0));

    let mut flow = Flow::new("success");
    for _ in 0..2 {
        let run_flag = runs.clone();
        let compensate_flag = compensations.clone();
        flow = flow.step(
            move || {
                let run_flag = run_flag.clone();
                async move {
                    run_flag.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
            move || {
                let compensate_flag = compensate_flag.clone();
                async move {
                    compensate_flag.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
        );
    }
    let result = flow.run().await;

    assert!(result.is_success());
    assert_eq!(result.completed_steps(), 2);
    assert!(result.error().is_none());
    assert_eq!(runs.load(Ordering::SeqCst), 2);
    assert_eq!(compensations.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn local_flow_failure_compensates_completed_steps_in_reverse() {
    let order = Arc::new(std::sync::Mutex::new(Vec::<u32>::new()));

    let mut flow = Flow::new("rollback");
    for index in 0..2_u32 {
        let run_order = order.clone();
        let compensate_order = order.clone();
        flow = flow.step(
            move || {
                let run_order = run_order.clone();
                async move {
                    run_order.lock().expect("mutex").push(index + 1);
                    Ok(())
                }
            },
            move || {
                let compensate_order = compensate_order.clone();
                async move {
                    compensate_order.lock().expect("mutex").push(index + 101);
                    Ok(())
                }
            },
        );
    }
    let result = flow
        .step(
            || async {
                Err(CatgaError::new(
                    ErrorCode::HandlerFailed,
                    "step three fails",
                ))
            },
            || async { Ok(()) },
        )
        .run()
        .await;

    assert!(!result.is_success());
    assert_eq!(result.completed_steps(), 2);
    assert_eq!(
        result.error().expect("error").code(),
        ErrorCode::HandlerFailed
    );
    // Forward order 1,2; reverse compensation 102,101.
    assert_eq!(*order.lock().expect("mutex"), vec![1, 2, 102, 101]);
}

#[tokio::test]
async fn local_flow_restart_bounds_and_compensation_limits() {
    // An out-of-range restart fails validation without running anything.
    let ran = Arc::new(AtomicU32::new(0));
    let ran_flag = ran.clone();
    let result = Flow::new("restart")
        .step(
            move || {
                let ran_flag = ran_flag.clone();
                async move {
                    ran_flag.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
            || async { Ok(()) },
        )
        .run_from(5, usize::MAX)
        .await;
    assert_eq!(
        result.error().expect("validation failure").code(),
        ErrorCode::Validation
    );
    assert_eq!(ran.load(Ordering::SeqCst), 0);

    // run_from resumes mid-flow and skips earlier steps.
    let executed = Arc::new(AtomicU32::new(0));
    let first_flag = executed.clone();
    let second_flag = executed.clone();
    let resumed = Flow::new("resume")
        .step(
            move || {
                let first_flag = first_flag.clone();
                async move {
                    first_flag.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
            || async { Ok(()) },
        )
        .step(
            move || {
                let second_flag = second_flag.clone();
                async move {
                    second_flag.fetch_add(10, Ordering::SeqCst);
                    Ok(())
                }
            },
            || async { Ok(()) },
        )
        .run_from(1, usize::MAX)
        .await;
    assert!(resumed.is_success());
    assert_eq!(executed.load(Ordering::SeqCst), 10);

    // max_compensations bounds the undone steps; failed compensations are swallowed.
    let compensated = Arc::new(AtomicU32::new(0));
    let mut bounded = Flow::new("bounded");
    for _ in 0..2 {
        let compensated_flag = compensated.clone();
        bounded = bounded.step(
            || async { Ok(()) },
            move || {
                let compensated_flag = compensated_flag.clone();
                async move {
                    compensated_flag.fetch_add(1, Ordering::SeqCst);
                    Err(CatgaError::new(ErrorCode::Internal, "compensation failed"))
                }
            },
        );
    }
    let result = bounded
        .step(
            || async { Err(CatgaError::new(ErrorCode::HandlerFailed, "boom")) },
            || async { Ok(()) },
        )
        .run_from(0, 1)
        .await;
    assert!(!result.is_success());
    assert_eq!(compensated.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn local_flow_cancellation_compensates_only_completed_steps() {
    let token = CancellationToken::new();
    let compensated = Arc::new(AtomicU32::new(0));
    let compensated_flag = compensated.clone();
    let trip = token.clone();

    let result = Flow::new("cancellable")
        .step(
            move || {
                let trip = trip.clone();
                async move {
                    trip.cancel();
                    Ok(())
                }
            },
            move || {
                let compensated_flag = compensated_flag.clone();
                async move {
                    compensated_flag.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
        )
        .step(
            || async {
                tokio::time::sleep(Duration::from_millis(200)).await;
                Ok(())
            },
            || async { Ok(()) },
        )
        .run_until_cancelled(token)
        .await;

    assert!(!result.is_success());
    assert_eq!(result.error().expect("error").code(), ErrorCode::Cancelled);
    assert_eq!(compensated.load(Ordering::SeqCst), 1);

    // A pre-cancelled token stops before the first action runs.
    let pre = CancellationToken::new();
    pre.cancel();
    let ran = Arc::new(AtomicU32::new(0));
    let ran_flag = ran.clone();
    let early = Flow::new("early")
        .step(
            move || {
                let ran_flag = ran_flag.clone();
                async move {
                    ran_flag.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
            || async { Ok(()) },
        )
        .run_until_cancelled(pre)
        .await;
    assert_eq!(early.error().expect("error").code(), ErrorCode::Cancelled);
    assert_eq!(ran.load(Ordering::SeqCst), 0);
}

#[test]
fn flow_result_accessors_report_elapsed_and_errors() {
    let success = FlowResult::success(4);
    assert!(success.is_success());
    assert_eq!(success.completed_steps(), 4);
    assert_eq!(success.elapsed(), Duration::ZERO);

    let failure = FlowResult::failure(2, CatgaError::new(ErrorCode::Timeout, "too slow"));
    assert!(!failure.is_success());
    assert_eq!(failure.completed_steps(), 2);
    assert_eq!(failure.error().expect("error").code(), ErrorCode::Timeout);
    assert!(format!("{failure:?}").contains("FlowResult"));
    let cloned = failure.clone();
    assert_eq!(cloned.completed_steps(), 2);
}

// ---------------------------------------------------------------------------
// Continuation persistence and serde helpers
// ---------------------------------------------------------------------------

#[test]
fn continuation_frames_round_trip_and_reject_foreign_versions() {
    let continuation = FlowContinuation::new(state("flow-9", "orders", "worker-a"), "pay");
    let encoded = assert_success(encode_continuation(&continuation));
    assert_eq!(encoded[0], 7);

    let decoded = assert_success(decode_continuation(&encoded));
    assert_success(decoded.validate());

    assert_error_code(decode_continuation(&[]), ErrorCode::Internal);
    assert_error_code(decode_continuation(&[6, 1, 2, 3]), ErrorCode::Internal);
    assert_error_code(decode_continuation(&[7, 0xFF, 0xFF]), ErrorCode::Internal);
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct ArcSliceHolder {
    #[serde(
        serialize_with = "catga_core::flow::serde_helpers::serialize_arc_slice",
        deserialize_with = "catga_core::flow::serde_helpers::deserialize_arc_slice"
    )]
    items: Arc<[u64]>,
    #[serde(
        default,
        serialize_with = "catga_core::flow::serde_helpers::serialize_optional_arc_slice",
        deserialize_with = "catga_core::flow::serde_helpers::deserialize_optional_arc_slice"
    )]
    maybe: Option<Arc<[u64]>>,
}

#[test]
fn serde_helpers_round_trip_arc_slices() {
    let filled = ArcSliceHolder {
        items: Arc::from(vec![1_u64, 2, 3]),
        maybe: Some(Arc::from(vec![9_u64])),
    };
    let json = serde_json::to_string(&filled).expect("serializes");
    let back: ArcSliceHolder = serde_json::from_str(&json).expect("deserializes");
    assert_eq!(back, filled);

    let empty = ArcSliceHolder {
        items: Arc::from(Vec::<u64>::new()),
        maybe: None,
    };
    let json = serde_json::to_string(&empty).expect("serializes");
    let back: ArcSliceHolder = serde_json::from_str(&json).expect("deserializes");
    assert_eq!(back, empty);
}

#[test]
fn flow_completion_values_carry_parent_and_child_identity() {
    let success = FlowCompletion::success("corr-1", "child-1", vec![1, 2]);
    assert_eq!(success.correlation_id(), "corr-1");
    assert_eq!(success.child_id(), "child-1");
    assert!(format!("{success:?}").contains("Success"));

    let failure = FlowCompletion::failure(
        "corr-2",
        "child-2",
        CatgaError::new(ErrorCode::HandlerFailed, "child failed"),
    );
    assert_eq!(failure.correlation_id(), "corr-2");
    assert_eq!(failure.child_id(), "child-2");
    assert!(format!("{failure:?}").contains("Failure"));
}
