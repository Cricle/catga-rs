//! Gap-filling contracts for [`DslFlow`]: mediator/remote request steps,
//! lifecycle hook error propagation, in-process `for_each` variants,
//! checkpointed nested branch children, corrupted durable progress records,
//! and the terminal-record creation race.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use catga_core::flow::DslFlow;
use catga_core::flow::dsl_lifecycle::DslFlowLifecycleHooks;
use catga_core::flow::dsl_progress::{
    DslProgressKind, DslStateCodec, DslStepProgress, DslStepProgressStore,
};
use catga_core::{
    CatgaError, CatgaResult, ErrorCode, Event, Mediator, Message, MessageTypeId, Registry, Request,
    RequestClient, event_handler, request_handler,
};
use futures::{StreamExt, future::BoxFuture};

// ---------------------------------------------------------------------------
// Shared fixtures
// ---------------------------------------------------------------------------

struct U64Codec;

impl DslStateCodec<u64> for U64Codec {
    fn encode(&self, state: &u64) -> CatgaResult<Vec<u8>> {
        Ok(state.to_be_bytes().to_vec())
    }

    fn decode(&self, bytes: &[u8]) -> CatgaResult<u64> {
        let bytes: [u8; 8] = bytes
            .try_into()
            .map_err(|_| CatgaError::new(ErrorCode::Validation, "invalid u64 state payload"))?;
        Ok(u64::from_be_bytes(bytes))
    }
}

#[derive(Default)]
struct MemoryProgressStore {
    records: Mutex<HashMap<(String, u32), DslStepProgress>>,
}

#[async_trait]
impl DslStepProgressStore for MemoryProgressStore {
    async fn create(&self, progress: DslStepProgress) -> CatgaResult<bool> {
        let key = (progress.flow_id().to_owned(), progress.step_index());
        let mut records = self.records.lock().expect("progress store lock");
        if records.contains_key(&key) {
            return Ok(false);
        }
        records.insert(key, progress);
        Ok(true)
    }

    async fn update(&self, expected_version: i64, next: DslStepProgress) -> CatgaResult<bool> {
        let key = (next.flow_id().to_owned(), next.step_index());
        let mut records = self.records.lock().expect("progress store lock");
        let Some(current) = records.get(&key) else {
            return Ok(false);
        };
        if current.version() != expected_version
            || !DslStepProgress::is_next_version(expected_version, next.version())
        {
            return Ok(false);
        }
        records.insert(key, next);
        Ok(true)
    }

    async fn get(&self, flow_id: &str, step_index: u32) -> CatgaResult<Option<DslStepProgress>> {
        Ok(self
            .records
            .lock()
            .expect("progress store lock")
            .get(&(flow_id.to_owned(), step_index))
            .cloned())
    }

    async fn delete(&self, flow_id: &str, step_index: u32) -> CatgaResult<bool> {
        Ok(self
            .records
            .lock()
            .expect("progress store lock")
            .remove(&(flow_id.to_owned(), step_index))
            .is_some())
    }
}

impl MemoryProgressStore {
    fn seed(&self, progress: DslStepProgress) {
        let key = (progress.flow_id().to_owned(), progress.step_index());
        self.records
            .lock()
            .expect("progress store lock")
            .insert(key, progress);
    }
}

/// Rebuilds a progress record with an explicit kind from its serde shape,
/// reaching the `pub(crate)` CheckpointFrame/Terminal kinds from tests.
fn progress_record(
    flow_id: &str,
    step_index: u32,
    kind: &str,
    payload: Vec<u8>,
) -> DslStepProgress {
    serde_json::from_value(serde_json::json!({
        "flow_id": flow_id,
        "step_index": step_index,
        "version": 0_i64,
        "kind": kind,
        "payload": payload,
        "updated_at": { "secs_since_epoch": 0_u64, "nanos_since_epoch": 0_u32 }
    }))
    .expect("progress record decodes")
}

fn increment_action()
-> impl for<'a> Fn(&'a mut u64) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync + 'static {
    |state: &mut u64| {
        Box::pin(async move {
            *state = state.saturating_add(1);
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// Mediator and remote request steps
// ---------------------------------------------------------------------------

struct GapEchoTypeId;
impl MessageTypeId for GapEchoTypeId {
    const NAME: &'static str = "GapEcho";
}

struct GapEcho(u64);
impl Message for GapEcho {}
impl Request for GapEcho {
    type Response = u64;
    type TypeId = GapEchoTypeId;
}

struct GapTickTypeId;
impl MessageTypeId for GapTickTypeId {
    const NAME: &'static str = "GapTick";
}

#[derive(Clone)]
struct GapTick;
impl Message for GapTick {}
impl Event for GapTick {
    type TypeId = GapTickTypeId;
}

struct StubClient;

#[async_trait]
impl RequestClient<GapEcho> for StubClient {
    async fn request(&self, request: &GapEcho) -> CatgaResult<u64> {
        Ok(request.0 + 100)
    }
}

#[tokio::test]
async fn dsl_send_publish_and_remote_steps_route_typed_messages() -> CatgaResult<()> {
    let mut registry = Registry::new();
    registry.register_request::<GapEcho, _>(request_handler(|echo: GapEcho| async move {
        Ok(echo.0 * 2)
    }))?;
    let ticks = Arc::new(AtomicUsize::new(0));
    let tick_marker = Arc::clone(&ticks);
    registry.register_event::<GapTick, _>(event_handler(move |_: GapTick| {
        let ticks = Arc::clone(&tick_marker);
        async move {
            ticks.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }));
    let mediator = Arc::new(Mediator::new(registry));
    let client = Arc::new(StubClient);

    let flow = DslFlow::new()
        .send(Arc::clone(&mediator), |state: &u64| GapEcho(*state))
        .send_into(
            Arc::clone(&mediator),
            |state: &u64| GapEcho(*state),
            |state: &mut u64, response| *state = response,
        )
        .publish(Arc::clone(&mediator), |_state: &u64| GapTick)
        .remote_send(Arc::clone(&client), |state: &u64| GapEcho(*state))
        .remote_send_into(
            client,
            |state: &u64| GapEcho(*state),
            |state: &mut u64, response| *state = state.saturating_add(response),
        );

    let mut state = 21_u64;
    flow.run(&mut state).await?;
    assert_eq!(
        state, 184,
        "send_into stores the doubled echo and remote_send_into adds the stubbed reply"
    );
    assert_eq!(
        ticks.load(Ordering::SeqCst),
        1,
        "publish reached its event handler"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Lifecycle hook error propagation
// ---------------------------------------------------------------------------

fn failing_action()
-> impl for<'a> Fn(&'a mut u64) -> BoxFuture<'a, CatgaResult<()>> + Send + Sync + 'static {
    |_state: &mut u64| {
        Box::pin(async { Err(CatgaError::new(ErrorCode::Internal, "step exploded")) })
    }
}

#[tokio::test]
async fn dsl_lifecycle_hook_errors_abort_execution() {
    let hook_error = CatgaError::new(ErrorCode::HandlerFailed, "hook rejected");

    // A failing step_failed hook replaces the step error.
    let flow = DslFlow::new()
        .action(failing_action())
        .with_lifecycle_hooks(DslFlowLifecycleHooks::new().on_step_failed({
            let hook_error = hook_error.clone();
            move |_state: &u64, _index, _error| {
                let hook_error = hook_error.clone();
                Box::pin(async move { Err(hook_error) })
            }
        }));
    let mut state = 0_u64;
    let error = flow.run(&mut state).await.expect_err("the hook error wins");
    assert_eq!(error.code(), ErrorCode::HandlerFailed);

    // A failing flow_failed hook replaces the step error after step hooks pass.
    let flow = DslFlow::new()
        .action(failing_action())
        .with_lifecycle_hooks(DslFlowLifecycleHooks::new().on_flow_failed({
            let hook_error = hook_error.clone();
            move |_state: &u64, _error| {
                let hook_error = hook_error.clone();
                Box::pin(async move { Err(hook_error) })
            }
        }));
    let mut state = 0_u64;
    let error = flow.run(&mut state).await.expect_err("the hook error wins");
    assert_eq!(error.code(), ErrorCode::HandlerFailed);

    // A failing flow_succeeded hook turns a successful run into an error.
    let flow = DslFlow::new()
        .action(increment_action())
        .with_lifecycle_hooks(DslFlowLifecycleHooks::new().on_flow_succeeded({
            let hook_error = hook_error.clone();
            move |_state: &u64| {
                let hook_error = hook_error.clone();
                Box::pin(async move { Err(hook_error) })
            }
        }));
    let mut state = 0_u64;
    let error = flow.run(&mut state).await.expect_err("the hook error wins");
    assert_eq!(error.code(), ErrorCode::HandlerFailed);
}

#[tokio::test]
async fn dsl_lifecycle_hooks_observe_success_and_failure_paths() -> CatgaResult<()> {
    let calls = Arc::new(Mutex::new(Vec::<String>::new()));

    let ok_calls = Arc::clone(&calls);
    let flow = DslFlow::new()
        .action(increment_action())
        .with_lifecycle_hooks(DslFlowLifecycleHooks::new().on_flow_succeeded(
            move |_state: &u64| {
                let calls = Arc::clone(&ok_calls);
                Box::pin(async move {
                    calls.lock().expect("hook lock").push("flow-ok".into());
                    Ok(())
                })
            },
        ));
    let mut state = 0_u64;
    flow.run(&mut state).await?;

    let err_calls = Arc::clone(&calls);
    let flow = DslFlow::new()
        .action(failing_action())
        .with_lifecycle_hooks(DslFlowLifecycleHooks::new().on_step_failed(
            move |_state: &u64, index, _error| {
                let calls = Arc::clone(&err_calls);
                Box::pin(async move {
                    calls
                        .lock()
                        .expect("hook lock")
                        .push(format!("step-err:{index}"));
                    Ok(())
                })
            },
        ));
    let mut state = 0_u64;
    assert!(flow.run(&mut state).await.is_err());

    assert_eq!(
        calls.lock().expect("hook lock").as_slice(),
        &["flow-ok".to_string(), "step-err:0".to_string()],
        "successful flow and failed step hooks both run"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// In-process for_each variants
// ---------------------------------------------------------------------------

#[derive(Default)]
struct SumState {
    items: Vec<u64>,
    sum: u64,
}

#[tokio::test]
async fn dsl_for_each_variants_run_sequential_stream_and_batched_work() -> CatgaResult<()> {
    let flow = DslFlow::new()
        .for_each(
            |state: &SumState| state.items.clone(),
            |state, item| {
                Box::pin(async move {
                    state.sum += item;
                    Ok(())
                })
            },
        )
        .for_each_stream(
            |state: &SumState| futures::stream::iter(state.items.clone()).boxed(),
            |state, item| {
                Box::pin(async move {
                    state.sum += item;
                    Ok(())
                })
            },
        )
        .for_each_stream_concurrent(
            2,
            |state: &SumState| futures::stream::iter(state.items.clone()).boxed(),
            |_state: &SumState, item| Box::pin(async move { Ok(item * 10) }),
            |state, result| {
                state.sum += result;
                Ok(())
            },
        )
        .expect("a positive concurrency limit is valid");

    let mut state = SumState {
        items: vec![1, 2, 3],
        sum: 0,
    };
    flow.run(&mut state).await?;
    assert_eq!(
        state.sum,
        6 + 6 + 60,
        "sequential, stream, and batched passes each add their share"
    );

    match DslFlow::<SumState>::new().for_each_stream_concurrent(
        0,
        |_state: &SumState| futures::stream::empty::<u64>().boxed(),
        |_state: &SumState, item: u64| Box::pin(async move { Ok(item) }),
        |_state: &mut SumState, _result: u64| Ok(()),
    ) {
        Err(error) => assert_eq!(error.code(), ErrorCode::Validation),
        Ok(_) => panic!("a zero concurrency limit must be rejected"),
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Replayable for_each under plain run()
// ---------------------------------------------------------------------------

#[derive(Default)]
struct ReplayState {
    items: Vec<u32>,
    processed: Vec<u32>,
    skipped: Vec<u32>,
}

#[tokio::test]
async fn dsl_replayable_for_each_runs_directly_with_and_without_error_callbacks() -> CatgaResult<()>
{
    let flow = DslFlow::new().for_each_replayable(
        |state: &ReplayState| state.items.clone(),
        |state, item| {
            Box::pin(async move {
                state.processed.push(item);
                Ok(())
            })
        },
    );
    let mut state = ReplayState {
        items: vec![1, 2],
        ..ReplayState::default()
    };
    flow.run(&mut state).await?;
    assert_eq!(state.processed, vec![1, 2]);

    let flow = DslFlow::new().for_each_replayable(
        |state: &ReplayState| state.items.clone(),
        |_state, item| {
            Box::pin(async move {
                if item == 13 {
                    return Err(CatgaError::new(ErrorCode::Internal, "unlucky item"));
                }
                Ok(())
            })
        },
    );
    let mut state = ReplayState {
        items: vec![1, 13],
        ..ReplayState::default()
    };
    let error = flow
        .run(&mut state)
        .await
        .expect_err("an item failure stops the run");
    assert_eq!(error.code(), ErrorCode::Internal);

    let flow = DslFlow::new().for_each_replayable_continue_on_error(
        |state: &ReplayState| state.items.clone(),
        |state, item| {
            Box::pin(async move {
                if item == 13 {
                    return Err(CatgaError::new(ErrorCode::Internal, "unlucky item"));
                }
                state.processed.push(item);
                Ok(())
            })
        },
        |state, _index, _error| {
            Box::pin(async move {
                state.skipped.push(13);
                Ok(())
            })
        },
    );
    let mut state = ReplayState {
        items: vec![1, 13, 2],
        ..ReplayState::default()
    };
    flow.run(&mut state).await?;
    assert_eq!(state.processed, vec![1, 2]);
    assert_eq!(state.skipped, vec![13]);
    Ok(())
}

#[tokio::test]
async fn dsl_retry_sleeps_between_transient_attempts() -> CatgaResult<()> {
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempt_marker = Arc::clone(&attempts);
    let flow = DslFlow::new().retry(3, Duration::from_millis(5), move |state: &mut u64| {
        let attempts = Arc::clone(&attempt_marker);
        Box::pin(async move {
            if attempts.fetch_add(1, Ordering::SeqCst) < 2 {
                return Err(CatgaError::new(ErrorCode::Transient, "still busy"));
            }
            *state = 42;
            Ok(())
        })
    });
    let mut state = 0_u64;
    flow.run(&mut state).await?;
    assert_eq!(state, 42);
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    Ok(())
}

#[test]
fn dsl_flow_default_builds_an_empty_flow() {
    let _flow = DslFlow::<u64>::default();
}

// ---------------------------------------------------------------------------
// Checkpointed nested branch children
// ---------------------------------------------------------------------------

#[tokio::test]
async fn run_checkpointed_executes_nested_branch_children() -> CatgaResult<()> {
    let branch = DslFlow::new()
        .action(increment_action())
        .for_each_replayable(
            |state: &u64| vec![*state, state.saturating_add(1)],
            |state, item| {
                Box::pin(async move {
                    *state = state.saturating_add(item);
                    Ok(())
                })
            },
        )
        .parallel(
            [
                DslFlow::new().action(|state: &mut u64| {
                    Box::pin(async move {
                        *state = state.saturating_add(10);
                        Ok(())
                    })
                }),
                DslFlow::new().action(|state: &mut u64| {
                    Box::pin(async move {
                        *state = state.saturating_add(100);
                        Ok(())
                    })
                }),
            ],
            |state, branch_states| {
                *state = branch_states.iter().sum();
                Ok(())
            },
        )
        .when_any(
            [
                DslFlow::new().action(|_state: &mut u64| {
                    Box::pin(async { Err(CatgaError::new(ErrorCode::Internal, "losing branch")) })
                }),
                DslFlow::new().action(|state: &mut u64| {
                    Box::pin(async move {
                        *state = state.saturating_add(1_000);
                        Ok(())
                    })
                }),
            ],
            |state, winner| {
                *state = winner;
                Ok(())
            },
        )
        .if_else(
            |state: &u64| *state > 0,
            DslFlow::new().action(|state: &mut u64| {
                Box::pin(async move {
                    *state = state.saturating_add(5);
                    Ok(())
                })
            }),
            DslFlow::new(),
        );
    let flow = DslFlow::new().if_else(|state: &u64| *state > 0, branch, DslFlow::new());

    let progress = MemoryProgressStore::default();
    let codec = U64Codec;
    let final_state = flow
        .run_checkpointed("nested-children", 1, &progress, &codec)
        .await?;
    assert_eq!(
        final_state, 1_129,
        "action, replayable items, parallel merge, when_any winner, and nested if all run"
    );
    Ok(())
}

#[tokio::test]
async fn run_checkpointed_rejects_nested_children_without_replay_cursors() -> CatgaResult<()> {
    let progress = MemoryProgressStore::default();
    let codec = U64Codec;

    let generic_for_each = DslFlow::new().if_else(
        |state: &u64| *state > 0,
        DslFlow::new().for_each(
            |state: &u64| vec![*state],
            |_state, _item| Box::pin(async { Ok(()) }),
        ),
        DslFlow::new(),
    );
    let error = generic_for_each
        .run_checkpointed("nested-generic", 1, &progress, &codec)
        .await
        .expect_err("a generic for_each has no replay cursor");
    assert_eq!(error.code(), ErrorCode::Validation);

    let stream_for_each = DslFlow::new().if_else(
        |state: &u64| *state > 0,
        DslFlow::new().for_each_stream(
            |state: &u64| futures::stream::iter(vec![*state]).boxed(),
            |_state, _item| Box::pin(async { Ok(()) }),
        ),
        DslFlow::new(),
    );
    let error = stream_for_each
        .run_checkpointed("nested-stream", 1, &progress, &codec)
        .await
        .expect_err("a stream for_each has no replay cursor");
    assert_eq!(error.code(), ErrorCode::Validation);
    Ok(())
}

// ---------------------------------------------------------------------------
// Corrupted durable progress records
// ---------------------------------------------------------------------------

#[tokio::test]
async fn run_checkpointed_rejects_corrupt_frame_and_terminal_records() {
    let progress = MemoryProgressStore::default();
    progress.seed(progress_record(
        "corrupt-frame",
        0,
        "CheckpointFrame",
        vec![1, 2, 3],
    ));
    let flow = DslFlow::new().action(increment_action());
    let error = flow
        .run_checkpointed("corrupt-frame", 0, &progress, &U64Codec)
        .await
        .expect_err("a frame record without a valid frame payload is rejected");
    assert_eq!(error.code(), ErrorCode::Validation);

    let progress = MemoryProgressStore::default();
    progress.seed(progress_record(
        "corrupt-terminal",
        u32::MAX,
        "Terminal",
        vec![9, 9, 9],
    ));
    let error = flow
        .run_checkpointed("corrupt-terminal", 0, &progress, &U64Codec)
        .await
        .expect_err("an undecodable terminal record is rejected");
    assert_eq!(error.code(), ErrorCode::Validation);
}

/// Progress store that hides the terminal slot on the first probe and then
/// reports a lost creation race, reproducing a concurrent terminal writer.
#[derive(Default)]
struct RacingTerminalStore {
    records: Mutex<HashMap<(String, u32), DslStepProgress>>,
    probed: AtomicBool,
}

#[async_trait]
impl DslStepProgressStore for RacingTerminalStore {
    async fn create(&self, progress: DslStepProgress) -> CatgaResult<bool> {
        let key = (progress.flow_id().to_owned(), progress.step_index());
        let is_terminal = progress.step_index() == u32::MAX;
        self.records
            .lock()
            .expect("progress store lock")
            .insert(key, progress);
        Ok(!is_terminal)
    }

    async fn update(&self, expected_version: i64, next: DslStepProgress) -> CatgaResult<bool> {
        let key = (next.flow_id().to_owned(), next.step_index());
        let mut records = self.records.lock().expect("progress store lock");
        let Some(current) = records.get(&key) else {
            return Ok(false);
        };
        if current.version() != expected_version
            || !DslStepProgress::is_next_version(expected_version, next.version())
        {
            return Ok(false);
        }
        records.insert(key, next);
        Ok(true)
    }

    async fn get(&self, flow_id: &str, step_index: u32) -> CatgaResult<Option<DslStepProgress>> {
        if step_index == u32::MAX && !self.probed.swap(true, Ordering::SeqCst) {
            return Ok(None);
        }
        Ok(self
            .records
            .lock()
            .expect("progress store lock")
            .get(&(flow_id.to_owned(), step_index))
            .cloned())
    }

    async fn delete(&self, flow_id: &str, step_index: u32) -> CatgaResult<bool> {
        Ok(self
            .records
            .lock()
            .expect("progress store lock")
            .remove(&(flow_id.to_owned(), step_index))
            .is_some())
    }
}

#[tokio::test]
async fn run_checkpointed_reports_the_terminal_winner_when_creation_races() -> CatgaResult<()> {
    let progress = RacingTerminalStore::default();
    let flow = DslFlow::new().action(|state: &mut u64| {
        Box::pin(async move {
            *state = 99;
            Ok(())
        })
    });
    let final_state = flow
        .run_checkpointed("racing-terminal", 0, &progress, &U64Codec)
        .await?;
    assert_eq!(
        final_state, 99,
        "a lost terminal creation race still returns the durable terminal state"
    );
    Ok(())
}

#[tokio::test]
async fn run_checkpointed_restores_application_state_from_saved_progress() -> CatgaResult<()> {
    let progress = MemoryProgressStore::default();
    progress.seed(progress_record(
        "resume-state",
        0,
        "ApplicationState",
        41_u64.to_be_bytes().to_vec(),
    ));
    let flow = DslFlow::new().action(increment_action());
    let final_state = flow
        .run_checkpointed("resume-state", 0, &progress, &U64Codec)
        .await?;
    assert_eq!(
        final_state, 41,
        "recovery resumes after the saved step with the saved state"
    );

    let kind = progress
        .get("resume-state", u32::MAX)
        .await?
        .expect("the terminal record exists");
    assert_eq!(kind.kind(), DslProgressKind::Terminal);
    Ok(())
}
