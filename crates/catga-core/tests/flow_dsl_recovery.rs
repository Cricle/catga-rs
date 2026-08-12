//! Strict recovery contracts for [`DslFlow::run_checkpointed`].
//!
//! Exercises durable step progress, terminal records, nested-branch and
//! parallel-branch cursors, replayable `for_each` cursors, and the conflict /
//! validation paths of the checkpoint persistence layer.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use catga_core::flow::DslFlow;
use catga_core::flow::dsl_lifecycle::{DslFlowLifecycleEvent, DslFlowLifecycleObserver};
use catga_core::flow::dsl_progress::{
    DslProgressKind, DslStateCodec, DslStepProgress, DslStepProgressStore,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};

const TERMINAL_STEP_INDEX: u32 = u32::MAX;

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
    fn record(&self, flow_id: &str, step_index: u32) -> Option<DslStepProgress> {
        self.records
            .lock()
            .expect("progress store lock")
            .get(&(flow_id.to_owned(), step_index))
            .cloned()
    }
}

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
struct CountingObserver {
    events: Mutex<Vec<String>>,
}

impl DslFlowLifecycleObserver for CountingObserver {
    fn observe(&self, event: &DslFlowLifecycleEvent) {
        let name = match event {
            DslFlowLifecycleEvent::StepSucceeded { step_index } => {
                format!("step-ok:{step_index}")
            }
            DslFlowLifecycleEvent::StepFailed { step_index, .. } => {
                format!("step-err:{step_index}")
            }
            DslFlowLifecycleEvent::FlowSucceeded => "flow-ok".to_string(),
            DslFlowLifecycleEvent::FlowFailed { .. } => "flow-err".to_string(),
        };
        self.events.lock().expect("observer lock").push(name);
    }
}

impl CountingObserver {
    fn count(&self) -> usize {
        self.events.lock().expect("observer lock").len()
    }
}

#[tokio::test]
async fn checkpointed_run_persists_progress_and_a_terminal_record() -> CatgaResult<()> {
    let store = MemoryProgressStore::default();
    let codec = U64Codec;
    let flow = DslFlow::new()
        .action(|state: &mut u64| {
            Box::pin(async move {
                *state += 1;
                Ok(())
            })
        })
        .action(|state: &mut u64| {
            Box::pin(async move {
                *state += 10;
                Ok(())
            })
        });

    let result = flow
        .run_checkpointed("flow-basic", 0_u64, &store, &codec)
        .await?;
    assert_eq!(result, 11);
    assert_eq!(
        store
            .record("flow-basic", 0)
            .expect("step zero progress")
            .kind(),
        DslProgressKind::ApplicationState
    );
    assert_eq!(
        store
            .record("flow-basic", TERMINAL_STEP_INDEX)
            .expect("a terminal record is written")
            .kind(),
        DslProgressKind::Terminal
    );
    Ok(())
}

#[tokio::test]
async fn checkpointed_rerun_returns_terminal_state_without_replaying_steps() -> CatgaResult<()> {
    let store = MemoryProgressStore::default();
    let codec = U64Codec;
    let attempts = Arc::new(AtomicUsize::new(0));
    let observer = Arc::new(CountingObserver::default());

    let step_attempts = Arc::clone(&attempts);
    let flow = DslFlow::new()
        .with_lifecycle_observer(observer.clone())
        .action(move |state: &mut u64| {
            let attempts = Arc::clone(&step_attempts);
            Box::pin(async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                *state += 1;
                Ok(())
            })
        });

    let first = flow
        .run_checkpointed("flow-done", 0_u64, &store, &codec)
        .await?;
    assert_eq!(first, 1);
    assert_eq!(observer.count(), 2, "one step success and one flow success");

    let second = flow
        .run_checkpointed("flow-done", 999_u64, &store, &codec)
        .await?;
    assert_eq!(second, 1, "the terminal state is restored, not recomputed");
    assert_eq!(attempts.load(Ordering::SeqCst), 1, "steps are not replayed");
    assert_eq!(observer.count(), 2, "lifecycle events are not re-emitted");
    Ok(())
}

#[tokio::test]
async fn checkpointed_run_resumes_after_a_mid_flow_failure() -> CatgaResult<()> {
    let store = MemoryProgressStore::default();
    let codec = U64Codec;
    let attempts: Vec<Arc<AtomicUsize>> = (0..3).map(|_| Arc::new(AtomicUsize::new(0))).collect();

    let first_attempts = Arc::clone(&attempts[0]);
    let second_attempts = Arc::clone(&attempts[1]);
    let third_attempts = Arc::clone(&attempts[2]);
    let flaky_attempts = Arc::clone(&attempts[1]);
    let flow = DslFlow::new()
        .action(move |state: &mut u64| {
            let attempts = Arc::clone(&first_attempts);
            Box::pin(async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                *state += 1;
                Ok(())
            })
        })
        .action(move |state: &mut u64| {
            let attempts = Arc::clone(&second_attempts);
            let flaky = Arc::clone(&flaky_attempts);
            Box::pin(async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                if flaky.load(Ordering::SeqCst) == 1 {
                    return Err(CatgaError::new(ErrorCode::Transient, "second step flaky"));
                }
                *state += 2;
                Ok(())
            })
        })
        .action(move |state: &mut u64| {
            let attempts = Arc::clone(&third_attempts);
            Box::pin(async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                *state *= 10;
                Ok(())
            })
        });

    let error = flow
        .run_checkpointed("flow-resume", 0_u64, &store, &codec)
        .await
        .expect_err("the first run stops at the flaky step");
    assert_eq!(error.code(), ErrorCode::Transient);
    assert!(
        store.record("flow-resume", TERMINAL_STEP_INDEX).is_none(),
        "a failed run never writes a terminal record"
    );

    let recovered = flow
        .run_checkpointed("flow-resume", 0_u64, &store, &codec)
        .await?;
    assert_eq!(
        recovered, 30,
        "((0 + 1) + 2) * 10 resumes from the checkpoint"
    );
    assert_eq!(
        attempts[0].load(Ordering::SeqCst),
        1,
        "step zero is not replayed"
    );
    assert_eq!(
        attempts[1].load(Ordering::SeqCst),
        2,
        "the flaky step is retried"
    );
    assert_eq!(
        attempts[2].load(Ordering::SeqCst),
        1,
        "the last step runs once"
    );
    Ok(())
}

#[tokio::test]
async fn checkpointed_run_rejects_loops_without_a_replay_cursor() -> CatgaResult<()> {
    use futures::StreamExt;

    let store = MemoryProgressStore::default();
    let codec = U64Codec;

    let collected = DslFlow::new().for_each(
        |_state: &u64| vec![1_u32, 2],
        |state: &mut u64, item: u32| {
            Box::pin(async move {
                *state += u64::from(item);
                Ok(())
            })
        },
    );
    let error = collected
        .run_checkpointed("flow-for-each", 0_u64, &store, &codec)
        .await
        .expect_err("generic for_each has no replay cursor");
    assert_eq!(error.code(), ErrorCode::Validation);

    let streamed = DslFlow::new().for_each_stream(
        |_state: &u64| futures::stream::iter(vec![1_u32]).boxed(),
        |state: &mut u64, item: u32| {
            Box::pin(async move {
                *state += u64::from(item);
                Ok(())
            })
        },
    );
    let error = streamed
        .run_checkpointed("flow-stream", 0_u64, &store, &codec)
        .await
        .expect_err("streamed for_each has no replay cursor");
    assert_eq!(error.code(), ErrorCode::Validation);

    let concurrent = DslFlow::new()
        .for_each_stream_concurrent(
            2,
            |_state: &u64| futures::stream::iter(vec![1_u32]).boxed(),
            |_state: &u64, item: u32| Box::pin(async move { Ok(item) }),
            |state: &mut u64, item: u32| {
                *state += u64::from(item);
                Ok(())
            },
        )
        .expect("a positive limit is valid");
    let error = concurrent
        .run_checkpointed("flow-concurrent", 0_u64, &store, &codec)
        .await
        .expect_err("concurrent streamed for_each has no replay cursor");
    assert_eq!(error.code(), ErrorCode::Validation);
    Ok(())
}

#[tokio::test]
async fn checkpointed_for_each_replayable_resumes_from_the_saved_cursor() -> CatgaResult<()> {
    let store = MemoryProgressStore::default();
    let codec = U64Codec;
    let attempts = Arc::new(Mutex::new(vec![0_usize; 3]));

    let item_attempts = Arc::clone(&attempts);
    let flow = DslFlow::new().for_each_replayable(
        |_state: &u64| vec![1_u32, 2, 3],
        move |state: &mut u64, item: u32| {
            let attempts = Arc::clone(&item_attempts);
            Box::pin(async move {
                let index = usize::try_from(item - 1).expect("small items");
                let attempt = {
                    let mut guard = attempts.lock().expect("attempts lock");
                    let attempt = guard[index];
                    guard[index] += 1;
                    attempt
                };
                if item == 2 && attempt == 0 {
                    return Err(CatgaError::new(ErrorCode::Transient, "item two flaky"));
                }
                *state += u64::from(item);
                Ok(())
            })
        },
    );

    let error = flow
        .run_checkpointed("flow-items", 0_u64, &store, &codec)
        .await
        .expect_err("the first run stops at the flaky item");
    assert_eq!(error.code(), ErrorCode::Transient);

    let recovered = flow
        .run_checkpointed("flow-items", 0_u64, &store, &codec)
        .await?;
    assert_eq!(
        recovered, 6,
        "1 + 2 + 3 with the cursor resumed at item two"
    );
    let attempts = attempts.lock().expect("attempts lock");
    assert_eq!(
        attempts.as_slice(),
        &[1, 2, 1],
        "the original selection is replayed from the failed item only"
    );
    Ok(())
}

#[tokio::test]
async fn checkpointed_for_each_replayable_continue_on_error_persists_progress() -> CatgaResult<()> {
    let store = MemoryProgressStore::default();
    let codec = U64Codec;
    let handled = Arc::new(Mutex::new(Vec::new()));

    let handled_indexes = Arc::clone(&handled);
    let flow = DslFlow::new().for_each_replayable_continue_on_error(
        |_state: &u64| vec![1_u32, 2, 3],
        |state: &mut u64, item: u32| {
            Box::pin(async move {
                if item == 2 {
                    return Err(CatgaError::new(ErrorCode::Internal, "item two rejected"));
                }
                *state += u64::from(item);
                Ok(())
            })
        },
        move |state: &mut u64, index: usize, error: CatgaError| {
            let handled = Arc::clone(&handled_indexes);
            Box::pin(async move {
                assert_eq!(error.message(), "item two rejected");
                handled.lock().expect("handled lock").push(index);
                *state += 1_000;
                Ok(())
            })
        },
    );

    let result = flow
        .run_checkpointed("flow-tolerant", 0_u64, &store, &codec)
        .await?;
    assert_eq!(result, 1_004, "1 + 3 plus the handled failure marker");
    assert_eq!(
        handled.lock().expect("handled lock").as_slice(),
        &[1],
        "the error handler observed the failing item index"
    );
    Ok(())
}

#[tokio::test]
async fn checkpointed_parallel_recovers_completed_branches_without_replay() -> CatgaResult<()> {
    let store = MemoryProgressStore::default();
    let codec = U64Codec;
    let fast_attempts = Arc::new(AtomicUsize::new(0));
    let flaky_attempts = Arc::new(AtomicUsize::new(0));

    let fast_marker = Arc::clone(&fast_attempts);
    let flaky_marker = Arc::clone(&flaky_attempts);
    let flaky_gate = Arc::clone(&flaky_attempts);
    let flow = DslFlow::new().parallel(
        [
            DslFlow::new().action(move |state: &mut u64| {
                let attempts = Arc::clone(&fast_marker);
                Box::pin(async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    *state += 1;
                    Ok(())
                })
            }),
            DslFlow::new().action(move |state: &mut u64| {
                let attempts = Arc::clone(&flaky_marker);
                let gate = Arc::clone(&flaky_gate);
                Box::pin(async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    if gate.load(Ordering::SeqCst) == 1 {
                        return Err(CatgaError::new(ErrorCode::Transient, "branch flaky"));
                    }
                    *state += 10;
                    Ok(())
                })
            }),
        ],
        |state: &mut u64, results: Vec<u64>| {
            *state = results.into_iter().sum();
            Ok(())
        },
    );

    let error = flow
        .run_checkpointed("flow-fanout", 0_u64, &store, &codec)
        .await
        .expect_err("the first run fails in the flaky branch");
    assert_eq!(error.code(), ErrorCode::Transient);

    let recovered = flow
        .run_checkpointed("flow-fanout", 0_u64, &store, &codec)
        .await?;
    assert_eq!(recovered, 11, "both branch states merge after recovery");
    assert_eq!(
        fast_attempts.load(Ordering::SeqCst),
        1,
        "the completed branch is restored from its checkpoint"
    );
    assert_eq!(
        flaky_attempts.load(Ordering::SeqCst),
        2,
        "only the failed branch is retried"
    );
    Ok(())
}

#[tokio::test]
async fn checkpointed_if_else_resumes_inside_the_selected_branch() -> CatgaResult<()> {
    let store = MemoryProgressStore::default();
    let codec = U64Codec;
    let first_attempts = Arc::new(AtomicUsize::new(0));
    let second_attempts = Arc::new(AtomicUsize::new(0));

    let first_marker = Arc::clone(&first_attempts);
    let second_marker = Arc::clone(&second_attempts);
    let second_gate = Arc::clone(&second_attempts);
    let flow = DslFlow::new().if_else(
        |_state: &u64| true,
        DslFlow::new()
            .action(move |state: &mut u64| {
                let attempts = Arc::clone(&first_marker);
                Box::pin(async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    *state += 1;
                    Ok(())
                })
            })
            .action(move |state: &mut u64| {
                let attempts = Arc::clone(&second_marker);
                let gate = Arc::clone(&second_gate);
                Box::pin(async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    if gate.load(Ordering::SeqCst) == 1 {
                        return Err(CatgaError::new(ErrorCode::Transient, "nested step flaky"));
                    }
                    *state += 2;
                    Ok(())
                })
            }),
        DslFlow::new().action(|state: &mut u64| {
            Box::pin(async move {
                *state += 100;
                Ok(())
            })
        }),
    );

    let error = flow
        .run_checkpointed("flow-branch", 0_u64, &store, &codec)
        .await
        .expect_err("the first run stops inside the then branch");
    assert_eq!(error.code(), ErrorCode::Transient);

    let recovered = flow
        .run_checkpointed("flow-branch", 0_u64, &store, &codec)
        .await?;
    assert_eq!(
        recovered, 3,
        "the saved branch and step cursor drive the resume"
    );
    assert_eq!(
        first_attempts.load(Ordering::SeqCst),
        1,
        "the completed nested step is not replayed"
    );
    assert_eq!(second_attempts.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn checkpointed_when_any_persists_the_winner_and_completes() -> CatgaResult<()> {
    let store = MemoryProgressStore::default();
    let codec = U64Codec;
    let flow = DslFlow::new().when_any(
        [
            DslFlow::new().action(|_state: &mut u64| {
                Box::pin(async move { Err(CatgaError::new(ErrorCode::Transient, "loser")) })
            }),
            DslFlow::new().action(|state: &mut u64| {
                Box::pin(async move {
                    *state = 5;
                    Ok(())
                })
            }),
        ],
        |state: &mut u64, winner: u64| {
            *state = winner;
            Ok(())
        },
    );

    let result = flow
        .run_checkpointed("flow-race", 0_u64, &store, &codec)
        .await?;
    assert_eq!(result, 5, "the winning branch state is merged");
    Ok(())
}

#[tokio::test]
async fn checkpointed_run_rejects_a_non_terminal_record_in_the_terminal_slot() -> CatgaResult<()> {
    let store = MemoryProgressStore::default();
    let codec = U64Codec;
    let bogus = DslStepProgress::new("flow-hijacked", TERMINAL_STEP_INDEX, []);
    assert_eq!(bogus.kind(), DslProgressKind::ApplicationState);
    assert!(store.create(bogus).await?, "the bogus record is stored");

    let flow = DslFlow::new().action(|_state: &mut u64| Box::pin(async move { Ok(()) }));
    let error = flow
        .run_checkpointed("flow-hijacked", 0_u64, &store, &codec)
        .await
        .expect_err("an application record in the terminal slot is a conflict");
    assert_eq!(error.code(), ErrorCode::Conflict);
    Ok(())
}

/// A store whose `create` persists but reports a conflict and whose `update`
/// always loses the CAS race, exhausting the bounded retry loop.
#[derive(Default)]
struct ConflictingProgressStore {
    records: Mutex<HashMap<(String, u32), DslStepProgress>>,
}

#[async_trait]
impl DslStepProgressStore for ConflictingProgressStore {
    async fn create(&self, progress: DslStepProgress) -> CatgaResult<bool> {
        let key = (progress.flow_id().to_owned(), progress.step_index());
        self.records
            .lock()
            .expect("progress store lock")
            .entry(key)
            .or_insert(progress);
        Ok(false)
    }

    async fn update(&self, _expected_version: i64, _next: DslStepProgress) -> CatgaResult<bool> {
        Ok(false)
    }

    async fn get(&self, flow_id: &str, step_index: u32) -> CatgaResult<Option<DslStepProgress>> {
        Ok(self
            .records
            .lock()
            .expect("progress store lock")
            .get(&(flow_id.to_owned(), step_index))
            .cloned())
    }

    async fn delete(&self, _flow_id: &str, _step_index: u32) -> CatgaResult<bool> {
        Ok(false)
    }
}

#[tokio::test]
async fn checkpointed_run_fails_after_bounded_cursor_cas_retries() -> CatgaResult<()> {
    let store = ConflictingProgressStore::default();
    let codec = U64Codec;
    let flow = DslFlow::new().action(|state: &mut u64| {
        Box::pin(async move {
            *state += 1;
            Ok(())
        })
    });

    let error = flow
        .run_checkpointed("flow-conflict", 0_u64, &store, &codec)
        .await
        .expect_err("a cursor that can never be persisted must fail");
    assert_eq!(error.code(), ErrorCode::Conflict);
    assert_eq!(
        error.message(),
        "DSL checkpoint cursor update conflicted after bounded retries"
    );
    Ok(())
}

struct FailingProgressStore;

#[async_trait]
impl DslStepProgressStore for FailingProgressStore {
    async fn create(&self, _progress: DslStepProgress) -> CatgaResult<bool> {
        Err(CatgaError::new(
            ErrorCode::PersistenceFailed,
            "store is down",
        ))
    }

    async fn update(&self, _expected_version: i64, _next: DslStepProgress) -> CatgaResult<bool> {
        Err(CatgaError::new(
            ErrorCode::PersistenceFailed,
            "store is down",
        ))
    }

    async fn get(&self, _flow_id: &str, _step_index: u32) -> CatgaResult<Option<DslStepProgress>> {
        Err(CatgaError::new(
            ErrorCode::PersistenceFailed,
            "store is down",
        ))
    }

    async fn delete(&self, _flow_id: &str, _step_index: u32) -> CatgaResult<bool> {
        Err(CatgaError::new(
            ErrorCode::PersistenceFailed,
            "store is down",
        ))
    }
}

#[tokio::test]
async fn checkpointed_run_propagates_progress_store_failures() -> CatgaResult<()> {
    let store = FailingProgressStore;
    let codec = U64Codec;
    let flow = DslFlow::new().action(|_state: &mut u64| Box::pin(async move { Ok(()) }));

    let error = flow
        .run_checkpointed("flow-down", 0_u64, &store, &codec)
        .await
        .expect_err("store failures surface unchanged");
    assert_eq!(error.code(), ErrorCode::PersistenceFailed);
    assert_eq!(error.message(), "store is down");
    Ok(())
}

struct OversizedCodec;

impl DslStateCodec<u64> for OversizedCodec {
    fn encode(&self, _state: &u64) -> CatgaResult<Vec<u8>> {
        Ok(vec![0_u8; 1024 * 1024 + 1])
    }

    fn decode(&self, _bytes: &[u8]) -> CatgaResult<u64> {
        Ok(0)
    }
}

#[tokio::test]
async fn checkpointed_terminal_record_enforces_the_size_limit() -> CatgaResult<()> {
    let store = MemoryProgressStore::default();
    let codec = OversizedCodec;
    let flow = DslFlow::new().action(|_state: &mut u64| Box::pin(async move { Ok(()) }));

    let error = flow
        .run_checkpointed("flow-huge", 0_u64, &store, &codec)
        .await
        .expect_err("an oversized terminal record is rejected");
    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(
        error.message(),
        "DSL terminal record exceeds the size limit"
    );
    Ok(())
}

#[tokio::test]
async fn checkpointed_empty_flow_completes_and_is_idempotent() -> CatgaResult<()> {
    let store = MemoryProgressStore::default();
    let codec = U64Codec;
    let flow = DslFlow::<u64>::new();

    let first = flow
        .run_checkpointed("flow-empty", 7_u64, &store, &codec)
        .await?;
    assert_eq!(first, 7, "an empty flow returns its initial state");
    let second = flow
        .run_checkpointed("flow-empty", 9_u64, &store, &codec)
        .await?;
    assert_eq!(second, 7, "the terminal record wins over new input");
    Ok(())
}
