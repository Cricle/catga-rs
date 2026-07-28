//! Durable DSL checkpoint boundary and recovery contracts.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{DslFlow, DslProgressKind, DslStateCodec, DslStepProgress, DslStepProgressStore};
use futures::{StreamExt, stream};
use tokio::sync::Mutex;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct State {
    total: u32,
    branch: bool,
}

struct StateCodec;

impl DslStateCodec<State> for StateCodec {
    fn encode(&self, state: &State) -> CatgaResult<Vec<u8>> {
        Ok([
            state.total.to_be_bytes().as_slice(),
            &[u8::from(state.branch)],
        ]
        .concat())
    }

    fn decode(&self, bytes: &[u8]) -> CatgaResult<State> {
        let total: [u8; 4] = bytes
            .get(..4)
            .ok_or_else(|| CatgaError::new(ErrorCode::Validation, "checkpoint state is truncated"))?
            .try_into()
            .map_err(|_| CatgaError::new(ErrorCode::Validation, "checkpoint state is invalid"))?;
        let branch = match bytes.get(4..) {
            Some([0]) => false,
            Some([1]) => true,
            _ => {
                return Err(CatgaError::new(
                    ErrorCode::Validation,
                    "checkpoint state has an invalid branch flag",
                ));
            }
        };
        Ok(State {
            total: u32::from_be_bytes(total),
            branch,
        })
    }
}

struct RejectingTerminalCodec;

impl DslStateCodec<State> for RejectingTerminalCodec {
    fn encode(&self, _: &State) -> CatgaResult<Vec<u8>> {
        Ok(vec![0])
    }

    fn decode(&self, _: &[u8]) -> CatgaResult<State> {
        Err(CatgaError::new(
            ErrorCode::Validation,
            "terminal state cannot be decoded",
        ))
    }
}

#[derive(Default)]
struct ProgressStore {
    records: Mutex<HashMap<(Box<str>, u32), DslStepProgress>>,
}

#[async_trait]
impl DslStepProgressStore for ProgressStore {
    async fn create(&self, progress: DslStepProgress) -> CatgaResult<bool> {
        let key = (progress.flow_id().into(), progress.step_index());
        let mut records = self.records.lock().await;
        if records.contains_key(&key) {
            return Ok(false);
        }
        records.insert(key, progress);
        Ok(true)
    }

    async fn update(&self, expected_version: i64, next: DslStepProgress) -> CatgaResult<bool> {
        let key = (next.flow_id().into(), next.step_index());
        let mut records = self.records.lock().await;
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
            .await
            .get(&(flow_id.into(), step_index))
            .cloned())
    }

    async fn delete(&self, flow_id: &str, step_index: u32) -> CatgaResult<bool> {
        Ok(self
            .records
            .lock()
            .await
            .remove(&(flow_id.into(), step_index))
            .is_some())
    }
}

#[tokio::test]
async fn checkpointed_if_branch_restores_its_cursor_without_replaying_completed_children()
-> CatgaResult<()> {
    let completed_child_calls = Arc::new(AtomicUsize::new(0));
    let fail_once = Arc::new(AtomicBool::new(true));
    let completed_child = Arc::clone(&completed_child_calls);
    let fails = Arc::clone(&fail_once);
    let flow = DslFlow::new().if_else(
        |state: &State| state.branch,
        DslFlow::new()
            .action(move |state: &mut State| {
                let completed_child = Arc::clone(&completed_child);
                Box::pin(async move {
                    completed_child.fetch_add(1, Ordering::SeqCst);
                    state.total = state.total.saturating_add(1);
                    Ok(())
                })
            })
            .action(move |state: &mut State| {
                let fails = Arc::clone(&fails);
                Box::pin(async move {
                    if fails.swap(false, Ordering::SeqCst) {
                        return Err(CatgaError::new(
                            ErrorCode::Transient,
                            "interrupted after cursor",
                        ));
                    }
                    state.total = state.total.saturating_add(10);
                    Ok(())
                })
            }),
        DslFlow::new().action(|state: &mut State| {
            Box::pin(async move {
                state.total = 100;
                Ok(())
            })
        }),
    );
    let progress = ProgressStore::default();

    let first = flow
        .run_checkpointed(
            "branch-recovery",
            State {
                total: 0,
                branch: true,
            },
            &progress,
            &StateCodec,
        )
        .await;
    assert!(matches!(first, Err(error) if error.code() == ErrorCode::Transient));
    assert_eq!(completed_child_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        progress
            .get("branch-recovery", 0)
            .await?
            .expect("nested branch checkpoint exists")
            .kind(),
        DslProgressKind::CheckpointFrame
    );

    let resumed = flow
        .run_checkpointed("branch-recovery", State::default(), &progress, &StateCodec)
        .await?;
    assert_eq!(resumed.total, 11);
    assert!(resumed.branch);
    assert_eq!(completed_child_calls.load(Ordering::SeqCst), 1);

    let terminal = flow
        .run_checkpointed("branch-recovery", State::default(), &progress, &StateCodec)
        .await?;
    assert_eq!(terminal, resumed);
    assert_eq!(completed_child_calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn replayable_for_each_resumes_saved_items_and_terminal_result_is_idempotent()
-> CatgaResult<()> {
    let fail_once = Arc::new(AtomicBool::new(true));
    let item_one_calls = Arc::new(AtomicUsize::new(0));
    let item_two_calls = Arc::new(AtomicUsize::new(0));
    let item_three_calls = Arc::new(AtomicUsize::new(0));
    let fail = Arc::clone(&fail_once);
    let one = Arc::clone(&item_one_calls);
    let two = Arc::clone(&item_two_calls);
    let three = Arc::clone(&item_three_calls);
    let flow = DslFlow::new().for_each_replayable(
        |_: &State| vec![1_u32, 2, 3],
        move |state: &mut State, item| {
            let one = Arc::clone(&one);
            let two = Arc::clone(&two);
            let three = Arc::clone(&three);
            let fail = Arc::clone(&fail);
            Box::pin(async move {
                match item {
                    1 => {
                        one.fetch_add(1, Ordering::SeqCst);
                    }
                    2 => {
                        two.fetch_add(1, Ordering::SeqCst);
                        if fail.swap(false, Ordering::SeqCst) {
                            return Err(CatgaError::new(
                                ErrorCode::Transient,
                                "item two interrupted",
                            ));
                        }
                    }
                    3 => {
                        three.fetch_add(1, Ordering::SeqCst);
                    }
                    _ => {
                        return Err(CatgaError::new(
                            ErrorCode::Validation,
                            "test selection contains an unexpected item",
                        ));
                    }
                }
                state.total = state.total.saturating_add(item);
                Ok(())
            })
        },
    );
    let progress = ProgressStore::default();

    let first = flow
        .run_checkpointed("item-recovery", State::default(), &progress, &StateCodec)
        .await;
    assert!(matches!(first, Err(error) if error.code() == ErrorCode::Transient));
    assert_eq!(item_one_calls.load(Ordering::SeqCst), 1);
    assert_eq!(item_two_calls.load(Ordering::SeqCst), 1);
    assert_eq!(item_three_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        progress
            .get("item-recovery", 0)
            .await?
            .expect("replayable cursor exists")
            .kind(),
        DslProgressKind::CheckpointFrame
    );

    let resumed = flow
        .run_checkpointed("item-recovery", State::default(), &progress, &StateCodec)
        .await?;
    assert_eq!(resumed.total, 6);
    assert_eq!(item_one_calls.load(Ordering::SeqCst), 1);
    assert_eq!(item_two_calls.load(Ordering::SeqCst), 2);
    assert_eq!(item_three_calls.load(Ordering::SeqCst), 1);

    let terminal = flow
        .run_checkpointed("item-recovery", State::default(), &progress, &StateCodec)
        .await?;
    assert_eq!(terminal, resumed);
    assert_eq!(item_one_calls.load(Ordering::SeqCst), 1);
    assert_eq!(item_two_calls.load(Ordering::SeqCst), 2);
    assert_eq!(item_three_calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn checkpointed_parallel_restores_completed_branches_without_replaying_them()
-> CatgaResult<()> {
    let completed_calls = Arc::new(AtomicUsize::new(0));
    let fail_once = Arc::new(AtomicBool::new(true));
    let completed = Arc::clone(&completed_calls);
    let fails = Arc::clone(&fail_once);
    let flow = DslFlow::new().parallel(
        [
            DslFlow::new().action(move |state: &mut State| {
                let completed = Arc::clone(&completed);
                Box::pin(async move {
                    completed.fetch_add(1, Ordering::SeqCst);
                    state.total = 1;
                    Ok(())
                })
            }),
            DslFlow::new().action(move |state: &mut State| {
                let fails = Arc::clone(&fails);
                Box::pin(async move {
                    if fails.swap(false, Ordering::SeqCst) {
                        return Err(CatgaError::new(ErrorCode::Transient, "branch interrupted"));
                    }
                    state.total = 10;
                    Ok(())
                })
            }),
        ],
        |state, branches| {
            state.total = branches.into_iter().map(|branch| branch.total).sum();
            Ok(())
        },
    );
    let progress = ProgressStore::default();

    let first = flow
        .run_checkpointed(
            "parallel-recovery",
            State::default(),
            &progress,
            &StateCodec,
        )
        .await;
    assert!(matches!(first, Err(error) if error.code() == ErrorCode::Transient));
    assert_eq!(completed_calls.load(Ordering::SeqCst), 1);

    let resumed = flow
        .run_checkpointed(
            "parallel-recovery",
            State::default(),
            &progress,
            &StateCodec,
        )
        .await?;
    assert_eq!(resumed.total, 11);
    assert_eq!(completed_calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn checkpointed_parallel_recovers_an_interrupted_branch_from_its_local_cursor()
-> CatgaResult<()> {
    let first_branch_step_calls = Arc::new(AtomicUsize::new(0));
    let interrupted_branch_step_calls = Arc::new(AtomicUsize::new(0));
    let stable_branch_calls = Arc::new(AtomicUsize::new(0));
    let fail_once = Arc::new(AtomicBool::new(true));
    let first_step = Arc::clone(&first_branch_step_calls);
    let interrupted_step = Arc::clone(&interrupted_branch_step_calls);
    let stable_branch = Arc::clone(&stable_branch_calls);
    let fails = Arc::clone(&fail_once);
    let flow = DslFlow::new().parallel(
        [
            DslFlow::new()
                .action(move |state: &mut State| {
                    let first_step = Arc::clone(&first_step);
                    Box::pin(async move {
                        first_step.fetch_add(1, Ordering::SeqCst);
                        state.total = state.total.saturating_add(2);
                        Ok(())
                    })
                })
                .action(move |state: &mut State| {
                    let interrupted_step = Arc::clone(&interrupted_step);
                    let fails = Arc::clone(&fails);
                    Box::pin(async move {
                        interrupted_step.fetch_add(1, Ordering::SeqCst);
                        if fails.swap(false, Ordering::SeqCst) {
                            return Err(CatgaError::new(
                                ErrorCode::Transient,
                                "branch interrupted after its local checkpoint",
                            ));
                        }
                        state.total = state.total.saturating_add(20);
                        Ok(())
                    })
                }),
            DslFlow::new().action(move |state: &mut State| {
                let stable_branch = Arc::clone(&stable_branch);
                Box::pin(async move {
                    stable_branch.fetch_add(1, Ordering::SeqCst);
                    state.total = state.total.saturating_add(100);
                    Ok(())
                })
            }),
        ],
        |state, branches| {
            state.total = branches.into_iter().map(|branch| branch.total).sum();
            Ok(())
        },
    );
    let progress = ProgressStore::default();

    let first = flow
        .run_checkpointed(
            "parallel-local-recovery",
            State::default(),
            &progress,
            &StateCodec,
        )
        .await;
    assert!(matches!(first, Err(error) if error.code() == ErrorCode::Transient));
    assert_eq!(first_branch_step_calls.load(Ordering::SeqCst), 1);
    assert_eq!(interrupted_branch_step_calls.load(Ordering::SeqCst), 1);
    assert_eq!(stable_branch_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        progress
            .get("parallel-local-recovery", 0)
            .await?
            .expect("parallel recovery writes one outer checkpoint")
            .kind(),
        DslProgressKind::CheckpointFrame
    );

    let resumed = flow
        .run_checkpointed(
            "parallel-local-recovery",
            State::default(),
            &progress,
            &StateCodec,
        )
        .await?;
    assert_eq!(resumed.total, 122);
    assert_eq!(first_branch_step_calls.load(Ordering::SeqCst), 1);
    assert_eq!(interrupted_branch_step_calls.load(Ordering::SeqCst), 2);
    assert_eq!(stable_branch_calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn checkpointed_when_any_reuses_its_saved_winner_after_a_later_failure() -> CatgaResult<()> {
    let winner_calls = Arc::new(AtomicUsize::new(0));
    let fail_once = Arc::new(AtomicBool::new(true));
    let winner = Arc::clone(&winner_calls);
    let fails = Arc::clone(&fail_once);
    let flow = DslFlow::new()
        .when_any(
            [
                DslFlow::new().action(|_: &mut State| {
                    Box::pin(async { Err(CatgaError::new(ErrorCode::Transient, "loser")) })
                }),
                DslFlow::new().action(move |state: &mut State| {
                    let winner = Arc::clone(&winner);
                    Box::pin(async move {
                        winner.fetch_add(1, Ordering::SeqCst);
                        state.total = 9;
                        Ok(())
                    })
                }),
            ],
            |state, selected| {
                state.total = selected.total;
                Ok(())
            },
        )
        .action(move |state: &mut State| {
            let fails = Arc::clone(&fails);
            Box::pin(async move {
                if fails.swap(false, Ordering::SeqCst) {
                    return Err(CatgaError::new(ErrorCode::Transient, "after winner"));
                }
                state.total = state.total.saturating_add(1);
                Ok(())
            })
        });
    let progress = ProgressStore::default();

    let first = flow
        .run_checkpointed("winner-recovery", State::default(), &progress, &StateCodec)
        .await;
    assert!(matches!(first, Err(error) if error.code() == ErrorCode::Transient));
    assert_eq!(winner_calls.load(Ordering::SeqCst), 1);

    let resumed = flow
        .run_checkpointed("winner-recovery", State::default(), &progress, &StateCodec)
        .await?;
    assert_eq!(resumed.total, 10);
    assert_eq!(winner_calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn checkpointed_match_restores_the_selected_branch_without_replaying_its_completed_steps()
-> CatgaResult<()> {
    let completed_child_calls = Arc::new(AtomicUsize::new(0));
    let fail_once = Arc::new(AtomicBool::new(true));
    let completed = Arc::clone(&completed_child_calls);
    let fails = Arc::clone(&fail_once);
    let flow = DslFlow::new().match_on(
        |state: &State| state.branch,
        [(
            true,
            DslFlow::new()
                .action(move |state: &mut State| {
                    let completed = Arc::clone(&completed);
                    Box::pin(async move {
                        completed.fetch_add(1, Ordering::SeqCst);
                        state.total = state.total.saturating_add(2);
                        Ok(())
                    })
                })
                .action(move |state: &mut State| {
                    let fails = Arc::clone(&fails);
                    Box::pin(async move {
                        if fails.swap(false, Ordering::SeqCst) {
                            return Err(CatgaError::new(
                                ErrorCode::Transient,
                                "match branch interrupted",
                            ));
                        }
                        state.total = state.total.saturating_add(20);
                        Ok(())
                    })
                }),
        )],
        DslFlow::new().action(|state: &mut State| {
            Box::pin(async move {
                state.total = 1_000;
                Ok(())
            })
        }),
    );
    let progress = ProgressStore::default();

    let first = flow
        .run_checkpointed(
            "match-recovery",
            State {
                total: 0,
                branch: true,
            },
            &progress,
            &StateCodec,
        )
        .await;
    assert!(matches!(first, Err(error) if error.code() == ErrorCode::Transient));
    assert_eq!(completed_child_calls.load(Ordering::SeqCst), 1);

    let resumed = flow
        .run_checkpointed("match-recovery", State::default(), &progress, &StateCodec)
        .await?;
    assert_eq!(resumed.total, 22);
    assert!(resumed.branch);
    assert_eq!(completed_child_calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn checkpointed_flows_reject_process_local_collection_steps_before_executing_them()
-> CatgaResult<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let sequential_calls = Arc::clone(&calls);
    let sequential = DslFlow::new().for_each(
        |_: &State| vec![1_u32],
        move |_: &mut State, _| {
            let calls = Arc::clone(&sequential_calls);
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        },
    );
    let stream_calls = Arc::clone(&calls);
    let stream_flow = DslFlow::new().for_each_stream(
        |_| stream::iter([1_u32]).boxed(),
        move |_: &mut State, _| {
            let calls = Arc::clone(&stream_calls);
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        },
    );
    let concurrent_calls = Arc::clone(&calls);
    let concurrent = DslFlow::new().for_each_stream_concurrent(
        1,
        |_| stream::iter([1_u32]).boxed(),
        move |_: &State, _| {
            let calls = Arc::clone(&concurrent_calls);
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        },
        |_: &mut State, ()| Ok(()),
    )?;
    let progress = ProgressStore::default();

    for (flow_id, flow) in [
        ("checkpointed-sequential", &sequential),
        ("checkpointed-stream", &stream_flow),
        ("checkpointed-concurrent", &concurrent),
    ] {
        let error = flow
            .run_checkpointed(flow_id, State::default(), &progress, &StateCodec)
            .await
            .expect_err("process-local collection steps require an explicit replay cursor");
        assert_eq!(error.code(), ErrorCode::Validation);
        assert!(progress.get(flow_id, 0).await?.is_none());
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn checkpointed_replayable_for_each_persists_handled_errors_without_replaying_items()
-> CatgaResult<()> {
    let item_calls = Arc::new(AtomicUsize::new(0));
    let fail_after_collection = Arc::new(AtomicBool::new(true));
    let calls = Arc::clone(&item_calls);
    let fail = Arc::clone(&fail_after_collection);
    let flow = DslFlow::new()
        .for_each_replayable_continue_on_error(
            |_: &State| vec![1_u32, 2, 3],
            move |state: &mut State, item| {
                let calls = Arc::clone(&calls);
                Box::pin(async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    if item == 2 {
                        return Err(CatgaError::new(ErrorCode::Validation, "item rejected"));
                    }
                    state.total = state.total.saturating_add(item);
                    Ok(())
                })
            },
            |state, index, error| {
                Box::pin(async move {
                    assert_eq!(index, 1);
                    assert_eq!(error.code(), ErrorCode::Validation);
                    state.total = state.total.saturating_add(100);
                    Ok(())
                })
            },
        )
        .action(move |state: &mut State| {
            let fail = Arc::clone(&fail);
            Box::pin(async move {
                if fail.swap(false, Ordering::SeqCst) {
                    return Err(CatgaError::new(
                        ErrorCode::Transient,
                        "interrupted after handled item",
                    ));
                }
                state.total = state.total.saturating_add(1);
                Ok(())
            })
        });
    let progress = ProgressStore::default();

    let first = flow
        .run_checkpointed(
            "handled-item-recovery",
            State::default(),
            &progress,
            &StateCodec,
        )
        .await;
    assert!(matches!(first, Err(error) if error.code() == ErrorCode::Transient));
    assert_eq!(item_calls.load(Ordering::SeqCst), 3);

    let resumed = flow
        .run_checkpointed(
            "handled-item-recovery",
            State::default(),
            &progress,
            &StateCodec,
        )
        .await?;
    assert_eq!(resumed.total, 105);
    assert_eq!(item_calls.load(Ordering::SeqCst), 3);
    Ok(())
}

#[tokio::test]
async fn checkpointed_legacy_step_payload_resumes_after_the_committed_step() -> CatgaResult<()> {
    let committed_step_calls = Arc::new(AtomicUsize::new(0));
    let committed_calls = Arc::clone(&committed_step_calls);
    let flow = DslFlow::new()
        .action(move |state: &mut State| {
            let committed_calls = Arc::clone(&committed_calls);
            Box::pin(async move {
                committed_calls.fetch_add(1, Ordering::SeqCst);
                state.total = state.total.saturating_add(100);
                Ok(())
            })
        })
        .action(|state: &mut State| {
            Box::pin(async move {
                state.total = state.total.saturating_add(1);
                Ok(())
            })
        });
    let progress = ProgressStore::default();
    let committed = State {
        total: 41,
        branch: true,
    };
    assert!(
        progress
            .create(DslStepProgress::new(
                "legacy-step-payload",
                0,
                StateCodec.encode(&committed)?,
            ))
            .await?
    );

    let resumed = flow
        .run_checkpointed(
            "legacy-step-payload",
            State::default(),
            &progress,
            &StateCodec,
        )
        .await?;

    assert_eq!(resumed.total, 42);
    assert!(resumed.branch);
    assert_eq!(committed_step_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        progress
            .get("legacy-step-payload", u32::MAX)
            .await?
            .expect("a completed checkpoint writes its terminal result")
            .kind(),
        DslProgressKind::Terminal
    );
    Ok(())
}

#[tokio::test]
async fn checkpointed_flows_reject_invalid_terminal_records_before_actions_run() -> CatgaResult<()>
{
    let actions = Arc::new(AtomicUsize::new(0));
    let action_calls = Arc::clone(&actions);
    let flow = DslFlow::new().action(move |_: &mut State| {
        let action_calls = Arc::clone(&action_calls);
        Box::pin(async move {
            action_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    });

    let non_terminal = ProgressStore::default();
    assert!(
        non_terminal
            .create(DslStepProgress::new("wrong-terminal-kind", u32::MAX, []))
            .await?
    );
    let error = flow
        .run_checkpointed(
            "wrong-terminal-kind",
            State::default(),
            &non_terminal,
            &StateCodec,
        )
        .await
        .expect_err("the reserved terminal slot cannot contain an ordinary progress record");
    assert_eq!(error.code(), ErrorCode::Conflict);

    assert_eq!(actions.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn checkpointed_terminal_decode_failure_never_replays_completed_actions() -> CatgaResult<()> {
    let actions = Arc::new(AtomicUsize::new(0));
    let action_calls = Arc::clone(&actions);
    let flow = DslFlow::new().action(move |_: &mut State| {
        let action_calls = Arc::clone(&action_calls);
        Box::pin(async move {
            action_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    });
    let progress = ProgressStore::default();

    flow.run_checkpointed(
        "undecodable-terminal",
        State::default(),
        &progress,
        &RejectingTerminalCodec,
    )
    .await?;
    let error = flow
        .run_checkpointed(
            "undecodable-terminal",
            State::default(),
            &progress,
            &RejectingTerminalCodec,
        )
        .await
        .expect_err("terminal data is decoded before the flow can execute again");

    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(actions.load(Ordering::SeqCst), 1);
    Ok(())
}
