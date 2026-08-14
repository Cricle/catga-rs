//! Strict scenario contracts for the in-process [`DslFlow`] execution engine.
//!
//! Covers sequential ordering, retry, timeout, conditional branches, parallel
//! fan-out/fan-in, `when_any`, item loops, step decorators, lifecycle
//! observation, throttling, and cancellation propagation. Every assertion is
//! deterministic: no wall-clock sleep exceeds a bounded timeout step.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use catga_core::retry_delay;
use catga_core::flow::dsl_lifecycle::{
    DslFlowLifecycleEvent, DslFlowLifecycleHooks, DslFlowLifecycleObserver,
};
use catga_core::flow::dsl_step::MAX_DSL_PARALLEL_BRANCHES;
use catga_core::flow::flow_throttle::FlowThrottle;
use catga_core::flow::{DslFlow, DslStep};
use catga_core::{CatgaError, CatgaResult, ErrorCode};

fn lifecycle_event_name(event: &DslFlowLifecycleEvent) -> String {
    match event {
        DslFlowLifecycleEvent::StepSucceeded { step_index } => format!("step-ok:{step_index}"),
        DslFlowLifecycleEvent::StepFailed { step_index, error } => {
            format!("step-err:{step_index}:{:?}", error.code())
        }
        DslFlowLifecycleEvent::FlowSucceeded => "flow-ok".to_string(),
        DslFlowLifecycleEvent::FlowFailed { error } => format!("flow-err:{:?}", error.code()),
    }
}

#[derive(Default)]
struct RecordingObserver {
    events: Mutex<Vec<String>>,
}

impl DslFlowLifecycleObserver for RecordingObserver {
    fn observe(&self, event: &DslFlowLifecycleEvent) {
        self.events
            .lock()
            .expect("observer lock")
            .push(lifecycle_event_name(event));
    }
}

impl RecordingObserver {
    fn events(&self) -> Vec<String> {
        self.events.lock().expect("observer lock").clone()
    }
}

#[tokio::test]
async fn dsl_run_executes_steps_in_declaration_order() -> CatgaResult<()> {
    let flow = DslFlow::new()
        .action(|state: &mut Vec<u32>| {
            Box::pin(async move {
                state.push(1);
                Ok(())
            })
        })
        .action(|state: &mut Vec<u32>| {
            Box::pin(async move {
                state.push(2);
                Ok(())
            })
        })
        .action(|state: &mut Vec<u32>| {
            Box::pin(async move {
                state.push(3);
                Ok(())
            })
        });

    let mut state = Vec::new();
    flow.run(&mut state).await?;
    assert_eq!(state, vec![1, 2, 3]);
    Ok(())
}

#[tokio::test]
async fn dsl_run_stops_at_the_first_failure_and_keeps_prior_mutations() -> CatgaResult<()> {
    let third = Arc::new(AtomicUsize::new(0));
    let third_marker = Arc::clone(&third);
    let flow = DslFlow::new()
        .action(|state: &mut u64| {
            Box::pin(async move {
                *state += 1;
                Ok(())
            })
        })
        .action(|_state: &mut u64| {
            Box::pin(async move { Err(CatgaError::new(ErrorCode::Internal, "step two failed")) })
        })
        .action(move |_state: &mut u64| {
            let third = Arc::clone(&third_marker);
            Box::pin(async move {
                third.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        });

    let mut state = 0_u64;
    let error = flow
        .run(&mut state)
        .await
        .expect_err("the failing step must stop the flow");
    assert_eq!(error.code(), ErrorCode::Internal);
    assert_eq!(error.message(), "step two failed");
    // The DSL performs no rollback: earlier mutations remain visible.
    assert_eq!(state, 1);
    assert_eq!(third.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn dsl_cancellation_error_propagates_out_of_run() -> CatgaResult<()> {
    let flow = DslFlow::new().action(|_state: &mut u64| {
        Box::pin(async move { Err(CatgaError::new(ErrorCode::Cancelled, "stop requested")) })
    });

    let mut state = 0_u64;
    let error = flow
        .run(&mut state)
        .await
        .expect_err("a cancelled step must fail the flow");
    assert_eq!(error.code(), ErrorCode::Cancelled);
    Ok(())
}

#[tokio::test]
async fn dsl_retry_recovers_from_transient_failures() -> CatgaResult<()> {
    let attempts = Arc::new(AtomicUsize::new(0));
    let retry_attempts = Arc::clone(&attempts);
    let flow = DslFlow::new().retry(3, Duration::ZERO, move |state: &mut u64| {
        let attempts = Arc::clone(&retry_attempts);
        Box::pin(async move {
            if attempts.fetch_add(1, Ordering::SeqCst) < 2 {
                return Err(CatgaError::new(ErrorCode::Transient, "still busy"));
            }
            *state += 1;
            Ok(())
        })
    });

    let mut state = 0_u64;
    flow.run(&mut state).await?;
    assert_eq!(state, 1);
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    Ok(())
}

#[tokio::test]
async fn dsl_retry_never_retries_non_transient_errors() -> CatgaResult<()> {
    let attempts = Arc::new(AtomicUsize::new(0));
    let retry_attempts = Arc::clone(&attempts);
    let flow = DslFlow::new().retry(5, Duration::ZERO, move |_state: &mut u64| {
        let attempts = Arc::clone(&retry_attempts);
        Box::pin(async move {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err(CatgaError::new(ErrorCode::Validation, "permanent"))
        })
    });

    let mut state = 0_u64;
    let error = flow
        .run(&mut state)
        .await
        .expect_err("non-transient errors are not retryable");
    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn dsl_retry_exhaustion_returns_the_last_transient_error() -> CatgaResult<()> {
    let attempts = Arc::new(AtomicUsize::new(0));
    let retry_attempts = Arc::clone(&attempts);
    let flow = DslFlow::new().retry(2, Duration::ZERO, move |_state: &mut u64| {
        let attempts = Arc::clone(&retry_attempts);
        Box::pin(async move {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            Err(CatgaError::new(
                ErrorCode::Transient,
                format!("attempt {attempt} failed"),
            ))
        })
    });

    let mut state = 0_u64;
    let error = flow
        .run(&mut state)
        .await
        .expect_err("exhausted retries must surface the last error");
    assert_eq!(error.code(), ErrorCode::Transient);
    // max_retries counts attempts after the first execution.
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert_eq!(error.message(), "attempt 2 failed");
    Ok(())
}

#[test]
fn retry_delay_doubles_from_the_initial_delay_and_saturates() {
    let initial = Duration::from_secs(1);
    assert_eq!(retry_delay(initial, 0), Duration::from_secs(1));
    assert_eq!(retry_delay(initial, 1), Duration::from_secs(2));
    assert_eq!(retry_delay(initial, 2), Duration::from_secs(4));
    assert_eq!(retry_delay(Duration::ZERO, 5), Duration::ZERO);
    assert_eq!(retry_delay(Duration::MAX, 1), Duration::MAX);
    assert_eq!(
        retry_delay(initial, usize::BITS as usize),
        initial.saturating_mul(u32::MAX)
    );
}

#[tokio::test]
async fn dsl_timeout_completes_fast_actions() -> CatgaResult<()> {
    let flow = DslFlow::new().timeout(Duration::from_secs(30), |state: &mut u64| {
        Box::pin(async move {
            *state += 1;
            Ok(())
        })
    });

    let mut state = 0_u64;
    flow.run(&mut state).await?;
    assert_eq!(state, 1);
    Ok(())
}

#[tokio::test]
async fn dsl_timeout_cancels_an_overrunning_action() -> CatgaResult<()> {
    let flow = DslFlow::new().timeout(Duration::from_millis(20), |_state: &mut u64| {
        Box::pin(async move {
            futures::future::pending::<()>().await;
            Ok(())
        })
    });

    let mut state = 0_u64;
    let error = flow
        .run(&mut state)
        .await
        .expect_err("an overrunning action must time out");
    assert_eq!(error.code(), ErrorCode::Timeout);
    assert_eq!(error.message(), "flow action timed out");
    Ok(())
}

#[tokio::test]
async fn dsl_if_else_runs_only_the_selected_branch() -> CatgaResult<()> {
    let flow = DslFlow::new().if_else(
        |state: &u64| *state > 0,
        DslFlow::new().action(|state: &mut u64| {
            Box::pin(async move {
                *state += 100;
                Ok(())
            })
        }),
        DslFlow::new().action(|state: &mut u64| {
            Box::pin(async move {
                *state += 1;
                Ok(())
            })
        }),
    );

    let mut positive = 5_u64;
    flow.run(&mut positive).await?;
    assert_eq!(positive, 105);

    let mut zero = 0_u64;
    flow.run(&mut zero).await?;
    assert_eq!(zero, 1);
    Ok(())
}

#[tokio::test]
async fn dsl_if_else_propagates_branch_failures() -> CatgaResult<()> {
    let flow = DslFlow::new().if_else(
        |_state: &u64| true,
        DslFlow::new().action(|_state: &mut u64| {
            Box::pin(async move { Err(CatgaError::new(ErrorCode::Forbidden, "then failed")) })
        }),
        DslFlow::new(),
    );

    let mut state = 0_u64;
    let error = flow
        .run(&mut state)
        .await
        .expect_err("branch failure must propagate");
    assert_eq!(error.code(), ErrorCode::Forbidden);
    Ok(())
}

#[tokio::test]
async fn dsl_match_on_selects_the_matching_case_or_default() -> CatgaResult<()> {
    let flow = DslFlow::new().match_on(
        |state: &u64| *state,
        [
            (
                1_u64,
                DslFlow::new().action(|state: &mut u64| {
                    Box::pin(async move {
                        *state += 10;
                        Ok(())
                    })
                }),
            ),
            (
                2_u64,
                DslFlow::new().action(|state: &mut u64| {
                    Box::pin(async move {
                        *state += 20;
                        Ok(())
                    })
                }),
            ),
        ],
        DslFlow::new().action(|state: &mut u64| {
            Box::pin(async move {
                *state += 1;
                Ok(())
            })
        }),
    );

    let mut one = 1_u64;
    flow.run(&mut one).await?;
    assert_eq!(one, 11);

    let mut two = 2_u64;
    flow.run(&mut two).await?;
    assert_eq!(two, 22);

    let mut unmatched = 9_u64;
    flow.run(&mut unmatched).await?;
    assert_eq!(unmatched, 10);
    Ok(())
}

#[tokio::test]
async fn dsl_match_on_duplicate_case_keeps_the_last_branch() -> CatgaResult<()> {
    let flow = DslFlow::new().match_on(
        |_state: &u64| 1_u64,
        [
            (
                1_u64,
                DslFlow::new().action(|state: &mut u64| {
                    Box::pin(async move {
                        *state += 1;
                        Ok(())
                    })
                }),
            ),
            (
                1_u64,
                DslFlow::new().action(|state: &mut u64| {
                    Box::pin(async move {
                        *state += 2;
                        Ok(())
                    })
                }),
            ),
        ],
        DslFlow::new(),
    );

    let mut state = 0_u64;
    flow.run(&mut state).await?;
    assert_eq!(state, 2, "map insertion semantics keep the last branch");
    Ok(())
}

#[tokio::test]
async fn dsl_parallel_runs_every_branch_and_merges_in_declaration_order() -> CatgaResult<()> {
    let branch_runs = Arc::new(AtomicUsize::new(0));
    let mut branches = Vec::new();
    for value in 0_u64..3 {
        let runs = Arc::clone(&branch_runs);
        branches.push(DslFlow::new().action(move |state: &mut u64| {
            let runs = Arc::clone(&runs);
            Box::pin(async move {
                runs.fetch_add(1, Ordering::SeqCst);
                *state = value + 1;
                Ok(())
            })
        }));
    }
    let flow = DslFlow::new().parallel(branches, |state: &mut u64, results: Vec<u64>| {
        assert_eq!(results, vec![1, 2, 3], "merge receives declaration order");
        *state = results.into_iter().sum();
        Ok(())
    });

    let mut state = 0_u64;
    flow.run(&mut state).await?;
    assert_eq!(branch_runs.load(Ordering::SeqCst), 3);
    assert_eq!(state, 6);
    Ok(())
}

#[tokio::test]
async fn dsl_parallel_surfaces_the_first_declared_error_and_skips_merge() -> CatgaResult<()> {
    let merge_runs = Arc::new(AtomicUsize::new(0));
    let merge_marker = Arc::clone(&merge_runs);
    let flow = DslFlow::new().parallel(
        [
            DslFlow::new().action(|state: &mut u64| {
                Box::pin(async move {
                    *state += 100;
                    Ok(())
                })
            }),
            DslFlow::new().action(|_state: &mut u64| {
                Box::pin(async move { Err(CatgaError::new(ErrorCode::Conflict, "branch failed")) })
            }),
        ],
        move |state: &mut u64, results: Vec<u64>| {
            merge_marker.fetch_add(1, Ordering::SeqCst);
            *state = results.into_iter().sum();
            Ok(())
        },
    );

    let mut state = 7_u64;
    let error = flow
        .run(&mut state)
        .await
        .expect_err("a failed branch must fail the parallel step");
    assert_eq!(error.code(), ErrorCode::Conflict);
    assert_eq!(merge_runs.load(Ordering::SeqCst), 0, "merge must not run");
    assert_eq!(
        state, 7,
        "a failed branch leaves the original state unchanged"
    );
    Ok(())
}

#[tokio::test]
async fn dsl_parallel_prefers_the_earliest_declared_branch_error() -> CatgaResult<()> {
    let flow = DslFlow::new().parallel(
        [
            DslFlow::new().action(|_state: &mut u64| {
                Box::pin(async move { Err(CatgaError::new(ErrorCode::Validation, "first")) })
            }),
            DslFlow::new().action(|_state: &mut u64| {
                Box::pin(async move { Err(CatgaError::new(ErrorCode::Internal, "second")) })
            }),
        ],
        |_state: &mut u64, _results: Vec<u64>| Ok(()),
    );

    let mut state = 0_u64;
    let error = flow
        .run(&mut state)
        .await
        .expect_err("parallel failure must surface an error");
    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(error.message(), "first");
    Ok(())
}

#[tokio::test]
async fn dsl_parallel_rejects_more_branches_than_the_supported_limit() -> CatgaResult<()> {
    let branches: Vec<DslFlow<u64>> = (0..=MAX_DSL_PARALLEL_BRANCHES)
        .map(|_| DslFlow::new())
        .collect();
    let flow = DslFlow::new().parallel(branches, |_state: &mut u64, _results: Vec<u64>| Ok(()));

    let mut state = 0_u64;
    let error = flow
        .run(&mut state)
        .await
        .expect_err("branch fanout beyond the limit must be rejected");
    assert_eq!(error.code(), ErrorCode::Validation);
    Ok(())
}

#[tokio::test]
async fn dsl_parallel_with_no_branches_merges_an_empty_result_set() -> CatgaResult<()> {
    let flow = DslFlow::new().parallel(
        Vec::<DslFlow<u64>>::new(),
        |state: &mut u64, results: Vec<u64>| {
            assert!(results.is_empty());
            *state = 42;
            Ok(())
        },
    );

    let mut state = 0_u64;
    flow.run(&mut state).await?;
    assert_eq!(state, 42);
    Ok(())
}

#[tokio::test]
async fn dsl_when_all_behaves_like_parallel() -> CatgaResult<()> {
    let flow = DslFlow::new().when_all(
        [
            DslFlow::new().action(|state: &mut u64| {
                Box::pin(async move {
                    *state = 2;
                    Ok(())
                })
            }),
            DslFlow::new().action(|state: &mut u64| {
                Box::pin(async move {
                    *state = 3;
                    Ok(())
                })
            }),
        ],
        |state: &mut u64, results: Vec<u64>| {
            *state = results.into_iter().product();
            Ok(())
        },
    );

    let mut state = 0_u64;
    flow.run(&mut state).await?;
    assert_eq!(state, 6);
    Ok(())
}

#[tokio::test]
async fn dsl_when_any_merges_the_first_successful_branch() -> CatgaResult<()> {
    let flow = DslFlow::new().when_any(
        [
            DslFlow::new().action(|_state: &mut u64| {
                Box::pin(async move { Err(CatgaError::new(ErrorCode::Transient, "loser")) })
            }),
            DslFlow::new().action(|state: &mut u64| {
                Box::pin(async move {
                    *state = 9;
                    Ok(())
                })
            }),
        ],
        |state: &mut u64, winner: u64| {
            *state = winner;
            Ok(())
        },
    );

    let mut state = 0_u64;
    flow.run(&mut state).await?;
    assert_eq!(state, 9, "the single successful branch supplies the winner");
    Ok(())
}

#[tokio::test]
async fn dsl_when_any_fails_only_when_every_branch_fails() -> CatgaResult<()> {
    let flow = DslFlow::new().when_any(
        [
            DslFlow::new().action(|_state: &mut u64| {
                Box::pin(async move { Err(CatgaError::new(ErrorCode::Transient, "a")) })
            }),
            DslFlow::new().action(|_state: &mut u64| {
                Box::pin(async move { Err(CatgaError::new(ErrorCode::Transient, "b")) })
            }),
        ],
        |state: &mut u64, winner: u64| {
            *state = winner;
            Ok(())
        },
    );

    let mut state = 7_u64;
    let error = flow
        .run(&mut state)
        .await
        .expect_err("when_any fails when every branch fails");
    assert_eq!(error.code(), ErrorCode::Transient);
    assert_eq!(state, 7, "a failed when_any leaves the state unchanged");
    Ok(())
}

#[tokio::test]
async fn dsl_when_any_with_no_branches_succeeds_without_merging() -> CatgaResult<()> {
    let merge_runs = Arc::new(AtomicUsize::new(0));
    let merge_marker = Arc::clone(&merge_runs);
    let flow = DslFlow::new().when_any(
        Vec::<DslFlow<u64>>::new(),
        move |state: &mut u64, winner: u64| {
            merge_marker.fetch_add(1, Ordering::SeqCst);
            *state = winner;
            Ok(())
        },
    );

    let mut state = 5_u64;
    flow.run(&mut state).await?;
    assert_eq!(state, 5);
    assert_eq!(merge_runs.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn dsl_for_each_processes_items_in_order_and_stops_on_error() -> CatgaResult<()> {
    let flow = DslFlow::new().for_each(
        |_state: &Vec<u32>| vec![1_u32, 2, 3],
        |state: &mut Vec<u32>, item: u32| {
            Box::pin(async move {
                if item == 2 {
                    return Err(CatgaError::new(ErrorCode::Internal, "item two failed"));
                }
                state.push(item * 10);
                Ok(())
            })
        },
    );

    let mut state = Vec::new();
    let error = flow
        .run(&mut state)
        .await
        .expect_err("an item failure stops the loop");
    assert_eq!(error.code(), ErrorCode::Internal);
    assert_eq!(state, vec![10], "item three must never start");
    Ok(())
}

#[tokio::test]
async fn dsl_for_each_continue_on_error_records_failures_and_completes() -> CatgaResult<()> {
    #[derive(Default)]
    struct LoopState {
        processed: Vec<u32>,
        errors: Vec<usize>,
    }

    let flow = DslFlow::new().for_each_continue_on_error(
        |_state: &LoopState| vec![1_u32, 2, 3],
        |state: &mut LoopState, item: u32| {
            Box::pin(async move {
                if item == 2 {
                    return Err(CatgaError::new(ErrorCode::Internal, "bad item"));
                }
                state.processed.push(item * 10);
                Ok(())
            })
        },
        |state: &mut LoopState, index: usize, error: CatgaError| {
            Box::pin(async move {
                assert_eq!(error.message(), "bad item");
                state.errors.push(index);
                Ok(())
            })
        },
    );

    let mut state = LoopState::default();
    flow.run(&mut state).await?;
    assert_eq!(state.processed, vec![10, 30]);
    assert_eq!(state.errors, vec![1], "the failing item index is reported");
    Ok(())
}

#[tokio::test]
async fn dsl_for_each_continue_on_error_stops_when_the_handler_fails() -> CatgaResult<()> {
    let flow = DslFlow::new().for_each_continue_on_error(
        |_state: &Vec<u32>| vec![1_u32, 2, 3],
        |state: &mut Vec<u32>, item: u32| {
            Box::pin(async move {
                if item == 1 {
                    return Err(CatgaError::new(ErrorCode::Internal, "item failed"));
                }
                state.push(item);
                Ok(())
            })
        },
        |_state: &mut Vec<u32>, _index: usize, _error: CatgaError| {
            Box::pin(
                async move { Err(CatgaError::new(ErrorCode::HandlerFailed, "handler failed")) },
            )
        },
    );

    let mut state = Vec::new();
    let error = flow
        .run(&mut state)
        .await
        .expect_err("a failing error handler stops the loop");
    assert_eq!(error.code(), ErrorCode::HandlerFailed);
    assert!(state.is_empty(), "later items must never start");
    Ok(())
}

#[tokio::test]
async fn dsl_for_each_stream_processes_items_and_stops_on_error() -> CatgaResult<()> {
    use futures::StreamExt;

    let flow = DslFlow::new().for_each_stream(
        |_state: &Vec<u32>| futures::stream::iter(vec![1_u32, 2, 3]).boxed(),
        |state: &mut Vec<u32>, item: u32| {
            Box::pin(async move {
                if item == 3 {
                    return Err(CatgaError::new(ErrorCode::Internal, "stream item failed"));
                }
                state.push(item);
                Ok(())
            })
        },
    );

    let mut state = Vec::new();
    let error = flow
        .run(&mut state)
        .await
        .expect_err("a stream item failure stops polling");
    assert_eq!(error.code(), ErrorCode::Internal);
    assert_eq!(state, vec![1, 2]);
    Ok(())
}

#[tokio::test]
async fn dsl_for_each_stream_concurrent_requires_a_positive_limit() {
    use futures::StreamExt;

    let result = DslFlow::new().for_each_stream_concurrent(
        0,
        |_state: &Vec<u32>| futures::stream::empty::<u32>().boxed(),
        |_state: &Vec<u32>, item: u32| Box::pin(async move { Ok(item) }),
        |_state: &mut Vec<u32>, _item: u32| Ok(()),
    );
    let error = result.err().expect("a zero concurrency limit is invalid");
    assert_eq!(error.code(), ErrorCode::Validation);
}

#[tokio::test]
async fn dsl_for_each_stream_concurrent_reduces_batches_in_source_order() -> CatgaResult<()> {
    use futures::StreamExt;

    let flow = DslFlow::new()
        .for_each_stream_concurrent(
            2,
            |_state: &Vec<u32>| futures::stream::iter(vec![1_u32, 2, 3, 4, 5]).boxed(),
            |_state: &Vec<u32>, item: u32| Box::pin(async move { Ok(item * 2) }),
            |state: &mut Vec<u32>, result: u32| {
                state.push(result);
                Ok(())
            },
        )
        .expect("a positive limit is valid");

    let mut state = Vec::new();
    flow.run(&mut state).await?;
    assert_eq!(
        state,
        vec![2, 4, 6, 8, 10],
        "reduction follows source order"
    );
    Ok(())
}

#[tokio::test]
async fn dsl_for_each_stream_concurrent_propagates_work_errors() -> CatgaResult<()> {
    use futures::StreamExt;

    let flow = DslFlow::new()
        .for_each_stream_concurrent(
            3,
            |_state: &Vec<u32>| futures::stream::iter(vec![1_u32, 2, 3]).boxed(),
            |_state: &Vec<u32>, item: u32| {
                Box::pin(async move {
                    if item == 2 {
                        return Err(CatgaError::new(ErrorCode::Internal, "work failed"));
                    }
                    Ok(item)
                })
            },
            |state: &mut Vec<u32>, result: u32| {
                state.push(result);
                Ok(())
            },
        )
        .expect("a positive limit is valid");

    let mut state = Vec::new();
    let error = flow
        .run(&mut state)
        .await
        .expect_err("a work failure must propagate");
    assert_eq!(error.code(), ErrorCode::Internal);
    Ok(())
}

#[tokio::test]
async fn dsl_step_only_when_accumulates_conditions_with_logical_and() -> CatgaResult<()> {
    let flow = DslFlow::new()
        .step(
            DslStep::action(|state: &mut u64| {
                Box::pin(async move {
                    *state += 1;
                    Ok(())
                })
            })
            .only_when(|_state: &u64| false),
        )
        .step(
            DslStep::action(|state: &mut u64| {
                Box::pin(async move {
                    *state += 10;
                    Ok(())
                })
            })
            .only_when(|_state: &u64| true)
            .only_when(|_state: &u64| false),
        )
        .step(
            DslStep::action(|state: &mut u64| {
                Box::pin(async move {
                    *state += 100;
                    Ok(())
                })
            })
            .only_when(|_state: &u64| true)
            .only_when(|_state: &u64| true),
        );

    let mut state = 0_u64;
    flow.run(&mut state).await?;
    assert_eq!(state, 100, "only the doubly-true step runs");
    Ok(())
}

#[tokio::test]
async fn dsl_step_optional_swallows_errors_but_never_cancellation() -> CatgaResult<()> {
    let flow = DslFlow::new()
        .step(
            DslStep::action(|_state: &mut u64| {
                Box::pin(async move { Err(CatgaError::new(ErrorCode::Internal, "ignored")) })
            })
            .optional(),
        )
        .action(|state: &mut u64| {
            Box::pin(async move {
                *state += 1;
                Ok(())
            })
        });

    let mut state = 0_u64;
    flow.run(&mut state).await?;
    assert_eq!(state, 1);

    let cancelling = DslFlow::new().step(
        DslStep::action(|_state: &mut u64| {
            Box::pin(async move { Err(CatgaError::new(ErrorCode::Cancelled, "stop")) })
        })
        .optional(),
    );
    let mut state = 0_u64;
    let error = cancelling
        .run(&mut state)
        .await
        .expect_err("optional must not swallow cancellation");
    assert_eq!(error.code(), ErrorCode::Cancelled);
    Ok(())
}

#[tokio::test]
async fn dsl_step_fail_if_variants_run_after_a_successful_action() -> CatgaResult<()> {
    let validated = DslFlow::new().step(
        DslStep::action(|state: &mut u64| {
            Box::pin(async move {
                *state = 1;
                Ok(())
            })
        })
        .fail_if(|state: &u64| *state > 0),
    );
    let mut state = 0_u64;
    let error = validated
        .run(&mut state)
        .await
        .expect_err("a matching state condition fails the step");
    assert_eq!(error.code(), ErrorCode::Validation);

    let custom = DslFlow::new().step(
        DslStep::action(|state: &mut u64| {
            Box::pin(async move {
                *state = 1;
                Ok(())
            })
        })
        .fail_if_with(
            |state: &u64| *state == 1,
            |_state: &u64| CatgaError::new(ErrorCode::HandlerFailed, "first matcher wins"),
        )
        .fail_if_with(
            |state: &u64| *state == 1,
            |_state: &u64| CatgaError::new(ErrorCode::Internal, "second matcher loses"),
        ),
    );
    let mut state = 0_u64;
    let error = custom
        .run(&mut state)
        .await
        .expect_err("the first matching failure condition supplies the error");
    assert_eq!(error.code(), ErrorCode::HandlerFailed);
    assert_eq!(error.message(), "first matcher wins");
    Ok(())
}

#[tokio::test]
async fn dsl_query_step_into_state_stores_passing_responses() -> CatgaResult<()> {
    let flow = DslFlow::new()
        .step(
            DslStep::query(|_state: &u64| Box::pin(async move { Ok(5_u32) }))
                .into_state(|state: &mut u64, response: u32| *state += u64::from(response)),
        )
        .step(
            DslStep::query(|_state: &u64| Box::pin(async move { Ok(7_u32) }))
                .fail_if_response(|response: &u32| *response == 7)
                .discard(),
        )
        .step(
            DslStep::query(|_state: &u64| {
                Box::pin(async move {
                    Err::<u32, CatgaError>(CatgaError::new(ErrorCode::Transient, "query down"))
                })
            })
            .optional()
            .discard(),
        );

    let mut state = 0_u64;
    let error = flow
        .run(&mut state)
        .await
        .expect_err("fail_if_response rejects the second query");
    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(error.message(), "DSL response failure condition matched");
    assert_eq!(state, 5, "the first query stored its response");

    let optional_only = DslFlow::new().step(
        DslStep::query(|_state: &u64| {
            Box::pin(async move {
                Err::<u32, CatgaError>(CatgaError::new(ErrorCode::Transient, "query down"))
            })
        })
        .optional()
        .discard(),
    );
    let mut state = 0_u64;
    optional_only.run(&mut state).await?;
    assert_eq!(state, 0, "an optional failed query is skipped");
    Ok(())
}

#[tokio::test]
async fn dsl_lifecycle_observers_receive_events_in_order() -> CatgaResult<()> {
    let first = Arc::new(RecordingObserver::default());
    let second = Arc::new(RecordingObserver::default());
    let flow = DslFlow::new()
        .with_lifecycle_observer(first.clone())
        .with_lifecycle_observer(second.clone())
        .action(|_state: &mut u64| Box::pin(async move { Ok(()) }))
        .action(|_state: &mut u64| Box::pin(async move { Ok(()) }));

    let mut state = 0_u64;
    flow.run(&mut state).await?;
    let expected = vec![
        "step-ok:0".to_string(),
        "step-ok:1".to_string(),
        "flow-ok".to_string(),
    ];
    assert_eq!(first.events(), expected);
    assert_eq!(
        second.events(),
        expected,
        "observers run in registration order"
    );
    Ok(())
}

#[tokio::test]
async fn dsl_lifecycle_observer_sees_step_and_flow_failures() -> CatgaResult<()> {
    let observer = Arc::new(RecordingObserver::default());
    let flow = DslFlow::new()
        .with_lifecycle_observer(observer.clone())
        .action(|_state: &mut u64| Box::pin(async move { Ok(()) }))
        .action(|_state: &mut u64| {
            Box::pin(async move { Err(CatgaError::new(ErrorCode::Internal, "boom")) })
        });

    let mut state = 0_u64;
    let error = flow
        .run(&mut state)
        .await
        .expect_err("the failing step ends the flow");
    assert_eq!(error.code(), ErrorCode::Internal);
    assert_eq!(
        observer.events(),
        vec![
            "step-ok:0".to_string(),
            "step-err:1:Internal".to_string(),
            "flow-err:Internal".to_string(),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn dsl_step_succeeded_hook_error_aborts_the_flow_unchanged() -> CatgaResult<()> {
    let observer = Arc::new(RecordingObserver::default());
    let second_step = Arc::new(AtomicUsize::new(0));
    let second_marker = Arc::clone(&second_step);
    let hooks = DslFlowLifecycleHooks::new().on_step_succeeded(|_state: &u64, _index: usize| {
        Box::pin(async move { Err(CatgaError::new(ErrorCode::HandlerFailed, "hook blew up")) })
    });
    let flow = DslFlow::new()
        .with_lifecycle_observer(observer.clone())
        .with_lifecycle_hooks(hooks)
        .action(|_state: &mut u64| Box::pin(async move { Ok(()) }))
        .action(move |_state: &mut u64| {
            let second = Arc::clone(&second_marker);
            Box::pin(async move {
                second.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        });

    let mut state = 0_u64;
    let error = flow
        .run(&mut state)
        .await
        .expect_err("a hook error is returned unchanged");
    assert_eq!(error.code(), ErrorCode::HandlerFailed);
    assert_eq!(error.message(), "hook blew up");
    assert_eq!(
        second_step.load(Ordering::SeqCst),
        0,
        "later steps never run"
    );
    assert_eq!(
        observer.events(),
        vec!["step-ok:0".to_string()],
        "a hook error is not converted into another lifecycle event"
    );
    Ok(())
}

#[tokio::test]
async fn dsl_flow_failed_hook_observes_the_original_error() -> CatgaResult<()> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let hook_seen = Arc::clone(&seen);
    let hooks =
        DslFlowLifecycleHooks::new().on_flow_failed(move |_state: &u64, error: &CatgaError| {
            let seen = Arc::clone(&hook_seen);
            let code = error.code();
            Box::pin(async move {
                seen.lock().expect("hook lock").push(format!("{code:?}"));
                Ok(())
            })
        });
    let flow = DslFlow::new()
        .with_lifecycle_hooks(hooks)
        .action(|_state: &mut u64| {
            Box::pin(async move { Err(CatgaError::new(ErrorCode::Internal, "original")) })
        });

    let mut state = 0_u64;
    let error = flow
        .run(&mut state)
        .await
        .expect_err("the original step error is preserved");
    assert_eq!(error.code(), ErrorCode::Internal);
    assert_eq!(error.message(), "original");
    assert_eq!(
        seen.lock().expect("hook lock").as_slice(),
        &["Internal".to_string()],
        "the flow_failed hook ran exactly once with the original error"
    );
    Ok(())
}

#[test]
fn flow_throttle_rejects_a_zero_limit() {
    let error = FlowThrottle::new(0)
        .err()
        .expect("a zero permit throttle is invalid");
    assert_eq!(error.code(), ErrorCode::Validation);
}

#[tokio::test]
async fn dsl_throttle_serializes_actions_across_parallel_branches() -> CatgaResult<()> {
    let throttle = FlowThrottle::new(1)?;
    let in_flight = Arc::new(AtomicUsize::new(0));
    let max_in_flight = Arc::new(AtomicUsize::new(0));

    let mut branches = Vec::new();
    for _ in 0..2 {
        let throttle = throttle.clone();
        let in_flight = Arc::clone(&in_flight);
        let max_in_flight = Arc::clone(&max_in_flight);
        branches.push(DslFlow::new().throttle(throttle, move |state: &mut u64| {
            let in_flight = Arc::clone(&in_flight);
            let max_in_flight = Arc::clone(&max_in_flight);
            Box::pin(async move {
                let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                max_in_flight.fetch_max(current, Ordering::SeqCst);
                tokio::task::yield_now().await;
                in_flight.fetch_sub(1, Ordering::SeqCst);
                *state = 1;
                Ok(())
            })
        }));
    }
    let flow = DslFlow::new().parallel(branches, |state: &mut u64, results: Vec<u64>| {
        *state = results.into_iter().sum();
        Ok(())
    });

    let mut state = 0_u64;
    flow.run(&mut state).await?;
    assert_eq!(state, 2, "both branches completed");
    assert_eq!(
        max_in_flight.load(Ordering::SeqCst),
        1,
        "one permit serializes throttled actions"
    );
    Ok(())
}
