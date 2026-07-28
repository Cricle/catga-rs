//! Strict edge and failure contracts for the process-local DSL.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{
    DslFlow, DslFlowLifecycleEvent, DslFlowLifecycleHooks, DslFlowLifecycleObserver, DslStep,
    FlowThrottle, MAX_DSL_PARALLEL_BRANCHES,
};
use futures::{StreamExt, stream};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct State {
    value: u32,
    items: Vec<u32>,
    errors: Vec<(usize, ErrorCode)>,
}

#[derive(Default)]
struct EventLog(Mutex<Vec<DslFlowLifecycleEvent>>);

impl DslFlowLifecycleObserver for EventLog {
    fn observe(&self, event: &DslFlowLifecycleEvent) {
        self.0
            .lock()
            .expect("event log lock is available")
            .push(event.clone());
    }
}

#[tokio::test]
async fn decorators_short_circuit_conditions_preserve_cancellation_and_keep_writes_atomic()
-> CatgaResult<()> {
    let second_condition_calls = Arc::new(AtomicUsize::new(0));
    let second_condition = Arc::clone(&second_condition_calls);
    let action_calls = Arc::new(AtomicUsize::new(0));
    let action_calls_for_step = Arc::clone(&action_calls);
    let skipped_query_calls = Arc::new(AtomicUsize::new(0));
    let skipped_query = Arc::clone(&skipped_query_calls);
    let flow = DslFlow::new()
        .step(
            DslStep::action(move |_: &mut State| {
                let calls = Arc::clone(&action_calls_for_step);
                Box::pin(async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            })
            .only_when(|_: &State| false)
            .only_when(move |_: &State| {
                second_condition.fetch_add(1, Ordering::SeqCst);
                true
            }),
        )
        .step(
            DslStep::query(move |_: &State| {
                let calls = Arc::clone(&skipped_query);
                Box::pin(async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(99_u32)
                })
            })
            .only_when(|_: &State| false)
            .into_state(|state, value| state.value = value),
        )
        .step(
            DslStep::query(|_: &State| {
                Box::pin(async {
                    Err::<u32, _>(CatgaError::new(ErrorCode::Transient, "best effort"))
                })
            })
            .optional()
            .into_state(|state, value| state.value = value),
        )
        .step(
            DslStep::query(|_: &State| Box::pin(async { Ok(7_u32) }))
                .fail_if_response_with(
                    |response| *response == 7,
                    |_| CatgaError::new(ErrorCode::Conflict, "response is reserved"),
                )
                .into_state(|state, value| state.value = value),
        );
    let mut state = State::default();

    let error = flow
        .run(&mut state)
        .await
        .expect_err("a response validator must reject before state mutation");
    assert_eq!(error.code(), ErrorCode::Conflict);
    assert_eq!(action_calls.load(Ordering::SeqCst), 0);
    assert_eq!(second_condition_calls.load(Ordering::SeqCst), 0);
    assert_eq!(skipped_query_calls.load(Ordering::SeqCst), 0);
    assert_eq!(state.value, 0);

    let cancellation = DslFlow::new().step(
        DslStep::action(|_: &mut State| {
            Box::pin(async { Err(CatgaError::new(ErrorCode::Cancelled, "shutting down")) })
        })
        .optional(),
    );
    let error = cancellation
        .run(&mut state)
        .await
        .expect_err("optional must never swallow cancellation");
    assert_eq!(error.code(), ErrorCode::Cancelled);
    Ok(())
}

#[tokio::test]
async fn lifecycle_hook_errors_are_authoritative_and_stop_later_events() -> CatgaResult<()> {
    let events = Arc::new(EventLog::default());
    let later_steps = Arc::new(AtomicUsize::new(0));
    let later_steps_for_action = Arc::clone(&later_steps);
    let flow = DslFlow::new()
        .with_lifecycle_observer(Arc::clone(&events))
        .with_lifecycle_hooks(
            DslFlowLifecycleHooks::new()
                .on_step_succeeded(|_: &State, index| {
                    Box::pin(async move {
                        assert_eq!(index, 0);
                        Err(CatgaError::new(ErrorCode::Unavailable, "audit unavailable"))
                    })
                })
                .on_flow_failed(|_: &State, _| {
                    Box::pin(async move {
                        panic!("a succeeded hook error must not become a flow failure")
                    })
                }),
        )
        .action(|state: &mut State| {
            Box::pin(async move {
                state.value = 1;
                Ok(())
            })
        })
        .action(move |_: &mut State| {
            let later_steps = Arc::clone(&later_steps_for_action);
            Box::pin(async move {
                later_steps.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        });
    let mut state = State::default();

    let error = flow
        .run(&mut state)
        .await
        .expect_err("hook failure is returned unchanged");
    assert_eq!(error.code(), ErrorCode::Unavailable);
    assert_eq!(later_steps.load(Ordering::SeqCst), 0);
    assert_eq!(state.value, 1);
    assert!(matches!(
        events
            .0
            .lock()
            .expect("event log lock is available")
            .as_slice(),
        [DslFlowLifecycleEvent::StepSucceeded { step_index: 0 }]
    ));

    let flow = DslFlow::new()
        .with_lifecycle_hooks(
            DslFlowLifecycleHooks::new()
                .on_step_failed(|_: &State, _, _| {
                    Box::pin(async {
                        Err(CatgaError::new(
                            ErrorCode::Unavailable,
                            "failed hook unavailable",
                        ))
                    })
                })
                .on_flow_failed(|_: &State, _| {
                    Box::pin(async move {
                        panic!("failed step hook error must skip the flow failed hook")
                    })
                }),
        )
        .action(|_: &mut State| {
            Box::pin(async { Err(CatgaError::new(ErrorCode::Validation, "declined")) })
        });
    let error = flow
        .run(&mut State::default())
        .await
        .expect_err("failed-step hook error replaces step error");
    assert_eq!(error.code(), ErrorCode::Unavailable);
    Ok(())
}

#[tokio::test]
async fn collection_error_callbacks_and_concurrent_reductions_have_strict_boundaries()
-> CatgaResult<()> {
    let flow = DslFlow::new()
        .for_each_continue_on_error(
            |_: &State| vec![1_u32, 2, 3],
            |state, item| {
                Box::pin(async move {
                    state.items.push(item);
                    if item == 2 {
                        return Err(CatgaError::new(ErrorCode::Validation, "bad item"));
                    }
                    Ok(())
                })
            },
            |state, index, error| {
                Box::pin(async move {
                    state.errors.push((index, error.code()));
                    Err(CatgaError::new(
                        ErrorCode::Conflict,
                        "error handling rejected",
                    ))
                })
            },
        )
        .action(|state: &mut State| {
            Box::pin(async move {
                state.items.push(99);
                Ok(())
            })
        });
    let mut state = State::default();
    let error = flow
        .run(&mut state)
        .await
        .expect_err("callback failure must stop further items and steps");
    assert_eq!(error.code(), ErrorCode::Conflict);
    assert_eq!(state.items, [1, 2]);
    assert_eq!(state.errors, [(1, ErrorCode::Validation)]);

    let invalid = DslFlow::<State>::new().for_each_stream_concurrent(
        0,
        |_| stream::empty().boxed(),
        |_, _: u32| Box::pin(async { Ok(()) }),
        |_, ()| Ok(()),
    );
    assert!(matches!(invalid, Err(error) if error.code() == ErrorCode::Validation));

    let calls = Arc::new(AtomicUsize::new(0));
    let work_calls = Arc::clone(&calls);
    let all_work_started = Arc::new(tokio::sync::Notify::new());
    let release_work = Arc::new(tokio::sync::Notify::new());
    let concurrent = DslFlow::new()
        .for_each_stream_concurrent(
            3,
            |_| stream::iter([3_u32, 1, 2]).boxed(),
            {
                let all_work_started = Arc::clone(&all_work_started);
                let release_work = Arc::clone(&release_work);
                move |_: &State, item| {
                    let calls = Arc::clone(&work_calls);
                    let all_work_started = Arc::clone(&all_work_started);
                    let release_work = Arc::clone(&release_work);
                    Box::pin(async move {
                        if calls.fetch_add(1, Ordering::SeqCst) == 2 {
                            all_work_started.notify_one();
                        }
                        release_work.notified().await;
                        if item == 1 {
                            return Err(CatgaError::new(ErrorCode::Transient, "work failed"));
                        }
                        Ok(item * 10)
                    })
                }
            },
            |state, result| {
                state.items.push(result);
                Ok(())
            },
        )
        .expect("positive concurrency is valid");
    let concurrent = Arc::new(concurrent);
    let task = tokio::spawn({
        let concurrent = Arc::clone(&concurrent);
        async move {
            let mut state = State::default();
            let result = concurrent.run(&mut state).await;
            (result, state)
        }
    });
    all_work_started.notified().await;
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    release_work.notify_waiters();
    let (result, state) = task.await.expect("concurrent flow task joins");
    let error = result.expect_err("a batch work failure must prevent every batch reduction");
    assert_eq!(error.code(), ErrorCode::Transient);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert!(state.items.is_empty());
    Ok(())
}

#[tokio::test]
async fn parallel_fanout_limits_empty_when_any_and_shared_throttle_are_enforced() -> CatgaResult<()>
{
    assert!(matches!(
        FlowThrottle::new(0),
        Err(error) if error.code() == ErrorCode::Validation
    ));

    let branch_calls = Arc::new(AtomicUsize::new(0));
    let branches = (0..=MAX_DSL_PARALLEL_BRANCHES).map(|_| {
        let branch_calls = Arc::clone(&branch_calls);
        DslFlow::new().action(move |_: &mut State| {
            let branch_calls = Arc::clone(&branch_calls);
            Box::pin(async move {
                branch_calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        })
    });
    let oversized = DslFlow::new().parallel(branches, |_, _| Ok(()));
    let mut state = State::default();
    let error = oversized
        .run(&mut state)
        .await
        .expect_err("the retained 65th branch must be rejected before execution");
    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(branch_calls.load(Ordering::SeqCst), 0);

    let merge_called = Arc::new(AtomicBool::new(false));
    let merge_called_for_empty = Arc::clone(&merge_called);
    let empty_parallel = DslFlow::new().parallel(Vec::<DslFlow<State>>::new(), move |_, states| {
        merge_called_for_empty.store(states.is_empty(), Ordering::SeqCst);
        Ok(())
    });
    empty_parallel.run(&mut state).await?;
    assert!(merge_called.load(Ordering::SeqCst));

    let merge_called = Arc::new(AtomicBool::new(false));
    let merge_called_for_any = Arc::clone(&merge_called);
    let empty_any = DslFlow::new().when_any(Vec::<DslFlow<State>>::new(), move |_, _| {
        merge_called_for_any.store(true, Ordering::SeqCst);
        Ok(())
    });
    empty_any.run(&mut state).await?;
    assert!(!merge_called.load(Ordering::SeqCst));

    let throttle = FlowThrottle::new(1)?;
    let entered = Arc::new(AtomicUsize::new(0));
    let first_entered = Arc::new(tokio::sync::Notify::new());
    let release_first = Arc::new(tokio::sync::Notify::new());
    let flow = Arc::new(DslFlow::new().throttle(throttle, {
        let entered = Arc::clone(&entered);
        let first_entered = Arc::clone(&first_entered);
        let release_first = Arc::clone(&release_first);
        move |state: &mut State| {
            let entered = Arc::clone(&entered);
            let first_entered = Arc::clone(&first_entered);
            let release_first = Arc::clone(&release_first);
            Box::pin(async move {
                let position = entered.fetch_add(1, Ordering::SeqCst);
                if position == 0 {
                    first_entered.notify_one();
                    release_first.notified().await;
                }
                state.value = 1;
                Ok(())
            })
        }
    }));
    let first_flow = Arc::clone(&flow);
    let first = tokio::spawn(async move {
        let mut state = State::default();
        first_flow.run(&mut state).await?;
        Ok::<_, CatgaError>(state)
    });
    first_entered.notified().await;
    let second_flow = Arc::clone(&flow);
    let second = tokio::spawn(async move {
        let mut state = State::default();
        second_flow.run(&mut state).await?;
        Ok::<_, CatgaError>(state)
    });
    tokio::task::yield_now().await;
    assert_eq!(entered.load(Ordering::SeqCst), 1);
    release_first.notify_one();
    assert_eq!(first.await.expect("first task joins")?.value, 1);
    assert_eq!(second.await.expect("second task joins")?.value, 1);
    assert_eq!(entered.load(Ordering::SeqCst), 2);
    Ok(())
}
