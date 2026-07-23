use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use catga_core::{CatgaError, ErrorCode};
use catga_flow::{DslFlow, Flow, dsl_action};

#[tokio::test]
async fn local_flow_compensates_completed_steps_in_reverse_order() {
    let trace = Arc::new(AtomicUsize::new(0));
    let reserve = Arc::clone(&trace);
    let release = Arc::clone(&trace);
    let charge = Arc::clone(&trace);

    let result = Flow::new("reserve")
        .step(
            move || {
                let trace = Arc::clone(&reserve);
                async move {
                    assert_eq!(trace.fetch_add(1, Ordering::Relaxed), 0);
                    Ok(())
                }
            },
            move || {
                let trace = Arc::clone(&release);
                async move {
                    assert_eq!(trace.fetch_add(1, Ordering::Relaxed), 2);
                    Ok(())
                }
            },
        )
        .step(
            move || {
                let trace = Arc::clone(&charge);
                async move {
                    assert_eq!(trace.fetch_add(1, Ordering::Relaxed), 1);
                    Err(CatgaError::new(ErrorCode::Transient, "charge"))
                }
            },
            || async { Ok(()) },
        )
        .run()
        .await;

    assert!(!result.is_success());
    assert_eq!(result.error().unwrap().message(), "charge");
    assert_eq!(trace.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn empty_local_flow_completes_successfully() {
    let result = Flow::new("empty").run().await;

    assert!(result.is_success());
    assert_eq!(result.completed_steps(), 0);
}

#[tokio::test]
async fn dsl_flow_runs_only_the_selected_nested_branch_against_one_state() {
    let mut state = Vec::new();
    let then_branch = DslFlow::new().action(|state: &mut Vec<&'static str>| {
        Box::pin(async move {
            state.push("then");
            Ok(())
        })
    });
    let else_branch = DslFlow::new().action(|state: &mut Vec<&'static str>| {
        Box::pin(async move {
            state.push("else");
            Ok(())
        })
    });
    let flow = DslFlow::new()
        .action(|state: &mut Vec<&'static str>| {
            Box::pin(async move {
                state.push("start");
                Ok(())
            })
        })
        .if_else(|state| state.len() == 1, then_branch, else_branch);

    flow.run(&mut state).await.unwrap();
    assert_eq!(state, ["start", "then"]);
}

#[tokio::test]
async fn dsl_flow_stops_before_later_steps_after_a_branch_error() {
    let mut state = Vec::new();
    let failed = DslFlow::new().action(|_: &mut Vec<&'static str>| {
        Box::pin(async { Err(CatgaError::new(ErrorCode::Validation, "branch failed")) })
    });
    let flow = DslFlow::new()
        .if_else(|_| true, failed, DslFlow::new())
        .action(|state: &mut Vec<&'static str>| {
            Box::pin(async move {
                state.push("after");
                Ok(())
            })
        });

    assert_eq!(
        flow.run(&mut state).await.unwrap_err().code(),
        ErrorCode::Validation
    );
    assert!(state.is_empty());
}

#[tokio::test]
async fn dsl_action_macro_hides_the_borrowed_future_boxing() {
    let mut value = 0_u32;
    DslFlow::new()
        .action(dsl_action!(|value: &mut u32| async move {
            *value += 1;
            Ok(())
        }))
        .run(&mut value)
        .await
        .unwrap();
    assert_eq!(value, 1);
}

#[derive(Clone, Debug)]
struct ParallelState {
    value: u32,
}

#[tokio::test]
async fn dsl_flow_parallel_runs_isolated_branches_concurrently_and_merges_in_definition_order() {
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let first_barrier = Arc::clone(&barrier);
    let second_barrier = Arc::clone(&barrier);
    let first = DslFlow::new().action(move |state: &mut ParallelState| {
        let barrier = Arc::clone(&first_barrier);
        Box::pin(async move {
            state.value = 10;
            barrier.wait().await;
            tokio::task::yield_now().await;
            Ok(())
        })
    });
    let second = DslFlow::new().action(move |state: &mut ParallelState| {
        let barrier = Arc::clone(&second_barrier);
        Box::pin(async move {
            state.value = 20;
            barrier.wait().await;
            Ok(())
        })
    });
    let flow = DslFlow::new().parallel([first, second], |state, branch_states| {
        state.value = branch_states
            .iter()
            .fold(0, |value, branch| value * 100 + branch.value);
        Ok(())
    });
    let mut state = ParallelState { value: 5 };

    tokio::time::timeout(std::time::Duration::from_secs(1), flow.run(&mut state))
        .await
        .expect("parallel branches must reach each other")
        .unwrap();

    assert_eq!(state.value, 1020);
}

#[tokio::test]
async fn dsl_flow_parallel_keeps_the_original_state_when_a_branch_fails() {
    let merge_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flow = DslFlow::new().parallel(
        [
            DslFlow::new().action(dsl_action!(|state: &mut ParallelState| async move {
                state.value = 10;
                Ok(())
            })),
            DslFlow::new().action(dsl_action!(|state: &mut ParallelState| async move {
                let _ = state;
                Err(CatgaError::new(ErrorCode::Validation, "parallel failed"))
            })),
        ],
        {
            let merge_called = Arc::clone(&merge_called);
            move |_, _: Vec<ParallelState>| {
                merge_called.store(true, Ordering::Relaxed);
                Ok(())
            }
        },
    );
    let mut state = ParallelState { value: 5 };

    assert_eq!(
        flow.run(&mut state).await.unwrap_err().code(),
        ErrorCode::Validation
    );
    assert_eq!(state.value, 5);
    assert!(!merge_called.load(Ordering::Relaxed));
}
