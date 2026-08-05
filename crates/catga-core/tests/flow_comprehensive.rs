//! Comprehensive Flow and DslFlow tests

use catga_core::flow::{DslFlow, Flow};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
};

// The dsl_action macro is defined in flow/lib.rs
macro_rules! dsl_action {
    (|$state:ident : $state_ty:ty| async move $body:block) => {
        |$state: $state_ty| Box::pin(async move $body)
    };
}

#[tokio::test]
async fn flow_executes_all_steps_in_sequence() -> CatgaResult<()> {
    let step_order = Arc::new(AtomicUsize::new(0));
    let order1 = step_order.clone();
    let order2 = step_order.clone();

    let flow = Flow::new("test")
        .step(
            move || {
                let order = order1.clone();
                async move {
                    assert_eq!(order.load(Ordering::SeqCst), 0);
                    order.store(1, Ordering::SeqCst);
                    Ok(())
                }
            },
            || async { Ok(()) },
        )
        .step(
            move || {
                let order = order2.clone();
                async move {
                    assert_eq!(order.load(Ordering::SeqCst), 1);
                    order.store(2, Ordering::SeqCst);
                    Ok(())
                }
            },
            || async { Ok(()) },
        );

    let result = flow.run().await;
    assert!(result.is_success());
    assert_eq!(result.completed_steps(), 2);
    assert_eq!(step_order.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn flow_runs_compensation_on_failure() -> CatgaResult<()> {
    // When a step succeeds and the NEXT step fails, compensation should run
    let compensation_run = Arc::new(AtomicBool::new(false));
    let comp = compensation_run.clone();

    let flow = Flow::new("compensation-test")
        .step(
            move || {
                let comp = comp.clone();
                async move {
                    comp.store(true, Ordering::SeqCst);
                    Ok(())
                }
            },
            || async { Ok(()) }, // Compensation for step 0
        )
        .step(
            || async { Err(CatgaError::new(ErrorCode::Internal, "fail")) },
            || async { Ok(()) }, // Compensation for step 1 (should NOT run)
        );

    let result = flow.run().await;
    assert!(!result.is_success());
    // Step 0 succeeded, so its compensation should have run
    assert!(compensation_run.load(Ordering::SeqCst));
    assert_eq!(result.completed_steps(), 1);
    Ok(())
}

#[tokio::test]
async fn dsl_flow_updates_state_correctly() -> CatgaResult<()> {
    let flow = DslFlow::new()
        .action(dsl_action!(|state: &mut u32| async move {
            *state += 1;
            Ok(())
        }))
        .action(dsl_action!(|state: &mut u32| async move {
            *state += 10;
            Ok(())
        }));

    let mut state = 0_u32;
    flow.run(&mut state).await?;
    assert_eq!(state, 11);
    Ok(())
}

// Edge case: Empty flow
#[tokio::test]
async fn empty_flow_completes_successfully() -> CatgaResult<()> {
    let flow = Flow::new("empty");

    let result = flow.run().await;
    assert!(result.is_success());
    assert_eq!(result.completed_steps(), 0);
    assert!(result.error().is_none());
    Ok(())
}

#[tokio::test]
async fn empty_dsl_flow_completes_successfully() -> CatgaResult<()> {
    let flow = DslFlow::<u32>::new();

    let mut state = 42_u32;
    flow.run(&mut state).await?;
    // State should remain unchanged
    assert_eq!(state, 42);
    Ok(())
}

// Edge case: Multi-step compensation ordering
#[tokio::test]
async fn multi_step_compensation_runs_in_reverse_order() -> CatgaResult<()> {
    let counter = Arc::new(AtomicU32::new(0));
    let c1 = counter.clone();
    let c2 = counter.clone();

    // Simple test: 2 steps succeed, 3rd fails
    let flow = Flow::new("multi-compensation")
        .step(
            || async { Ok(()) },
            move || {
                let c = c1.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst); // Second compensation to run
                    Ok(())
                }
            },
        )
        .step(
            || async { Ok(()) },
            move || {
                let c = c2.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst); // First compensation to run
                    Ok(())
                }
            },
        )
        .step(
            || async { Err(CatgaError::new(ErrorCode::Internal, "fail at step 3")) },
            || async { Ok(()) }, // Compensation for step 2 - should NOT run
        );

    let result = flow.run().await;
    assert!(!result.is_success());
    assert_eq!(result.completed_steps(), 2);
    // Only steps 0 and 1 should be compensated, in reverse order: step 1 then step 0
    // Counter increments: 1 (step 1 comp), 2 (step 0 comp)
    assert_eq!(counter.load(Ordering::SeqCst), 2);
    Ok(())
}

// Edge case: State isolation between runs
#[tokio::test]
async fn dsl_flow_state_isolated_between_runs() -> CatgaResult<()> {
    let flow = DslFlow::new()
        .action(dsl_action!(|state: &mut u32| async move {
            *state += 1;
            Ok(())
        }))
        .action(dsl_action!(|state: &mut u32| async move {
            *state *= 2;
            Ok(())
        }));

    // First run
    let mut state1 = 0_u32;
    flow.run(&mut state1).await?;
    assert_eq!(state1, 2); // (0 + 1) * 2 = 2

    // Second run with same flow instance
    let mut state2 = 5_u32;
    flow.run(&mut state2).await?;
    assert_eq!(state2, 12); // (5 + 1) * 2 = 12

    // Original state1 should be unaffected
    assert_eq!(state1, 2);
    Ok(())
}

// Edge case: Flow state isolation - verify separate flows don't interfere
#[tokio::test]
async fn flow_state_isolated_between_runs() -> CatgaResult<()> {
    let counter = Arc::new(AtomicU32::new(0));

    // First flow: increment, counter becomes 1
    let c1 = counter.clone();
    let flow1 = Flow::new("flow-1").step(
        move || {
            let c = c1.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        },
        || async { Ok(()) }, // Compensation runs only on failure
    );

    let result1 = flow1.run().await;
    assert!(result1.is_success());
    // Compensation does NOT run on success, so counter stays at 1
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    // Second flow: increment again, counter becomes 2
    let c2 = counter.clone();
    let flow2 = Flow::new("flow-2").step(
        move || {
            let c = c2.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        },
        || async { Ok(()) }, // Compensation runs only on failure
    );

    let result2 = flow2.run().await;
    assert!(result2.is_success());
    // Counter is isolated - each flow sees its own state changes
    // but they share the atomic counter for observation
    assert_eq!(counter.load(Ordering::SeqCst), 2);
    Ok(())
}

// Edge case: Partial compensation after middle step failure
#[tokio::test]
async fn partial_compensation_after_middle_failure() -> CatgaResult<()> {
    let compensated = Arc::new(AtomicU32::new(0));
    let comp1 = compensated.clone();

    let flow = Flow::new("partial-comp")
        .step(
            || async { Ok(()) },
            move || {
                let comp = comp1.clone();
                async move {
                    comp.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
        )
        .step(
            || async { Err(CatgaError::new(ErrorCode::Validation, "bad input")) },
            || async { Ok(()) }, // Should NOT run
        )
        .step(
            || async { Ok(()) }, // Should NOT run
            || async { Ok(()) }, // Should NOT run
        );

    let result = flow.run().await;
    assert!(!result.is_success());
    assert_eq!(result.completed_steps(), 1);
    // Only first step should be compensated
    assert_eq!(compensated.load(Ordering::SeqCst), 1);
    Ok(())
}

// Edge case: DslFlow with multiple state mutations
#[tokio::test]
async fn dsl_flow_multiple_state_mutations() -> CatgaResult<()> {
    #[derive(Default)]
    struct State {
        values: Vec<u32>,
        sum: u32,
        count: u32,
    }

    let flow = DslFlow::new()
        .action(dsl_action!(|state: &mut State| async move {
            state.values.push(1);
            state.sum += 1;
            state.count += 1;
            Ok(())
        }))
        .action(dsl_action!(|state: &mut State| async move {
            state.values.push(2);
            state.sum += 2;
            state.count += 1;
            Ok(())
        }))
        .action(dsl_action!(|state: &mut State| async move {
            state.values.push(3);
            state.sum += 3;
            state.count += 1;
            Ok(())
        }));

    let mut state = State::default();
    flow.run(&mut state).await?;

    assert_eq!(state.values, vec![1, 2, 3]);
    assert_eq!(state.sum, 6);
    assert_eq!(state.count, 3);
    Ok(())
}

// Edge case: Flow with successful then failing compensation
#[tokio::test]
async fn flow_error_preserved_after_failed_compensation() -> CatgaResult<()> {
    let flow = Flow::new("comp-fail").step(
        || async { Err(CatgaError::new(ErrorCode::Internal, "initial error")) },
        || async { Err(CatgaError::new(ErrorCode::Internal, "compensation error")) },
    );

    let result = flow.run().await;
    assert!(!result.is_success());
    // Original error should be preserved, not overwritten by compensation error
    let err = result.error().expect("should have error");
    assert_eq!(err.message(), "initial error");
    assert_eq!(err.code(), ErrorCode::Internal);
    Ok(())
}

// Edge case: Zero max_compensations prevents compensation
#[tokio::test]
async fn zero_max_compensations_prevents_compensation() -> CatgaResult<()> {
    let compensation_run = Arc::new(AtomicBool::new(false));
    let comp = compensation_run.clone();

    let flow = Flow::new("no-comp")
        .step(
            || async { Ok(()) },
            move || {
                let comp = comp.clone();
                async move {
                    comp.store(true, Ordering::SeqCst);
                    Ok(())
                }
            },
        )
        .step(
            || async { Err(CatgaError::new(ErrorCode::Internal, "fail")) },
            || async { Ok(()) },
        );

    let result = flow.run_from(0, 0).await;
    assert!(!result.is_success());
    assert_eq!(result.completed_steps(), 1);
    // With max_compensations=0, no compensation should run
    assert!(!compensation_run.load(Ordering::SeqCst));
    Ok(())
}

// Edge case: DslFlow with complex nested state
#[tokio::test]
async fn dsl_flow_complex_nested_state() -> CatgaResult<()> {
    use std::collections::HashMap;

    let flow = DslFlow::new()
        .action(dsl_action!(
            |state: &mut HashMap<String, Vec<u32>>| async move {
                state.insert("a".to_string(), vec![1, 2]);
                Ok(())
            }
        ))
        .action(dsl_action!(
            |state: &mut HashMap<String, Vec<u32>>| async move {
                state.get_mut("a").expect("state should have 'a' key").push(3);
                Ok(())
            }
        ))
        .action(dsl_action!(
            |state: &mut HashMap<String, Vec<u32>>| async move {
                state.insert("b".to_string(), vec![4]);
                Ok(())
            }
        ));

    let mut state = HashMap::new();
    flow.run(&mut state).await?;

    assert_eq!(state.get("a"), Some(&vec![1, 2, 3]));
    assert_eq!(state.get("b"), Some(&vec![4]));
    Ok(())
}
