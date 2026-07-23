use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use catga_core::{CatgaError, ErrorCode};
use catga_flow::{DslFlow, Flow};

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
