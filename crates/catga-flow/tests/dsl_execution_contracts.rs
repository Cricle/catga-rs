//! Process-local DSL branch, retry, and decorator contracts.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{DslFlow, DslStep};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct State {
    value: u32,
    enabled: bool,
}

#[tokio::test]
async fn dsl_step_decorators_skip_optional_failures_and_keep_response_writes_atomic()
-> CatgaResult<()> {
    let skipped_calls = Arc::new(AtomicUsize::new(0));
    let skipped = Arc::clone(&skipped_calls);
    let flow = DslFlow::new()
        .step(
            DslStep::action(move |_: &mut State| {
                let skipped = Arc::clone(&skipped);
                Box::pin(async move {
                    skipped.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            })
            .only_when(|state| state.enabled),
        )
        .step(
            DslStep::action(|_: &mut State| {
                Box::pin(async {
                    Err(CatgaError::new(
                        ErrorCode::Transient,
                        "optional operation failed",
                    ))
                })
            })
            .optional(),
        )
        .step(
            DslStep::query(|state: &State| {
                let value = state.value;
                Box::pin(async move { Ok(value.saturating_add(7)) })
            })
            .only_when(|state| !state.enabled)
            .into_state(|state, value| state.value = value),
        );
    let mut state = State::default();

    flow.run(&mut state).await?;
    assert_eq!(skipped_calls.load(Ordering::SeqCst), 0);
    assert_eq!(state.value, 7);

    let rejecting = DslFlow::new().step(
        DslStep::query(|_: &State| Box::pin(async { Ok(9_u32) }))
            .fail_if_response(|response| *response == 9)
            .into_state(|state, value| state.value = value),
    );
    let error = rejecting
        .run(&mut state)
        .await
        .expect_err("rejected response must not be stored");
    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(state.value, 7);
    Ok(())
}

#[tokio::test]
async fn dsl_retry_and_timeout_preserve_error_classification_and_attempt_bounds() -> CatgaResult<()>
{
    let attempts = Arc::new(AtomicUsize::new(0));
    let retry_attempts = Arc::clone(&attempts);
    let retrying = DslFlow::new().retry(2, Duration::ZERO, move |state: &mut State| {
        let retry_attempts = Arc::clone(&retry_attempts);
        Box::pin(async move {
            let attempt = retry_attempts.fetch_add(1, Ordering::SeqCst);
            if attempt < 2 {
                return Err(CatgaError::new(ErrorCode::Transient, "retry me"));
            }
            state.value = state.value.saturating_add(1);
            Ok(())
        })
    });
    let mut state = State::default();
    retrying.run(&mut state).await?;
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert_eq!(state.value, 1);

    let non_retry_attempts = Arc::new(AtomicUsize::new(0));
    let permanent_attempts = Arc::clone(&non_retry_attempts);
    let permanent = DslFlow::new().retry(3, Duration::ZERO, move |_: &mut State| {
        let permanent_attempts = Arc::clone(&permanent_attempts);
        Box::pin(async move {
            permanent_attempts.fetch_add(1, Ordering::SeqCst);
            Err(CatgaError::new(
                ErrorCode::Validation,
                "do not retry validation",
            ))
        })
    });
    let error = permanent
        .run(&mut state)
        .await
        .expect_err("non-transient failure returns immediately");
    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(non_retry_attempts.load(Ordering::SeqCst), 1);

    let timeout = DslFlow::new().timeout(Duration::ZERO, |_: &mut State| {
        Box::pin(std::future::pending::<CatgaResult<()>>())
    });
    let error = timeout
        .run(&mut state)
        .await
        .expect_err("expired timeout stops the flow");
    assert_eq!(error.code(), ErrorCode::Timeout);
    Ok(())
}

#[tokio::test]
async fn dsl_branches_parallel_and_when_any_merge_only_the_selected_state() -> CatgaResult<()> {
    let then_branch = DslFlow::new().action(|state: &mut State| {
        Box::pin(async move {
            state.value = state.value.saturating_add(1);
            Ok(())
        })
    });
    let else_branch = DslFlow::new().action(|state: &mut State| {
        Box::pin(async move {
            state.value = state.value.saturating_add(10);
            Ok(())
        })
    });
    let default_branch = DslFlow::new().action(|state: &mut State| {
        Box::pin(async move {
            state.value = state.value.saturating_add(100);
            Ok(())
        })
    });
    let duplicate_old = DslFlow::new().action(|state: &mut State| {
        Box::pin(async move {
            state.value = 1_000;
            Ok(())
        })
    });
    let duplicate_new = DslFlow::new().action(|state: &mut State| {
        Box::pin(async move {
            state.value = state.value.saturating_add(20);
            Ok(())
        })
    });
    let parallel_left = DslFlow::new().action(|state: &mut State| {
        Box::pin(async move {
            state.value = 2;
            Ok(())
        })
    });
    let parallel_right = DslFlow::new().action(|state: &mut State| {
        Box::pin(async move {
            state.value = 3;
            Ok(())
        })
    });
    let failed_any = DslFlow::new().action(|_: &mut State| {
        Box::pin(async { Err(CatgaError::new(ErrorCode::Transient, "not the winner")) })
    });
    let successful_any = DslFlow::new().action(|state: &mut State| {
        Box::pin(async move {
            state.value = 50;
            Ok(())
        })
    });
    let flow = DslFlow::new()
        .if_else(|state: &State| state.enabled, then_branch, else_branch)
        .match_on(
            |state: &State| state.value,
            [(10, duplicate_old), (10, duplicate_new)],
            default_branch,
        )
        .parallel([parallel_left, parallel_right], |state, branches| {
            state.value = branches.into_iter().map(|branch| branch.value).sum();
            Ok(())
        })
        .when_any([failed_any, successful_any], |state, winner| {
            state.value = winner.value;
            Ok(())
        });
    let mut state = State::default();

    flow.run(&mut state).await?;
    assert_eq!(state.value, 50);
    Ok(())
}
