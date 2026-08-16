//! Strict contracts for the request-chain behaviors: retry backoff bounds,
//! timeout cancellation, correlation propagation, and observability wrappers.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use catga_core::{
    CatgaError, CorrelationBehavior, ErrorCode, Mediator, Pipeline, RetryBehavior, RetryJitter,
    TimeoutBehavior, current_correlation_id,
};

#[path = "support/behavior_support.rs"]
mod behavior_support;

use behavior_support::{Req, req_mediator};

/// Builds a mediator whose handler fails `failures` times before succeeding,
/// recording each attempt. It can also panic once when `panics_once` is set.
fn attempt_mediator(failures: usize, code: ErrorCode) -> (Arc<Mediator>, Arc<AtomicUsize>) {
    let attempts = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&attempts);
    let mediator = req_mediator(catga_core::request_handler(move |req: Req| {
        let observed = Arc::clone(&observed);
        async move {
            if observed.fetch_add(1, Ordering::SeqCst) < failures {
                Err(CatgaError::new(code, "attempt failed"))
            } else {
                Ok(req.id)
            }
        }
    }));
    (mediator, attempts)
}

#[tokio::test]
async fn retry_behavior_exhausts_additional_attempts_with_zero_delay() {
    let (mediator, attempts) = attempt_mediator(2, ErrorCode::Transient);
    let pipeline = Pipeline::<Req>::new().with(RetryBehavior::with_jitter(
        3,
        Duration::ZERO,
        RetryJitter::fixed(Duration::ZERO),
    ));
    let value = mediator
        .send_with(
            Req {
                id: 11,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("the third attempt succeeds");
    assert_eq!(value, 11);
    assert_eq!(attempts.load(Ordering::SeqCst), 3);

    // Exhausted retries surface the final handler error unchanged.
    let (mediator, attempts) = attempt_mediator(usize::MAX, ErrorCode::Transient);
    let error = mediator
        .send_with(
            Req {
                id: 11,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("retries exhaust");
    assert_eq!(error.code(), ErrorCode::Transient);
    assert_eq!(attempts.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn retry_behavior_never_retries_cancelled_or_terminal_errors() {
    let (mediator, attempts) = attempt_mediator(usize::MAX, ErrorCode::Cancelled);
    let pipeline = Pipeline::<Req>::new().with(RetryBehavior::with_jitter(
        5,
        Duration::ZERO,
        RetryJitter::fixed(Duration::ZERO),
    ));
    let error = mediator
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("cancelled errors surface");
    assert_eq!(error.code(), ErrorCode::Cancelled);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    let (mediator, attempts) = attempt_mediator(usize::MAX, ErrorCode::Validation);
    let error = mediator
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("terminal errors surface");
    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn retry_behavior_applies_bounded_backoff_and_saturates_growth() {
    // A nonzero delay exercises the sleeping branch without slowing the suite.
    let (mediator, attempts) = attempt_mediator(1, ErrorCode::Timeout);
    let pipeline = Pipeline::<Req>::new().with(RetryBehavior::with_jitter(
        2,
        Duration::from_micros(10),
        RetryJitter::fixed(Duration::from_micros(10)),
    ));
    mediator
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("the retry succeeds");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    // Far more retries than the shift width saturate the multiplier instead of
    // overflowing, and the zero initial delay keeps the test instant.
    let (mediator, attempts) = attempt_mediator(2, ErrorCode::Transient);
    let behavior =
        RetryBehavior::with_jitter(72, Duration::ZERO, RetryJitter::fixed(Duration::ZERO));
    assert!(matches!(
        behavior.jitter_policy(),
        RetryJitter::Fixed { .. }
    ));
    let pipeline = Pipeline::<Req>::new().with(behavior);
    mediator
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("the saturated retry path succeeds");
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn timeout_behavior_cancels_slow_handlers_and_passes_fast_ones() {
    let slow = req_mediator(catga_core::request_handler(move |_: Req| async move {
        tokio::time::sleep(Duration::from_secs(60)).await;
        Ok(1_u64)
    }));
    let pipeline = Pipeline::<Req>::new().with(TimeoutBehavior::new(Duration::from_millis(20)));
    let error = slow
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the slow handler times out");
    assert_eq!(error.code(), ErrorCode::Timeout);

    let (fast, _) = attempt_mediator(0, ErrorCode::Transient);
    let value = fast
        .send_with(
            Req {
                id: 9,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("a fast handler passes");
    assert_eq!(value, 9);
}

#[tokio::test]
async fn correlation_behavior_scopes_the_correlation_identity() {
    let observed = Arc::new(std::sync::Mutex::new(None::<u64>));
    let slot = Arc::clone(&observed);
    let mediator = req_mediator(catga_core::request_handler(move |req: Req| {
        let slot = Arc::clone(&slot);
        async move {
            *slot.lock().expect("observation slot lock") = current_correlation_id();
            Ok(req.id)
        }
    }));
    let pipeline = Pipeline::<Req>::new().with(CorrelationBehavior);

    mediator
        .send_with(
            Req {
                id: 5,
                correlation: Some(77),
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("the request succeeds");
    assert_eq!(
        *observed.lock().expect("observation slot lock"),
        Some(77),
        "the declared correlation id is scoped"
    );

    mediator
        .send_with(
            Req {
                id: 42,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("the request succeeds");
    assert_eq!(
        *observed.lock().expect("observation slot lock"),
        Some(42),
        "without a correlation id the message id is used"
    );

    // The scope does not leak beyond the request.
    assert_eq!(current_correlation_id(), None);
}
