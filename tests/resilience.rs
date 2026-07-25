//! Explicit, bounded resilience-executor contracts.

use std::{sync::Arc, time::Duration};

use catga_core::{CatgaError, ErrorCode, ResilienceExecutor, ResilienceOptions};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

fn options() -> ResilienceOptions {
    ResilienceOptions {
        max_concurrent: 1,
        max_queued: 0,
        timeout: Duration::from_secs(1),
        max_retries: 1,
        retry_delay: Duration::ZERO,
        circuit_failure_threshold: 2,
        circuit_reset_timeout: Duration::from_secs(1),
    }
}

#[tokio::test]
async fn executor_retries_only_transient_failures_and_preserves_validation_errors() {
    let executor = ResilienceExecutor::new(options()).expect("valid options");
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let transient_attempts = Arc::clone(&attempts);
    let value = executor
        .execute(CancellationToken::new(), move |_| {
            let attempts = Arc::clone(&transient_attempts);
            async move {
                let attempt = attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if attempt == 0 {
                    Err(CatgaError::new(ErrorCode::Transient, "retry me"))
                } else {
                    Ok(42_u8)
                }
            }
        })
        .await;
    assert_eq!(value, Ok(42));
    assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 2);

    let validation_attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let validation_counter = Arc::clone(&validation_attempts);
    let error = executor
        .execute(CancellationToken::new(), move |_| {
            let attempts = Arc::clone(&validation_counter);
            async move {
                attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Err::<(), _>(CatgaError::new(ErrorCode::Validation, "invalid input"))
            }
        })
        .await
        .expect_err("validation must not retry");
    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(
        validation_attempts.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

#[tokio::test]
async fn executor_bounds_inflight_and_queue_work_without_retaining_waiters() {
    let executor = Arc::new(ResilienceExecutor::new(options()).expect("valid options"));
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let started = entered.notified();
    let running_executor = Arc::clone(&executor);
    let running_entered = Arc::clone(&entered);
    let running_release = Arc::clone(&release);
    let running = tokio::spawn(async move {
        running_executor
            .execute(CancellationToken::new(), move |_| {
                let entered = Arc::clone(&running_entered);
                let release = Arc::clone(&running_release);
                async move {
                    entered.notify_one();
                    release.notified().await;
                    Ok::<(), CatgaError>(())
                }
            })
            .await
    });
    started.await;

    let error = executor
        .execute(CancellationToken::new(), |_| async {
            Ok::<(), CatgaError>(())
        })
        .await
        .expect_err("full executor rejects rather than retaining an unbounded waiter");
    assert_eq!(error.code(), ErrorCode::Unavailable);

    release.notify_one();
    assert_eq!(running.await.expect("operation task completes"), Ok(()));
}

#[tokio::test]
async fn executor_cancels_timed_out_attempts_and_opens_after_failed_operations() {
    let mut timeout_options = options();
    timeout_options.timeout = Duration::from_millis(10);
    timeout_options.max_retries = 0;
    let timeout_executor = ResilienceExecutor::new(timeout_options).expect("valid options");
    let timed_out = timeout_executor
        .execute(CancellationToken::new(), |attempt| async move {
            attempt.cancelled().await;
            Ok::<(), CatgaError>(())
        })
        .await
        .expect_err("timeout cancels the attempt future");
    assert_eq!(timed_out.code(), ErrorCode::Timeout);

    let mut circuit_options = options();
    circuit_options.max_retries = 0;
    let circuit_executor = ResilienceExecutor::new(circuit_options).expect("valid options");
    for _ in 0..2 {
        let error = circuit_executor
            .execute(CancellationToken::new(), |_| async {
                Err::<(), _>(CatgaError::new(ErrorCode::Transient, "backend unavailable"))
            })
            .await
            .expect_err("operation fails");
        assert_eq!(error.code(), ErrorCode::Transient);
    }
    let rejected = circuit_executor
        .execute(CancellationToken::new(), |_| async {
            Ok::<(), CatgaError>(())
        })
        .await
        .expect_err("open circuit rejects before calling the operation");
    assert_eq!(rejected.code(), ErrorCode::Transient);
}
