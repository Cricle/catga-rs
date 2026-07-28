use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use catga_core::{
    CatgaError, CatgaResult, ErrorCode, ResilienceExecutor, ResilienceOptions, RetryJitter,
};
use tokio_util::sync::CancellationToken;

fn options() -> ResilienceOptions {
    ResilienceOptions {
        max_concurrent: 0,
        max_queued: 0,
        timeout: Duration::from_secs(1),
        max_retries: 1,
        retry_delay: Duration::ZERO,
        circuit_failure_threshold: 2,
        circuit_reset_timeout: Duration::from_secs(1),
    }
}

#[test]
fn resilience_options_reject_invalid_admission_and_timeout_configuration() {
    let invalid_queue = ResilienceOptions {
        max_queued: 1,
        ..options()
    };
    assert!(matches!(
        ResilienceExecutor::new(invalid_queue),
        Err(error) if error.code() == ErrorCode::Validation
    ));

    let invalid_timeout = ResilienceOptions {
        timeout: Duration::ZERO,
        ..options()
    };
    assert!(matches!(
        ResilienceExecutor::new(invalid_timeout),
        Err(error) if error.code() == ErrorCode::Validation
    ));
}

#[tokio::test]
async fn resilience_retries_recoverable_errors_but_not_validation_or_cancellation()
-> CatgaResult<()> {
    let executor = ResilienceExecutor::with_jitter(options(), RetryJitter::none())?;
    assert_eq!(executor.jitter_policy(), RetryJitter::none());

    let attempts = Arc::new(AtomicUsize::new(0));
    let retry_attempts = Arc::clone(&attempts);
    let value = executor
        .execute(CancellationToken::new(), move |_| {
            let attempts = Arc::clone(&retry_attempts);
            async move {
                if attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                    Err(CatgaError::new(ErrorCode::Transient, "retry once"))
                } else {
                    Ok(42_u8)
                }
            }
        })
        .await?;
    assert_eq!(value, 42);
    assert_eq!(attempts.load(Ordering::Relaxed), 2);

    let validation_attempts = Arc::new(AtomicUsize::new(0));
    let validation_counter = Arc::clone(&validation_attempts);
    let validation = executor
        .execute(CancellationToken::new(), move |_| {
            let attempts = Arc::clone(&validation_counter);
            async move {
                attempts.fetch_add(1, Ordering::Relaxed);
                Err::<(), _>(CatgaError::new(ErrorCode::Validation, "invalid input"))
            }
        })
        .await;
    assert!(matches!(validation, Err(error) if error.code() == ErrorCode::Validation));
    assert_eq!(validation_attempts.load(Ordering::Relaxed), 1);

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert!(matches!(
        executor
            .execute(cancellation, |_| async { Ok::<(), CatgaError>(()) })
            .await,
        Err(error) if error.code() == ErrorCode::Cancelled
    ));
    Ok(())
}

#[tokio::test]
async fn resilience_cancels_timed_out_attempts_and_opens_the_circuit() -> CatgaResult<()> {
    let timeout_executor = ResilienceExecutor::with_jitter(
        ResilienceOptions {
            timeout: Duration::from_millis(1),
            max_retries: 0,
            ..options()
        },
        RetryJitter::none(),
    )?;
    let timeout = timeout_executor
        .execute(CancellationToken::new(), |attempt| async move {
            attempt.cancelled().await;
            Ok::<(), CatgaError>(())
        })
        .await;
    assert!(matches!(timeout, Err(error) if error.code() == ErrorCode::Timeout));

    let circuit_executor = ResilienceExecutor::with_jitter(
        ResilienceOptions {
            max_retries: 0,
            circuit_failure_threshold: 1,
            ..options()
        },
        RetryJitter::none(),
    )?;
    assert!(matches!(
        circuit_executor
            .execute(CancellationToken::new(), |_| async {
                Err::<(), _>(CatgaError::new(ErrorCode::Transient, "backend unavailable"))
            })
            .await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    let blocked_attempts = Arc::new(AtomicUsize::new(0));
    let blocked_counter = Arc::clone(&blocked_attempts);
    assert!(matches!(
        circuit_executor
            .execute(CancellationToken::new(), move |_| {
                let attempts = Arc::clone(&blocked_counter);
                async move {
                    attempts.fetch_add(1, Ordering::Relaxed);
                    Ok::<(), CatgaError>(())
                }
            })
            .await,
        Err(error) if error.code() == ErrorCode::Transient
    ));
    assert_eq!(blocked_attempts.load(Ordering::Relaxed), 0);
    Ok(())
}
