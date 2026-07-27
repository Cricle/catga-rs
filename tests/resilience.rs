//! Explicit, bounded resilience-executor contracts.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, CircuitBreakerOptions, Delivery, Envelope, ErrorCode, MessageMetadata,
    MessageTransport, ResilienceExecutor, ResilienceOptions, ResilientTransport, RetryJitter,
};
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

fn error_with_retryability(code: ErrorCode, retryable: bool) -> CatgaError {
    serde_json::from_value(serde_json::json!({
        "code": code,
        "message": "configured retryability",
        "retryable": retryable,
    }))
    .expect("a CatgaError wire override is valid")
}

#[test]
fn default_resilience_executor_uses_full_jitter() {
    assert!(matches!(
        ResilienceExecutor::new(options())
            .expect("valid resilience options")
            .jitter_policy(),
        RetryJitter::Full { .. }
    ));
    assert_eq!(
        ResilienceExecutor::with_jitter(options(), RetryJitter::none())
            .expect("valid explicit jitter policy")
            .jitter_policy(),
        RetryJitter::none()
    );
    assert_eq!(
        ResilienceExecutor::with_jitter(options(), RetryJitter::fixed(Duration::ZERO))
            .expect("valid explicit jitter policy")
            .jitter_policy(),
        RetryJitter::fixed(Duration::ZERO)
    );
    let circuit = CircuitBreakerOptions::builder(2, Duration::from_secs(1))
        .build()
        .expect("valid explicit circuit policy");
    assert_eq!(
        ResilienceExecutor::with_policies(options(), circuit, RetryJitter::fixed(Duration::ZERO),)
            .expect("valid explicit resilience policies")
            .jitter_policy(),
        RetryJitter::fixed(Duration::ZERO)
    );
}

#[derive(Default)]
struct FlakyTransport {
    publish_attempts: AtomicUsize,
    receive_attempts: AtomicUsize,
}

#[async_trait]
impl MessageTransport for FlakyTransport {
    async fn publish(&self, _: Envelope) -> CatgaResult<()> {
        self.publish_attempts.fetch_add(1, Ordering::Relaxed);
        Err(CatgaError::new(
            ErrorCode::Transient,
            "test publish response lost",
        ))
    }

    async fn receive(&self) -> CatgaResult<Delivery> {
        if self.receive_attempts.fetch_add(1, Ordering::Relaxed) == 0 {
            return Err(CatgaError::new(
                ErrorCode::Transient,
                "test broker is reconnecting",
            ));
        }
        Ok(Delivery::new(Envelope::new(
            1,
            "resilience.test",
            Vec::new(),
            MessageMetadata::new(1, None),
        )))
    }
}

#[tokio::test]
async fn resilient_transport_retries_reads_but_not_non_idempotent_writes() {
    let inner = Arc::new(FlakyTransport::default());
    let mut read_options = options();
    read_options.max_retries = 1;
    let mut write_options = options();
    write_options.max_retries = 0;
    let transport = ResilientTransport::new(
        Arc::clone(&inner),
        Arc::new(ResilienceExecutor::new(write_options).expect("valid write policy")),
        Arc::new(ResilienceExecutor::new(read_options).expect("valid read policy")),
    );

    let delivery = transport
        .receive()
        .await
        .expect("a transient read failure is retried");
    assert_eq!(delivery.envelope().id(), 1);
    assert_eq!(inner.receive_attempts.load(Ordering::Relaxed), 2);

    let error = transport
        .publish(Envelope::new(
            2,
            "resilience.test",
            Vec::new(),
            MessageMetadata::new(2, None),
        ))
        .await
        .expect_err("a non-idempotent write policy does not retry");
    assert_eq!(error.code(), ErrorCode::Transient);
    assert_eq!(inner.publish_attempts.load(Ordering::Relaxed), 1);
}

#[test]
fn jitter_is_bounded_and_fixed_jitter_is_predictable() {
    let base = Duration::from_millis(100);
    let full = RetryJitter::full(17);

    assert_eq!(full.delay_for_sample(base, 0), Duration::ZERO);
    assert_eq!(full.delay_for_sample(base, u64::MAX), base);
    assert!(full.delay_for_sample(base, 7).as_nanos() <= base.as_nanos());
    assert_eq!(
        RetryJitter::fixed(Duration::from_millis(7)).delay_for_sample(base, 99),
        Duration::from_millis(7)
    );
}

#[tokio::test]
async fn executor_uses_injected_fixed_jitter_without_blocking_a_runtime_thread() {
    let mut configured = options();
    configured.retry_delay = Duration::from_secs(1);
    let executor = ResilienceExecutor::with_jitter(configured, RetryJitter::fixed(Duration::ZERO))
        .expect("valid fixed-jitter executor");
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let retry_attempts = Arc::clone(&attempts);

    let result = tokio::time::timeout(
        Duration::from_millis(100),
        executor.execute(CancellationToken::new(), move |_| {
            let attempts = Arc::clone(&retry_attempts);
            async move {
                if attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 0 {
                    Err(CatgaError::new(ErrorCode::Transient, "retry me"))
                } else {
                    Ok(42_u8)
                }
            }
        }),
    )
    .await
    .expect("fixed zero jitter avoids the one-second base delay");

    assert_eq!(result, Ok(42));
    assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 2);
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
async fn executor_retries_unavailable_errors() {
    let executor = ResilienceExecutor::new(options()).expect("valid options");
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let retry_attempts = Arc::clone(&attempts);

    let value = executor
        .execute(CancellationToken::new(), move |_| {
            let attempts = Arc::clone(&retry_attempts);
            async move {
                if attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 0 {
                    Err(CatgaError::new(ErrorCode::Unavailable, "retry me"))
                } else {
                    Ok(42_u8)
                }
            }
        })
        .await;

    assert_eq!(value, Ok(42));
    assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 2);
}

#[tokio::test]
async fn executor_honors_retryability_overrides() {
    let executor = ResilienceExecutor::new(options()).expect("valid options");
    let retryable_attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let retryable_counter = Arc::clone(&retryable_attempts);

    let value = executor
        .execute(CancellationToken::new(), move |_| {
            let attempts = Arc::clone(&retryable_counter);
            async move {
                if attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 0 {
                    Err(error_with_retryability(ErrorCode::Validation, true))
                } else {
                    Ok(42_u8)
                }
            }
        })
        .await;

    assert_eq!(value, Ok(42));
    assert_eq!(
        retryable_attempts.load(std::sync::atomic::Ordering::Relaxed),
        2
    );

    let non_retryable_attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let non_retryable_counter = Arc::clone(&non_retryable_attempts);
    let error = executor
        .execute(CancellationToken::new(), move |_| {
            let attempts = Arc::clone(&non_retryable_counter);
            async move {
                attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Err::<(), _>(error_with_retryability(ErrorCode::Transient, false))
            }
        })
        .await
        .expect_err("an explicit non-retryable override is returned");

    assert_eq!(error.code(), ErrorCode::Transient);
    assert_eq!(
        non_retryable_attempts.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

#[tokio::test]
async fn executor_never_retries_cancelled_errors() {
    let executor = ResilienceExecutor::new(options()).expect("valid options");
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let retry_attempts = Arc::clone(&attempts);

    let error = executor
        .execute(CancellationToken::new(), move |_| {
            let attempts = Arc::clone(&retry_attempts);
            async move {
                attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Err::<(), _>(error_with_retryability(ErrorCode::Cancelled, true))
            }
        })
        .await
        .expect_err("cancelled work is returned");

    assert_eq!(error.code(), ErrorCode::Cancelled);
    assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 1);
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

#[tokio::test]
async fn executor_circuit_uses_failure_ratio_after_minimum_throughput() {
    let circuit = CircuitBreakerOptions::builder(2, Duration::from_secs(1))
        .sampling_window(4)
        .minimum_throughput(4)
        .failure_ratio(1, 2)
        .build()
        .expect("valid circuit options");
    let executor =
        ResilienceExecutor::with_policies(options(), circuit, RetryJitter::fixed(Duration::ZERO))
            .expect("valid resilience policies");
    let outcomes = [true, false, true, true];

    for fails in outcomes {
        let result = executor
            .execute(CancellationToken::new(), move |_| async move {
                if fails {
                    Err(CatgaError::new(ErrorCode::Transient, "backend unavailable"))
                } else {
                    Ok(())
                }
            })
            .await;
        if fails {
            assert_eq!(
                result.expect_err("transient failure").code(),
                ErrorCode::Transient
            );
        } else {
            assert_eq!(result, Ok(()));
        }
    }

    let rejected = executor
        .execute(CancellationToken::new(), |_| async {
            Ok::<(), CatgaError>(())
        })
        .await
        .expect_err("three failures from four outcomes open the circuit");
    assert_eq!(rejected.code(), ErrorCode::Transient);
}
