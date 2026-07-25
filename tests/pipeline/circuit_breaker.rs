//! Circuit breaker pipeline tests.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, CircuitBreakerBehavior, CircuitBreakerOptions, ErrorCode, Handler,
    Mediator, Pipeline, Registry, Request,
};
use tokio::sync::Notify;

#[derive(Debug)]
struct RemoteCall;

impl catga_core::Message for RemoteCall {}

impl Request for RemoteCall {
    type Response = ();
}

struct IntermittentHandler {
    calls: Arc<AtomicUsize>,
}

struct RecoveryProbeHandler {
    calls: Arc<AtomicUsize>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

struct RatioHandler {
    calls: Arc<AtomicUsize>,
}

struct RollingWindowHandler {
    calls: Arc<AtomicUsize>,
}

struct ValidationThenTransientHandler {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Handler<RemoteCall> for RatioHandler {
    async fn handle(&self, _: RemoteCall) -> CatgaResult<()> {
        let attempt = self.calls.fetch_add(1, Ordering::Relaxed);
        if matches!(attempt, 0 | 2 | 3) {
            return Err(CatgaError::new(ErrorCode::Transient, "backend overloaded"));
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<RemoteCall> for RollingWindowHandler {
    async fn handle(&self, _: RemoteCall) -> CatgaResult<()> {
        let attempt = self.calls.fetch_add(1, Ordering::Relaxed);
        if attempt != 3 {
            return Err(CatgaError::new(ErrorCode::Transient, "backend overloaded"));
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<RemoteCall> for ValidationThenTransientHandler {
    async fn handle(&self, _: RemoteCall) -> CatgaResult<()> {
        if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
            return Err(CatgaError::new(ErrorCode::Validation, "invalid request"));
        }
        Err(CatgaError::new(ErrorCode::Transient, "backend overloaded"))
    }
}

#[async_trait]
impl Handler<RemoteCall> for RecoveryProbeHandler {
    async fn handle(&self, _: RemoteCall) -> CatgaResult<()> {
        let attempt = self.calls.fetch_add(1, Ordering::Relaxed);
        if attempt < 2 {
            return Err(CatgaError::new(
                ErrorCode::Transient,
                "remote service is unavailable",
            ));
        }
        if attempt == 2 {
            self.entered.notify_one();
            self.release.notified().await;
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<RemoteCall> for IntermittentHandler {
    async fn handle(&self, _: RemoteCall) -> CatgaResult<()> {
        let attempt = self.calls.fetch_add(1, Ordering::Relaxed);
        if attempt < 2 {
            return Err(CatgaError::new(
                ErrorCode::Transient,
                "remote service is unavailable",
            ));
        }
        Ok(())
    }
}

#[tokio::test]
async fn circuit_opens_after_failures_rejects_fast_and_allows_one_recovery_probe() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry
        .register_request::<RemoteCall, _>(IntermittentHandler {
            calls: Arc::clone(&calls),
        })
        .expect("test registry accepts one handler");
    let mediator = Mediator::new(registry);
    let pipeline = Pipeline::new().with(
        CircuitBreakerBehavior::new(2, Duration::from_millis(15))
            .expect("valid circuit breaker configuration"),
    );

    assert_eq!(
        mediator.send_with(RemoteCall, &pipeline).await,
        Err(CatgaError::new(
            ErrorCode::Transient,
            "remote service is unavailable"
        ))
    );
    assert_eq!(
        mediator.send_with(RemoteCall, &pipeline).await,
        Err(CatgaError::new(
            ErrorCode::Transient,
            "remote service is unavailable"
        ))
    );
    assert_eq!(calls.load(Ordering::Relaxed), 2);

    let rejected = mediator
        .send_with(RemoteCall, &pipeline)
        .await
        .expect_err("open circuit rejects without a handler call");
    assert_eq!(rejected.code(), ErrorCode::Transient);
    assert_eq!(calls.load(Ordering::Relaxed), 2);

    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(mediator.send_with(RemoteCall, &pipeline).await, Ok(()));
    assert_eq!(calls.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn half_open_circuit_allows_only_one_concurrent_recovery_probe() {
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut registry = Registry::new();
    registry
        .register_request::<RemoteCall, _>(RecoveryProbeHandler {
            calls: Arc::clone(&calls),
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        })
        .expect("test registry accepts one handler");
    let mediator = Arc::new(Mediator::new(registry));
    let pipeline = Arc::new(
        Pipeline::new().with(
            CircuitBreakerBehavior::new(2, Duration::from_millis(15))
                .expect("valid circuit breaker configuration"),
        ),
    );

    for _ in 0..2 {
        let error = mediator
            .send_with(RemoteCall, &pipeline)
            .await
            .expect_err("initial handler failures open the circuit");
        assert_eq!(error.code(), ErrorCode::Transient);
    }
    tokio::time::sleep(Duration::from_millis(25)).await;

    let started = entered.notified();
    let probe_mediator = Arc::clone(&mediator);
    let probe_pipeline = Arc::clone(&pipeline);
    let probe =
        tokio::spawn(async move { probe_mediator.send_with(RemoteCall, &probe_pipeline).await });
    started.await;

    let rejected = mediator
        .send_with(RemoteCall, &pipeline)
        .await
        .expect_err("second half-open request is rejected while the probe runs");
    assert_eq!(rejected.code(), ErrorCode::Transient);
    assert_eq!(calls.load(Ordering::Relaxed), 3);

    release.notify_one();
    assert_eq!(probe.await.expect("recovery probe task completes"), Ok(()));
}

#[tokio::test]
async fn circuit_waits_for_minimum_throughput_then_opens_on_failure_ratio() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry
        .register_request::<RemoteCall, _>(RatioHandler {
            calls: Arc::clone(&calls),
        })
        .expect("test registry accepts one handler");
    let mediator = Mediator::new(registry);
    let options = CircuitBreakerOptions::builder(2, Duration::from_secs(1))
        .sampling_window(4)
        .minimum_throughput(4)
        .failure_ratio(1, 2)
        .build()
        .expect("valid failure-ratio circuit configuration");
    let pipeline = Pipeline::new().with(CircuitBreakerBehavior::with_options(options));

    for _ in 0..3 {
        let _ = mediator.send_with(RemoteCall, &pipeline).await;
    }
    assert_eq!(calls.load(Ordering::Relaxed), 3);

    let fourth = mediator
        .send_with(RemoteCall, &pipeline)
        .await
        .expect_err("the fourth outcome remains the handler failure that opens the circuit");
    assert_eq!(fourth.code(), ErrorCode::Transient);
    assert_eq!(calls.load(Ordering::Relaxed), 4);

    let rejected = mediator
        .send_with(RemoteCall, &pipeline)
        .await
        .expect_err("75 percent failures across four outcomes opens the circuit");
    assert_eq!(rejected.code(), ErrorCode::Transient);
    assert_eq!(calls.load(Ordering::Relaxed), 4);
}

#[tokio::test]
async fn circuit_discards_old_outcomes_when_its_bounded_window_rolls() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry
        .register_request::<RemoteCall, _>(RollingWindowHandler {
            calls: Arc::clone(&calls),
        })
        .expect("test registry accepts one handler");
    let mediator = Mediator::new(registry);
    let options = CircuitBreakerOptions::builder(2, Duration::from_secs(1))
        .sampling_window(4)
        .minimum_throughput(4)
        .failure_ratio(4, 5)
        .build()
        .expect("valid bounded window configuration");
    let pipeline = Pipeline::new().with(CircuitBreakerBehavior::with_options(options));

    for _ in 0..6 {
        let _ = mediator.send_with(RemoteCall, &pipeline).await;
    }

    assert_eq!(
        calls.load(Ordering::Relaxed),
        6,
        "the fifth outcome cannot use discarded history to open the circuit"
    );
}

#[test]
fn circuit_options_reject_invalid_window_throughput_and_ratio() {
    let window_error = CircuitBreakerOptions::builder(2, Duration::from_secs(1))
        .sampling_window(10_001)
        .build()
        .expect_err("bounded windows reject an unbounded capacity");
    assert_eq!(window_error.code(), ErrorCode::Validation);

    let throughput_error = CircuitBreakerOptions::builder(2, Duration::from_secs(1))
        .sampling_window(2)
        .minimum_throughput(3)
        .build()
        .expect_err("minimum throughput must fit in the window");
    assert_eq!(throughput_error.code(), ErrorCode::Validation);

    let ratio_error = CircuitBreakerOptions::builder(2, Duration::from_secs(1))
        .failure_ratio(0, 1)
        .build()
        .expect_err("zero failure ratios are invalid");
    assert_eq!(ratio_error.code(), ErrorCode::Validation);
}

#[tokio::test]
async fn circuit_does_not_count_validation_errors_as_recoverable_failures() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry
        .register_request::<RemoteCall, _>(ValidationThenTransientHandler {
            calls: Arc::clone(&calls),
        })
        .expect("test registry accepts one handler");
    let mediator = Mediator::new(registry);
    let pipeline = Pipeline::new().with(
        CircuitBreakerBehavior::new(1, Duration::from_secs(1))
            .expect("valid compatibility circuit configuration"),
    );

    let validation = mediator
        .send_with(RemoteCall, &pipeline)
        .await
        .expect_err("the handler returns validation errors directly");
    assert_eq!(validation.code(), ErrorCode::Validation);

    let transient = mediator
        .send_with(RemoteCall, &pipeline)
        .await
        .expect_err("validation did not open the circuit before a transient failure");
    assert_eq!(transient.code(), ErrorCode::Transient);
    assert_eq!(calls.load(Ordering::Relaxed), 2);
}
