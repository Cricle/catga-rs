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
    CatgaError, CatgaResult, CircuitBreakerBehavior, ErrorCode, Handler, Mediator, Pipeline,
    Registry, Request,
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
