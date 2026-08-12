//! Strict circuit-breaker contracts: configuration validation, failure-ratio
//! opening, cooldown probing, and recovery semantics in request pipelines.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

use catga_core::{
    CatgaError, CircuitBreakerBehavior, CircuitBreakerOptions, ErrorCode, Mediator, Pipeline,
};

#[path = "support/behavior_support.rs"]
mod behavior_support;

use behavior_support::{Req, req_mediator};

/// Builds a mediator whose handler fails `failures` times with `code` before succeeding.
fn failing_mediator(failures: usize, code: ErrorCode) -> (Arc<Mediator>, Arc<AtomicUsize>) {
    let attempts = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&attempts);
    let mediator = req_mediator(catga_core::request_handler(move |req: Req| {
        let observed = Arc::clone(&observed);
        async move {
            if observed.fetch_add(1, Ordering::SeqCst) < failures {
                Err(CatgaError::new(code, "downstream failed"))
            } else {
                Ok(req.id)
            }
        }
    }));
    (mediator, attempts)
}

fn breaker_pipeline(behavior: CircuitBreakerBehavior) -> Pipeline<Req> {
    Pipeline::<Req>::new().with(behavior)
}

#[tokio::test]
async fn circuit_breaker_options_builder_validates_every_bound() {
    let ok = CircuitBreakerOptions::builder(3, Duration::from_millis(10))
        .sampling_window(8)
        .minimum_throughput(4)
        .failure_ratio(1, 2)
        .build()
        .expect("valid options build");
    assert_eq!(ok.sampling_window(), 8);
    assert_eq!(ok.minimum_throughput(), 4);
    assert_eq!(ok.failure_ratio_numerator(), 1);
    assert_eq!(ok.failure_ratio_denominator(), 2);
    assert_eq!(ok.reset_timeout(), Duration::from_millis(10));

    let cases: Vec<(usize, Duration, usize, usize, u32, u32)> = vec![
        (0, Duration::from_millis(1), 1, 1, 1, 1),
        (1, Duration::ZERO, 1, 1, 1, 1),
        (1, Duration::from_millis(1), 0, 1, 1, 1),
        (1, Duration::from_millis(1), 10_001, 1, 1, 1),
        (1, Duration::from_millis(1), 4, 0, 1, 1),
        (1, Duration::from_millis(1), 4, 5, 1, 1),
        (1, Duration::from_millis(1), 4, 4, 0, 1),
        (1, Duration::from_millis(1), 4, 4, 1, 0),
        (1, Duration::from_millis(1), 4, 4, 2, 1),
    ];
    for (threshold, reset, window, throughput, numerator, denominator) in cases {
        let error = CircuitBreakerOptions::builder(threshold, reset)
            .sampling_window(window)
            .minimum_throughput(throughput)
            .failure_ratio(numerator, denominator)
            .build()
            .expect_err("invalid options must fail");
        assert_eq!(error.code(), ErrorCode::Validation);
    }

    match CircuitBreakerBehavior::new(0, Duration::from_millis(1)) {
        Ok(_) => panic!("a zero failure threshold must fail validation"),
        Err(error) => assert_eq!(error.code(), ErrorCode::Validation),
    }
}

#[tokio::test]
async fn circuit_breaker_opens_after_threshold_transient_failures_and_rejects() {
    let (mediator, attempts) = failing_mediator(usize::MAX, ErrorCode::Transient);
    let behavior = CircuitBreakerBehavior::new(2, Duration::from_secs(60)).expect("breaker builds");
    let pipeline = breaker_pipeline(behavior);

    for _ in 0..2 {
        let error = mediator
            .send_with(
                Req {
                    id: 1,
                    ..Default::default()
                },
                &pipeline,
            )
            .await
            .expect_err("handler fails");
        assert_eq!(error.code(), ErrorCode::Transient);
    }

    // The open circuit rejects without touching the handler.
    let before = attempts.load(Ordering::SeqCst);
    let error = mediator
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("an open circuit rejects");
    assert_eq!(error.code(), ErrorCode::Transient);
    assert!(error.message().contains("circuit breaker is open"));
    assert_eq!(attempts.load(Ordering::SeqCst), before);
}

#[tokio::test]
async fn circuit_breaker_ignores_terminal_and_cancelled_errors() {
    // Non-retryable errors never count toward opening.
    let (mediator, _) = failing_mediator(usize::MAX, ErrorCode::Validation);
    let behavior = CircuitBreakerBehavior::new(1, Duration::from_secs(60)).expect("breaker builds");
    let pipeline = breaker_pipeline(behavior);
    for _ in 0..3 {
        let error = mediator
            .send_with(
                Req {
                    id: 1,
                    ..Default::default()
                },
                &pipeline,
            )
            .await
            .expect_err("handler fails terminally");
        assert_eq!(error.code(), ErrorCode::Validation);
    }

    // Cancelled errors short-circuit classification too.
    let (mediator, _) = failing_mediator(usize::MAX, ErrorCode::Cancelled);
    let behavior = CircuitBreakerBehavior::new(1, Duration::from_secs(60)).expect("breaker builds");
    let pipeline = breaker_pipeline(behavior);
    for _ in 0..3 {
        let error = mediator
            .send_with(
                Req {
                    id: 1,
                    ..Default::default()
                },
                &pipeline,
            )
            .await
            .expect_err("handler cancels");
        assert_eq!(error.code(), ErrorCode::Cancelled);
    }
}

#[tokio::test]
async fn circuit_breaker_half_open_probe_success_closes_the_circuit() {
    let (mediator, _) = failing_mediator(2, ErrorCode::Transient);
    let behavior = CircuitBreakerBehavior::new(2, Duration::from_millis(20)).expect("builds");
    let pipeline = breaker_pipeline(behavior);

    for _ in 0..2 {
        mediator
            .send_with(
                Req {
                    id: 1,
                    ..Default::default()
                },
                &pipeline,
            )
            .await
            .expect_err("failure opens the circuit");
    }
    mediator
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the circuit is open before the cooldown");

    tokio::time::sleep(Duration::from_millis(40)).await;
    let value = mediator
        .send_with(
            Req {
                id: 7,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("the recovery probe succeeds");
    assert_eq!(value, 7);

    // A successful probe clears the window and re-admits normal traffic.
    let value = mediator
        .send_with(
            Req {
                id: 8,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("the closed circuit admits requests");
    assert_eq!(value, 8);
}

#[tokio::test]
async fn circuit_breaker_probe_failure_reopens_and_only_one_probe_runs() {
    let started = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(tokio::sync::Notify::new());
    let observed_started = Arc::clone(&started);
    let observed_release = Arc::clone(&release);
    let mediator = req_mediator(catga_core::request_handler(move |req: Req| {
        let observed_started = Arc::clone(&observed_started);
        let observed_release = Arc::clone(&observed_release);
        async move {
            if req.id == 2 {
                observed_started.fetch_add(1, Ordering::SeqCst);
                observed_release.notified().await;
            }
            Err(CatgaError::new(ErrorCode::Transient, "probe fails"))
        }
    }));
    let behavior = CircuitBreakerBehavior::new(1, Duration::from_millis(20)).expect("builds");
    let pipeline = Arc::new(breaker_pipeline(behavior));

    mediator
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the first failure opens the circuit");

    tokio::time::sleep(Duration::from_millis(40)).await;

    // The probe occupies the half-open slot; concurrent requests stay rejected.
    let probe_mediator = Arc::clone(&mediator);
    let probe_pipeline = Arc::clone(&pipeline);
    let probe = tokio::spawn(async move {
        probe_mediator
            .send_with(
                Req {
                    id: 2,
                    ..Default::default()
                },
                &probe_pipeline,
            )
            .await
    });
    wait_for_count(&started, 1).await;

    let error = mediator
        .send_with(
            Req {
                id: 3,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("a half-open circuit admits exactly one probe");
    assert_eq!(error.code(), ErrorCode::Transient);

    release.notify_one();
    let error = probe
        .await
        .expect("probe completes")
        .expect_err("the probe fails");
    assert_eq!(error.code(), ErrorCode::Transient);

    // A failed probe reopens the circuit immediately.
    let error = mediator
        .send_with(
            Req {
                id: 4,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the circuit reopens after a failed probe");
    assert_eq!(error.code(), ErrorCode::Transient);
    assert!(error.message().contains("circuit breaker is open"));
}

#[tokio::test]
async fn circuit_breaker_panicking_probe_reopens_on_guard_drop() {
    let panicked = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&panicked);
    let mediator = req_mediator(catga_core::request_handler(move |req: Req| {
        let observed = Arc::clone(&observed);
        async move {
            if req.id == 1 {
                return Err(CatgaError::new(ErrorCode::Transient, "opens the circuit"));
            }
            if !observed.swap(true, Ordering::SeqCst) {
                panic!("handler exploded");
            }
            Ok(req.id)
        }
    }));
    let behavior = CircuitBreakerBehavior::new(1, Duration::from_millis(20)).expect("builds");
    let pipeline = breaker_pipeline(behavior);

    mediator
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the transient failure opens the circuit");
    tokio::time::sleep(Duration::from_millis(40)).await;

    // The panicking probe is mapped to Internal and its guard reopens the breaker.
    let error = mediator
        .send_with(
            Req {
                id: 2,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the probe panics");
    assert_eq!(error.code(), ErrorCode::Internal);
    let error = mediator
        .send_with(
            Req {
                id: 3,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the abandoned probe reopens the circuit");
    assert!(error.message().contains("circuit breaker is open"));
}

#[tokio::test]
async fn circuit_breaker_failure_ratio_and_window_eviction_govern_opening() {
    // Ratio 1/2 over a four-outcome window: two failures among four outcomes open.
    let options = CircuitBreakerOptions::builder(1, Duration::from_secs(60))
        .sampling_window(4)
        .minimum_throughput(4)
        .failure_ratio(1, 2)
        .build()
        .expect("options build");
    let outcome = Arc::new(std::sync::Mutex::new(Vec::<bool>::new()));
    let scripted = Arc::clone(&outcome);
    let mediator = req_mediator(catga_core::request_handler(move |req: Req| {
        let scripted = Arc::clone(&scripted);
        async move {
            let succeed = scripted
                .lock()
                .expect("outcome list lock")
                .pop()
                .unwrap_or(true);
            if succeed {
                Ok(req.id)
            } else {
                Err(CatgaError::new(ErrorCode::Transient, "scripted failure"))
            }
        }
    }));
    let pipeline = breaker_pipeline(CircuitBreakerBehavior::with_options(options));

    // Scripted call order: success, failure, success, failure. The fourth
    // outcome reaches the minimum throughput with a 2/4 failure ratio, and
    // opening is evaluated while recording that final failure.
    {
        let mut script = outcome.lock().expect("outcome list lock");
        script.extend([false, true, false, true]);
    }
    mediator
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("the first scripted success passes");
    mediator
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the first scripted failure surfaces");
    mediator
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("the second scripted success passes");
    mediator
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the second scripted failure surfaces");
    // Window now holds [success, failure, success, failure] -> 2/4 failures,
    // meeting the 1/2 ratio at minimum throughput, so the circuit opened.
    let error = mediator
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the ratio opened the circuit");
    assert!(error.message().contains("circuit breaker is open"));

    // Window eviction: a two-slot window with ratio 1/2 opens when the
    // second failure evicts the first one and the retained pair still meets
    // the ratio.
    let options = CircuitBreakerOptions::builder(1, Duration::from_secs(60))
        .sampling_window(2)
        .minimum_throughput(2)
        .failure_ratio(1, 2)
        .build()
        .expect("options build");
    let (mediator, _) = failing_mediator(usize::MAX, ErrorCode::Transient);
    let pipeline = breaker_pipeline(CircuitBreakerBehavior::with_options(options));
    mediator
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the first failure is recorded");
    mediator
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the second failure evicts the first and opens");
    let error = mediator
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the circuit is open");
    assert!(error.message().contains("circuit breaker is open"));

    // Evicting a success keeps the failure count intact without opening early:
    // ratio 1/1 needs every retained outcome to be a failure.
    let options = CircuitBreakerOptions::builder(1, Duration::from_secs(60))
        .sampling_window(2)
        .minimum_throughput(2)
        .failure_ratio(1, 1)
        .build()
        .expect("options build");
    let pipeline = breaker_pipeline(CircuitBreakerBehavior::with_options(options));
    let (mediator, attempts) = failing_mediator(1, ErrorCode::Transient);
    mediator
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("the first attempt fails");
    mediator
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("the second attempt succeeds and joins the window");
    mediator
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("evicting the failure keeps the circuit closed");
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

async fn wait_for_count(counter: &AtomicUsize, target: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while counter.load(Ordering::SeqCst) < target {
        assert!(
            tokio::time::Instant::now() < deadline,
            "handler never started"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}
