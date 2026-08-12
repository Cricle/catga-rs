//! Strict contracts for keyed automatic request batching: option validation,
//! size/timeout flush triggers, shard bounds, cancellation, and panic fencing.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use catga_core::{AutoBatchingBehavior, AutoBatchingRunner, BatchOptions, ErrorCode, Pipeline};
use tokio_util::sync::CancellationToken;

#[path = "support/behavior_support.rs"]
mod behavior_support;

use behavior_support::{Req, req_mediator};

fn options(max_batch_size: usize, timeout_ms: u64) -> BatchOptions {
    BatchOptions {
        max_batch_size,
        batch_timeout: Duration::from_millis(timeout_ms),
        max_queue_length: 64,
        max_shards: 16,
        flush_concurrency: 2,
    }
}

/// Runs `runner` until cancelled and asserts it finishes cleanly.
fn spawn_runner<M: catga_core::Request>(
    runner: AutoBatchingRunner<M>,
    token: CancellationToken,
) -> tokio::task::JoinHandle<catga_core::CatgaResult<()>> {
    tokio::spawn(runner.run_until_cancelled(token))
}

#[tokio::test]
async fn batch_options_validation_rejects_zero_limits() {
    let mut opts = options(2, 10);
    opts.max_batch_size = 0;
    assert!(AutoBatchingBehavior::<Req>::new(opts.clone()).is_err());
    let mut opts = options(2, 10);
    opts.batch_timeout = Duration::ZERO;
    assert!(AutoBatchingBehavior::<Req>::new(opts.clone()).is_err());
    let mut opts = options(2, 10);
    opts.max_queue_length = 0;
    assert!(AutoBatchingBehavior::<Req>::new(opts.clone()).is_err());
    let mut opts = options(2, 10);
    opts.max_shards = 0;
    assert!(AutoBatchingBehavior::<Req>::new(opts.clone()).is_err());
    let mut opts = options(2, 10);
    opts.flush_concurrency = 0;
    assert!(AutoBatchingBehavior::<Req>::new(opts).is_err());
}

#[tokio::test]
async fn batching_flushes_when_the_batch_size_is_reached() {
    let executions = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&executions);
    let mediator = req_mediator(catga_core::request_handler(move |req: Req| {
        let observed = Arc::clone(&observed);
        async move {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(req.id)
        }
    }));
    let (behavior, runner) =
        AutoBatchingBehavior::<Req>::new(options(2, 60_000)).expect("batch behavior builds");
    let token = CancellationToken::new();
    let runner_task = spawn_runner(runner, token.clone());
    let pipeline = Arc::new(Pipeline::<Req>::new().with(behavior));

    let first = tokio::spawn({
        let mediator = Arc::clone(&mediator);
        let pipeline = Arc::clone(&pipeline);
        async move {
            mediator
                .send_with(
                    Req {
                        id: 1,
                        ..Default::default()
                    },
                    &pipeline,
                )
                .await
        }
    });
    // One pending request waits for its shard timeout; the second completes it.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let second = mediator
        .send_with(
            Req {
                id: 2,
                ..Default::default()
            },
            &pipeline,
        )
        .await;
    let first = first.await.expect("task join");
    assert_eq!(first.expect("the first request succeeds"), 1);
    assert_eq!(second.expect("the second request succeeds"), 2);
    assert_eq!(executions.load(Ordering::SeqCst), 2);

    token.cancel();
    runner_task
        .await
        .expect("runner exits")
        .expect("runner clean");
}

#[tokio::test]
async fn batching_flushes_on_timeout_and_keeps_keys_independent() {
    let mediator = req_mediator(catga_core::request_handler(move |req: Req| async move {
        Ok(req.id)
    }));
    let (behavior, runner) =
        AutoBatchingBehavior::<Req>::with_key(options(10, 30), |req| req.key.into())
            .expect("keyed behavior builds");
    let token = CancellationToken::new();
    let runner_task = spawn_runner(runner, token.clone());
    let pipeline = Arc::new(Pipeline::<Req>::new().with(behavior));

    let mut handles = Vec::new();
    for (index, key) in [(1_u64, "a"), (2, "b"), (3, "a")] {
        let mediator = Arc::clone(&mediator);
        let pipeline = Arc::clone(&pipeline);
        handles.push(tokio::spawn(async move {
            mediator
                .send_with(
                    Req {
                        id: index,
                        key,
                        ..Default::default()
                    },
                    &pipeline,
                )
                .await
        }));
    }
    let mut values = Vec::new();
    for handle in handles {
        values.push(handle.await.expect("join").expect("request succeeds"));
    }
    values.sort_unstable();
    assert_eq!(values, vec![1, 2, 3]);

    token.cancel();
    runner_task
        .await
        .expect("runner exits")
        .expect("runner clean");
}

#[tokio::test]
async fn batching_bypasses_when_max_batch_size_is_one() {
    let mediator = req_mediator(catga_core::request_handler(move |req: Req| async move {
        Ok(req.id + 100)
    }));
    let (behavior, runner) =
        AutoBatchingBehavior::<Req>::new(options(1, 60_000)).expect("batch behavior builds");
    drop(runner); // A bypassing behavior never needs its runner.
    let pipeline = Pipeline::<Req>::new().with(behavior);
    let value = mediator
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("the request bypasses batching");
    assert_eq!(value, 101);
}

#[tokio::test]
async fn batching_constructors_from_message_options_and_keys() {
    let (behavior, runner) =
        AutoBatchingBehavior::<Req>::from_message_options().expect("provider options build");
    drop((behavior, runner));
    let (behavior, runner) =
        AutoBatchingBehavior::<Req>::from_message_options_with_key().expect("provider key builds");
    drop((behavior, runner));
    let (behavior, runner) = AutoBatchingBehavior::<Req>::with_message_key(BatchOptions::default())
        .expect("message key builds");
    // `with_message_key` falls back to the default shard when batch_key is None.
    let token = CancellationToken::new();
    let runner_task = spawn_runner(runner, token.clone());
    let mediator = req_mediator(catga_core::request_handler(move |req: Req| async move {
        Ok(req.id)
    }));
    let pipeline = Pipeline::<Req>::new().with(behavior);
    let value = mediator
        .send_with(
            Req {
                id: 5,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("the default shard processes");
    assert_eq!(value, 5);
    let value = mediator
        .send_with(
            Req {
                id: 6,
                key: "custom",
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect("a named shard processes");
    assert_eq!(value, 6);
    token.cancel();
    runner_task
        .await
        .expect("runner exits")
        .expect("runner clean");
}

#[tokio::test]
async fn batching_rejects_queued_work_on_cancellation_and_runner_drop() {
    let mediator = req_mediator(catga_core::request_handler(move |req: Req| async move {
        Ok(req.id)
    }));

    // Cancellation rejects pending work with Unavailable.
    let (behavior, runner) =
        AutoBatchingBehavior::<Req>::new(options(10, 60_000)).expect("behavior builds");
    let token = CancellationToken::new();
    let runner_task = spawn_runner(runner, token.clone());
    let pipeline = Arc::new(Pipeline::<Req>::new().with(behavior));
    let pending = {
        let mediator = Arc::clone(&mediator);
        let pipeline = Arc::clone(&pipeline);
        tokio::spawn(async move {
            mediator
                .send_with(
                    Req {
                        id: 1,
                        ..Default::default()
                    },
                    &pipeline,
                )
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    token.cancel();
    runner_task
        .await
        .expect("runner exits")
        .expect("runner clean");
    let error = pending
        .await
        .expect("join")
        .expect_err("queued work is rejected");
    assert_eq!(error.code(), ErrorCode::Unavailable);

    // Dropping the runner before it starts also rejects callers.
    let (behavior, runner) =
        AutoBatchingBehavior::<Req>::new(options(10, 60_000)).expect("behavior builds");
    drop(runner);
    let pipeline = Pipeline::<Req>::new().with(behavior);
    let error = mediator
        .send_with(
            Req {
                id: 1,
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("a dropped runner rejects");
    assert_eq!(error.code(), ErrorCode::Unavailable);
}

#[tokio::test]
async fn batching_bounds_shard_queues_and_shard_counts() {
    let started = Arc::new(AtomicBool::new(false));
    let gate = Arc::new(tokio::sync::Notify::new());
    let observed_gate = Arc::clone(&gate);
    let observed_started = Arc::clone(&started);
    let mediator = req_mediator(catga_core::request_handler(move |req: Req| {
        let observed_gate = Arc::clone(&observed_gate);
        let observed_started = Arc::clone(&observed_started);
        async move {
            if req.id == 1 {
                observed_started.store(true, Ordering::SeqCst);
                observed_gate.notified().await;
            }
            Ok(req.id)
        }
    }));

    // Shard queue overflow rejects the oldest waiting request.
    let tight = BatchOptions {
        max_batch_size: 2,
        batch_timeout: Duration::from_secs(60),
        max_queue_length: 2,
        max_shards: 4,
        flush_concurrency: 1,
    };
    let (behavior, runner) = AutoBatchingBehavior::<Req>::new(tight).expect("behavior builds");
    let token = CancellationToken::new();
    let pipeline = Arc::new(Pipeline::<Req>::new().with(behavior));
    let runner_task = tokio::spawn({
        let token = token.clone();
        async move { runner.run_until_cancelled(token).await }
    });

    // Hold the only flush slot with a gated first request so the shard backs up.
    // The batch needs two entries before it flushes, so send request 2 first.
    let gated = {
        let mediator = Arc::clone(&mediator);
        let pipeline = Arc::clone(&pipeline);
        tokio::spawn(async move {
            mediator
                .send_with(
                    Req {
                        id: 1,
                        ..Default::default()
                    },
                    &pipeline,
                )
                .await
        })
    };
    let trigger = {
        let mediator = Arc::clone(&mediator);
        let pipeline = Arc::clone(&pipeline);
        tokio::spawn(async move {
            mediator
                .send_with(
                    Req {
                        id: 2,
                        ..Default::default()
                    },
                    &pipeline,
                )
                .await
        })
    };
    wait_for_flag(&started).await;

    let mut waiting = Vec::new();
    for id in 3..=7_u64 {
        let mediator = Arc::clone(&mediator);
        let pipeline = Arc::clone(&pipeline);
        waiting.push(tokio::spawn(async move {
            mediator
                .send_with(
                    Req {
                        id,
                        ..Default::default()
                    },
                    &pipeline,
                )
                .await
        }));
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    gate.notify_one();
    let gated_value = gated
        .await
        .expect("join")
        .expect("the gated request succeeds");
    assert_eq!(gated_value, 1);
    let trigger_value = trigger
        .await
        .expect("join")
        .expect("the trigger request succeeds");
    assert_eq!(trigger_value, 2);

    let mut successes = Vec::new();
    let mut overflow_errors = 0;
    for handle in waiting {
        match handle.await.expect("join") {
            Ok(value) => successes.push(value),
            Err(error) => {
                assert_eq!(error.code(), ErrorCode::Transient);
                assert!(error.message().contains("queue is full"));
                overflow_errors += 1;
            }
        }
    }
    assert!(overflow_errors >= 1, "the bounded queue rejects overflow");
    successes.sort_unstable();
    assert!(successes.iter().all(|value| (3..=7).contains(value)));

    token.cancel();
    runner_task
        .await
        .expect("runner exits")
        .expect("runner clean");

    // Shard capacity: while one waiting shard occupies the single allowed
    // slot, a request for another key is rejected.
    let one_shard = BatchOptions {
        max_batch_size: 2,
        batch_timeout: Duration::from_secs(60),
        max_queue_length: 64,
        max_shards: 1,
        flush_concurrency: 1,
    };
    let (behavior, runner) = AutoBatchingBehavior::<Req>::with_key(one_shard, |req| req.key.into())
        .expect("behavior builds");
    let token = CancellationToken::new();
    let runner_task = spawn_runner(runner, token.clone());
    let pipeline = Arc::new(Pipeline::<Req>::new().with(behavior));
    let parked = {
        let mediator = Arc::clone(&mediator);
        let pipeline = Arc::clone(&pipeline);
        tokio::spawn(async move {
            mediator
                .send_with(
                    Req {
                        id: 1,
                        key: "alpha",
                        ..Default::default()
                    },
                    &pipeline,
                )
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    let error = mediator
        .send_with(
            Req {
                id: 2,
                key: "beta",
                ..Default::default()
            },
            &pipeline,
        )
        .await
        .expect_err("a second key exceeds the shard budget");
    assert_eq!(error.code(), ErrorCode::Transient);
    assert!(error.message().contains("shard capacity"));

    token.cancel();
    runner_task
        .await
        .expect("runner exits")
        .expect("runner clean");
    let error = parked
        .await
        .expect("join")
        .expect_err("cancellation rejects the parked request");
    assert_eq!(error.code(), ErrorCode::Unavailable);
}

#[tokio::test]
async fn batching_maps_handler_panics_to_internal_errors() {
    let exploded = Arc::new(AtomicBool::new(true));
    let observed = Arc::clone(&exploded);
    let mediator = req_mediator(catga_core::request_handler(move |req: Req| {
        let observed = Arc::clone(&observed);
        async move {
            if observed.swap(false, Ordering::SeqCst) && req.id == 1 {
                panic!("batch handler exploded");
            }
            Ok(req.id)
        }
    }));
    let (behavior, runner) =
        AutoBatchingBehavior::<Req>::new(options(2, 60_000)).expect("behavior builds");
    let token = CancellationToken::new();
    let runner_task = spawn_runner(runner, token.clone());
    let pipeline = Arc::new(Pipeline::<Req>::new().with(behavior));

    let first = {
        let mediator = Arc::clone(&mediator);
        let pipeline = Arc::clone(&pipeline);
        tokio::spawn(async move {
            mediator
                .send_with(
                    Req {
                        id: 1,
                        ..Default::default()
                    },
                    &pipeline,
                )
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    let second = mediator
        .send_with(
            Req {
                id: 2,
                ..Default::default()
            },
            &pipeline,
        )
        .await;

    let first_error = first
        .await
        .expect("join")
        .expect_err("the panic maps to Internal");
    assert_eq!(first_error.code(), ErrorCode::Internal);
    assert_eq!(second.expect("the sibling request survives"), 2);

    token.cancel();
    runner_task
        .await
        .expect("runner exits")
        .expect("runner clean");
}

#[tokio::test]
async fn batching_splits_oversized_shards_into_max_batches() {
    let seen = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let slot = Arc::clone(&seen);
    let mediator = req_mediator(catga_core::request_handler(move |req: Req| {
        let slot = Arc::clone(&slot);
        async move {
            slot.lock().expect("seen lock").push(req.id);
            Ok(req.id)
        }
    }));
    let (behavior, runner) =
        AutoBatchingBehavior::<Req>::new(options(2, 60_000)).expect("behavior builds");
    let pipeline = Arc::new(Pipeline::<Req>::new().with(behavior));

    // Queue four same-shard requests before the runner starts so one shard
    // holds more than max_batch_size entries and must be split.
    let mut handles = Vec::new();
    for id in 1..=4_u64 {
        let mediator = Arc::clone(&mediator);
        let pipeline = Arc::clone(&pipeline);
        handles.push(tokio::spawn(async move {
            mediator
                .send_with(
                    Req {
                        id,
                        ..Default::default()
                    },
                    &pipeline,
                )
                .await
        }));
    }
    tokio::time::sleep(Duration::from_millis(30)).await;

    let token = CancellationToken::new();
    let runner_task = spawn_runner(runner, token.clone());
    for handle in handles {
        let value = handle.await.expect("join").expect("the request succeeds");
        assert!((1..=4).contains(&value));
    }
    assert_eq!(seen.lock().expect("seen lock").len(), 4);

    token.cancel();
    runner_task
        .await
        .expect("runner exits")
        .expect("runner clean");
}

async fn wait_for_flag(flag: &AtomicBool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !flag.load(Ordering::SeqCst) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the gated handler never started"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}
