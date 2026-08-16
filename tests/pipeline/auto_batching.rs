//! Automatic request batching tests.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use catga_core::{
    AutoBatchingBehavior, AutoBatchingRunner, BatchOptions, CatgaResult, ErrorCode, Handler,
    Mediator, Pipeline, Registry, Request,
};
use tokio::{sync::Barrier, task::JoinHandle, time::timeout};
use tokio_util::sync::CancellationToken;

#[derive(Debug, catga_core::Message)]
#[catga(batch_key = "lane")]
struct BatchedWork {
    id: u64,
    lane: &'static str,
}

impl Request for BatchedWork {
    type Response = u64;
}

struct BatchHandler {
    calls: Arc<AtomicUsize>,
    barrier: Option<Arc<Barrier>>,
}

type BatchingRuntime = (
    Arc<Mediator>,
    Arc<Pipeline<BatchedWork>>,
    Arc<AtomicUsize>,
    CancellationToken,
    JoinHandle<CatgaResult<()>>,
);

impl BatchHandler {
    fn new(calls: Arc<AtomicUsize>) -> Self {
        Self {
            calls,
            barrier: None,
        }
    }

    fn synchronized(calls: Arc<AtomicUsize>, barrier: Arc<Barrier>) -> Self {
        Self {
            calls,
            barrier: Some(barrier),
        }
    }
}

#[async_trait]
impl Handler<BatchedWork> for BatchHandler {
    async fn handle(&self, message: BatchedWork) -> CatgaResult<u64> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if let Some(barrier) = &self.barrier {
            barrier.wait().await;
        }
        Ok(message.id)
    }
}

fn batching_runtime(
    behavior: AutoBatchingBehavior<BatchedWork>,
    runner: AutoBatchingRunner<BatchedWork>,
) -> BatchingRuntime {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry
        .register_request::<BatchedWork, _>(BatchHandler::new(Arc::clone(&calls)))
        .expect("test registry accepts one handler");

    let shutdown = CancellationToken::new();
    let runner_shutdown = shutdown.clone();
    let runner_task = tokio::spawn(runner.run_until_cancelled(runner_shutdown));

    (
        Arc::new(Mediator::new(registry)),
        Arc::new(Pipeline::new().with(behavior)),
        calls,
        shutdown,
        runner_task,
    )
}

async fn stop_batching_runner(shutdown: CancellationToken, runner: JoinHandle<CatgaResult<()>>) {
    shutdown.cancel();
    assert_eq!(runner.await.expect("batch runner task completes"), Ok(()));
}

#[tokio::test]
async fn threshold_flushes_pending_requests() {
    let (behavior, runner) = AutoBatchingBehavior::new(BatchOptions {
        max_batch_size: 2,
        batch_timeout: Duration::from_secs(1),
        ..BatchOptions::default()
    })
    .expect("valid batching options");
    let (mediator, pipeline, calls, shutdown, runner_task) = batching_runtime(behavior, runner);

    let first_mediator = Arc::clone(&mediator);
    let first_pipeline = Arc::clone(&pipeline);
    let first = tokio::spawn(async move {
        first_mediator
            .send_with(
                BatchedWork {
                    id: 1,
                    lane: "default",
                },
                &first_pipeline,
            )
            .await
    });

    tokio::task::yield_now().await;
    let second = mediator
        .send_with(
            BatchedWork {
                id: 2,
                lane: "default",
            },
            &pipeline,
        )
        .await;

    assert_eq!(first.await.expect("batch task completes"), Ok(1));
    assert_eq!(second, Ok(2));
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    stop_batching_runner(shutdown, runner_task).await;
}

#[tokio::test]
async fn timeout_flushes_a_partial_batch() {
    let (behavior, runner) = AutoBatchingBehavior::new(BatchOptions {
        max_batch_size: 4,
        batch_timeout: Duration::from_millis(20),
        ..BatchOptions::default()
    })
    .expect("valid batching options");
    let (mediator, pipeline, calls, shutdown, runner_task) = batching_runtime(behavior, runner);

    let result = tokio::time::timeout(
        Duration::from_millis(250),
        mediator.send_with(
            BatchedWork {
                id: 7,
                lane: "default",
            },
            &pipeline,
        ),
    )
    .await;

    assert_eq!(result.expect("partial batch is flushed"), Ok(7));
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    stop_batching_runner(shutdown, runner_task).await;
}

#[tokio::test]
async fn key_selector_keeps_independent_lanes_separate() {
    let (behavior, runner) = AutoBatchingBehavior::with_message_key(BatchOptions {
        max_batch_size: 2,
        batch_timeout: Duration::from_secs(1),
        ..BatchOptions::default()
    })
    .expect("valid keyed batching options");
    let (mediator, pipeline, calls, shutdown, runner_task) = batching_runtime(behavior, runner);

    let waiting_mediator = Arc::clone(&mediator);
    let waiting_pipeline = Arc::clone(&pipeline);
    let waiting = tokio::spawn(async move {
        waiting_mediator
            .send_with(BatchedWork { id: 3, lane: "B" }, &waiting_pipeline)
            .await
    });

    let first_mediator = Arc::clone(&mediator);
    let first_pipeline = Arc::clone(&pipeline);
    let first = tokio::spawn(async move {
        first_mediator
            .send_with(BatchedWork { id: 1, lane: "A" }, &first_pipeline)
            .await
    });
    tokio::task::yield_now().await;

    let second = mediator
        .send_with(BatchedWork { id: 2, lane: "A" }, &pipeline)
        .await;

    assert_eq!(first.await.expect("first lane A request completes"), Ok(1));
    assert_eq!(second, Ok(2));
    assert!(!waiting.is_finished());
    assert_eq!(calls.load(Ordering::Relaxed), 2);

    waiting.abort();
    let _ = waiting.await;
    stop_batching_runner(shutdown, runner_task).await;
}

#[tokio::test]
async fn overflow_rejects_the_oldest_pending_request() {
    let (behavior, runner) = AutoBatchingBehavior::new(BatchOptions {
        max_batch_size: 4,
        max_queue_length: 2,
        batch_timeout: Duration::from_millis(25),
        ..BatchOptions::default()
    })
    .expect("valid batching options");
    let (mediator, pipeline, calls, shutdown, runner_task) = batching_runtime(behavior, runner);

    let first_mediator = Arc::clone(&mediator);
    let first_pipeline = Arc::clone(&pipeline);
    let first = tokio::spawn(async move {
        first_mediator
            .send_with(
                BatchedWork {
                    id: 1,
                    lane: "default",
                },
                &first_pipeline,
            )
            .await
    });
    tokio::task::yield_now().await;

    let second_mediator = Arc::clone(&mediator);
    let second_pipeline = Arc::clone(&pipeline);
    let second = tokio::spawn(async move {
        second_mediator
            .send_with(
                BatchedWork {
                    id: 2,
                    lane: "default",
                },
                &second_pipeline,
            )
            .await
    });
    tokio::task::yield_now().await;

    let third = mediator
        .send_with(
            BatchedWork {
                id: 3,
                lane: "default",
            },
            &pipeline,
        )
        .await;

    let first_error = first
        .await
        .expect("overflowed request receives a result")
        .expect_err("oldest request is rejected");
    assert_eq!(first_error.code(), ErrorCode::Transient);
    assert_eq!(second.await.expect("second request completes"), Ok(2));
    assert_eq!(third, Ok(3));
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    stop_batching_runner(shutdown, runner_task).await;
}

#[tokio::test]
async fn dropped_runner_rejects_requests_without_implicit_startup() {
    let (behavior, runner) =
        AutoBatchingBehavior::new(BatchOptions::default()).expect("valid batching options");
    drop(runner);
    let (mediator, pipeline, _) = batching_runtime_without_runner(behavior);

    let error = mediator
        .send_with(
            BatchedWork {
                id: 1,
                lane: "default",
            },
            &pipeline,
        )
        .await
        .expect_err("closed runner is reported");

    assert_eq!(error.code(), ErrorCode::Unavailable);
}

#[tokio::test]
async fn cancellation_rejects_unstarted_queued_requests() {
    let (behavior, runner) = AutoBatchingBehavior::new(BatchOptions {
        max_batch_size: 2,
        batch_timeout: Duration::from_secs(30),
        ..BatchOptions::default()
    })
    .expect("valid batching options");
    let (mediator, pipeline, _, shutdown, runner_task) = batching_runtime(behavior, runner);

    let waiting_mediator = Arc::clone(&mediator);
    let waiting_pipeline = Arc::clone(&pipeline);
    let waiting = tokio::spawn(async move {
        waiting_mediator
            .send_with(
                BatchedWork {
                    id: 9,
                    lane: "default",
                },
                &waiting_pipeline,
            )
            .await
    });
    tokio::task::yield_now().await;

    shutdown.cancel();
    let result = timeout(Duration::from_secs(1), waiting)
        .await
        .expect("cancellation resolves the queued request")
        .expect("request task completes")
        .expect_err("unstarted request is rejected");
    assert_eq!(result.code(), ErrorCode::Unavailable);
    assert_eq!(
        runner_task.await.expect("batch runner task completes"),
        Ok(())
    );
}

#[tokio::test]
async fn flush_concurrency_runs_entries_in_one_batch_in_parallel() {
    let (behavior, runner) = AutoBatchingBehavior::new(BatchOptions {
        max_batch_size: 2,
        batch_timeout: Duration::from_secs(1),
        flush_concurrency: 2,
        ..BatchOptions::default()
    })
    .expect("valid batching options");
    let calls = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(2));
    let mut registry = Registry::new();
    registry
        .register_request::<BatchedWork, _>(BatchHandler::synchronized(Arc::clone(&calls), barrier))
        .expect("test registry accepts one handler");
    let shutdown = CancellationToken::new();
    let runner_task = tokio::spawn(runner.run_until_cancelled(shutdown.clone()));
    let mediator = Arc::new(Mediator::new(registry));
    let pipeline = Arc::new(Pipeline::new().with(behavior));

    let first_mediator = Arc::clone(&mediator);
    let first_pipeline = Arc::clone(&pipeline);
    let first = tokio::spawn(async move {
        first_mediator
            .send_with(BatchedWork { id: 1, lane: "A" }, &first_pipeline)
            .await
    });
    tokio::task::yield_now().await;

    let second_mediator = Arc::clone(&mediator);
    let second_pipeline = Arc::clone(&pipeline);
    let second = tokio::spawn(async move {
        second_mediator
            .send_with(BatchedWork { id: 2, lane: "A" }, &second_pipeline)
            .await
    });
    let (first, second) = timeout(Duration::from_secs(1), async {
        (
            first.await.expect("first request task completes"),
            second.await.expect("second request task completes"),
        )
    })
    .await
    .expect("both batch entries reach the handler together");

    assert_eq!(first, Ok(1));
    assert_eq!(second, Ok(2));
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    stop_batching_runner(shutdown, runner_task).await;
}

fn batching_runtime_without_runner(
    behavior: AutoBatchingBehavior<BatchedWork>,
) -> (Arc<Mediator>, Arc<Pipeline<BatchedWork>>, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = Registry::new();
    registry
        .register_request::<BatchedWork, _>(BatchHandler::new(Arc::clone(&calls)))
        .expect("test registry accepts one handler");

    (
        Arc::new(Mediator::new(registry)),
        Arc::new(Pipeline::new().with(behavior)),
        calls,
    )
}
