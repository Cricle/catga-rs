//! Shared durable DSL recovery contracts for each step-progress backend.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{DslFlow, DslStateCodec, DslStepProgressStore};

pub struct U32Codec;

impl DslStateCodec<u32> for U32Codec {
    fn encode(&self, state: &u32) -> CatgaResult<Vec<u8>> {
        Ok(state.to_be_bytes().to_vec())
    }

    fn decode(&self, bytes: &[u8]) -> CatgaResult<u32> {
        bytes
            .try_into()
            .map(u32::from_be_bytes)
            .map_err(|_| CatgaError::new(ErrorCode::Internal, "bad checkpoint"))
    }
}

/// Runs the recovery cases that must persist their nested execution cursor.
pub async fn run_durable_recovery_contracts<S>(store: &S, prefix: &str) -> CatgaResult<()>
where
    S: DslStepProgressStore + ?Sized,
{
    nested_conditional(store, &format!("{prefix}/nested")).await?;
    replayable_for_each(store, &format!("{prefix}/foreach")).await?;
    parallel_multi_step(store, &format!("{prefix}/parallel")).await?;
    when_any(store, &format!("{prefix}/when-any")).await
}

/// Returns whether an integration failure means the configured service is unavailable.
#[allow(dead_code)] // Used only by the Redis and NATS integration-test crates.
pub const fn service_unavailable(error: &CatgaError) -> bool {
    matches!(
        error.code(),
        ErrorCode::Transient | ErrorCode::Unavailable | ErrorCode::Timeout
    )
}

fn expect_retry<T>(result: CatgaResult<T>) -> CatgaResult<()> {
    match result {
        Err(error) if error.code() == ErrorCode::Transient => Ok(()),
        Err(error) => Err(error),
        Ok(_) => Err(CatgaError::new(
            ErrorCode::Internal,
            "durable recovery scenario unexpectedly completed on its first attempt",
        )),
    }
}

fn expect_equal(actual: u32, expected: u32, message: &'static str) -> CatgaResult<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(CatgaError::new(ErrorCode::Internal, message))
    }
}

async fn nested_conditional<S>(store: &S, flow_id: &str) -> CatgaResult<()>
where
    S: DslStepProgressStore + ?Sized,
{
    let completed = Arc::new(AtomicUsize::new(0));
    let attempted = Arc::new(AtomicUsize::new(0));
    let first = Arc::clone(&completed);
    let second = Arc::clone(&attempted);
    let flow = DslFlow::new().if_else(
        |_| true,
        DslFlow::new()
            .action(move |value: &mut u32| {
                let first = Arc::clone(&first);
                Box::pin(async move {
                    first.fetch_add(1, Ordering::SeqCst);
                    *value += 1;
                    Ok(())
                })
            })
            .action(move |value: &mut u32| {
                let second = Arc::clone(&second);
                Box::pin(async move {
                    if second.fetch_add(1, Ordering::SeqCst) == 0 {
                        return Err(CatgaError::new(ErrorCode::Transient, "retry nested child"));
                    }
                    *value += 10;
                    Ok(())
                })
            }),
        DslFlow::new(),
    );

    expect_retry(flow.run_checkpointed(flow_id, 0, store, &U32Codec).await)?;
    expect_equal(
        completed.load(Ordering::SeqCst) as u32,
        1,
        "nested child replayed",
    )?;
    expect_equal(
        flow.run_checkpointed(flow_id, 0, store, &U32Codec).await?,
        11,
        "nested state was not restored",
    )?;
    expect_equal(
        completed.load(Ordering::SeqCst) as u32,
        1,
        "completed nested child replayed",
    )?;
    expect_equal(
        attempted.load(Ordering::SeqCst) as u32,
        2,
        "nested retry count changed",
    )
}

async fn replayable_for_each<S>(store: &S, flow_id: &str) -> CatgaResult<()>
where
    S: DslStepProgressStore + ?Sized,
{
    let first_item = Arc::new(AtomicUsize::new(0));
    let second_item = Arc::new(AtomicUsize::new(0));
    let first = Arc::clone(&first_item);
    let second = Arc::clone(&second_item);
    let flow = DslFlow::new().for_each_replayable(
        |_| vec![1_u32, 2_u32],
        move |value, item| {
            let first = Arc::clone(&first);
            let second = Arc::clone(&second);
            Box::pin(async move {
                if item == 1 {
                    first.fetch_add(1, Ordering::SeqCst);
                } else if second.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(CatgaError::new(ErrorCode::Transient, "retry foreach item"));
                }
                *value += item;
                Ok(())
            })
        },
    );

    expect_retry(flow.run_checkpointed(flow_id, 0, store, &U32Codec).await)?;
    expect_equal(
        first_item.load(Ordering::SeqCst) as u32,
        1,
        "completed foreach item replayed",
    )?;
    expect_equal(
        flow.run_checkpointed(flow_id, 0, store, &U32Codec).await?,
        3,
        "foreach state was not restored",
    )?;
    expect_equal(
        first_item.load(Ordering::SeqCst) as u32,
        1,
        "completed foreach item replayed",
    )?;
    expect_equal(
        second_item.load(Ordering::SeqCst) as u32,
        2,
        "foreach retry count changed",
    )
}

async fn parallel_multi_step<S>(store: &S, flow_id: &str) -> CatgaResult<()>
where
    S: DslStepProgressStore + ?Sized,
{
    let first_action = Arc::new(AtomicUsize::new(0));
    let second_action = Arc::new(AtomicUsize::new(0));
    let first = Arc::clone(&first_action);
    let second = Arc::clone(&second_action);
    let flow = DslFlow::new().parallel(
        [DslFlow::new()
            .action(move |value: &mut u32| {
                let first = Arc::clone(&first);
                Box::pin(async move {
                    first.fetch_add(1, Ordering::SeqCst);
                    *value = 1;
                    Ok(())
                })
            })
            .action(move |value: &mut u32| {
                let second = Arc::clone(&second);
                Box::pin(async move {
                    if second.fetch_add(1, Ordering::SeqCst) == 0 {
                        return Err(CatgaError::new(
                            ErrorCode::Transient,
                            "retry parallel child",
                        ));
                    }
                    *value += 1;
                    Ok(())
                })
            })],
        |value, branches| {
            *value = branches[0];
            Ok(())
        },
    );

    expect_retry(flow.run_checkpointed(flow_id, 0, store, &U32Codec).await)?;
    expect_equal(
        first_action.load(Ordering::SeqCst) as u32,
        1,
        "completed parallel action replayed",
    )?;
    expect_equal(
        flow.run_checkpointed(flow_id, 0, store, &U32Codec).await?,
        2,
        "parallel state was not restored",
    )?;
    expect_equal(
        first_action.load(Ordering::SeqCst) as u32,
        1,
        "completed parallel action replayed",
    )?;
    expect_equal(
        second_action.load(Ordering::SeqCst) as u32,
        2,
        "parallel retry count changed",
    )
}

async fn when_any<S>(store: &S, flow_id: &str) -> CatgaResult<()>
where
    S: DslStepProgressStore + ?Sized,
{
    let first_branch = Arc::new(AtomicUsize::new(0));
    let second_branch = Arc::new(AtomicUsize::new(0));
    let merge_attempts = Arc::new(AtomicUsize::new(0));
    let first = Arc::clone(&first_branch);
    let second = Arc::clone(&second_branch);
    let merge = Arc::clone(&merge_attempts);
    let flow = DslFlow::new().when_any(
        [
            DslFlow::new().action(move |value: &mut u32| {
                let first = Arc::clone(&first);
                Box::pin(async move {
                    first.fetch_add(1, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                    *value = 1;
                    Ok(())
                })
            }),
            DslFlow::new().action(move |value: &mut u32| {
                let second = Arc::clone(&second);
                Box::pin(async move {
                    second.fetch_add(1, Ordering::SeqCst);
                    *value = 2;
                    Ok(())
                })
            }),
        ],
        move |value, winner| {
            if merge.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(CatgaError::new(
                    ErrorCode::Transient,
                    "retry when_any merge",
                ));
            }
            *value = winner;
            Ok(())
        },
    );

    expect_retry(flow.run_checkpointed(flow_id, 0, store, &U32Codec).await)?;
    expect_equal(
        first_branch.load(Ordering::SeqCst) as u32,
        1,
        "when_any branch replayed",
    )?;
    expect_equal(
        second_branch.load(Ordering::SeqCst) as u32,
        1,
        "when_any branch replayed",
    )?;
    expect_equal(
        flow.run_checkpointed(flow_id, 0, store, &U32Codec).await?,
        2,
        "when_any winner was not restored",
    )?;
    expect_equal(
        first_branch.load(Ordering::SeqCst) as u32,
        1,
        "when_any branch replayed",
    )?;
    expect_equal(
        second_branch.load(Ordering::SeqCst) as u32,
        1,
        "when_any branch replayed",
    )?;
    expect_equal(
        merge_attempts.load(Ordering::SeqCst) as u32,
        2,
        "when_any merge retry count changed",
    )
}
