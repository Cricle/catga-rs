//! Durable cursor validation and progress persistence for the closure-based DSL.

use crate::{DslStateCodec, DslStepProgress, DslStepProgressStore};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use futures::future::BoxFuture;

const MAX_CHECKPOINT_REPLAYABLE_ITEMS: usize = 65_536;
const MAX_CHECKPOINT_REPLAYABLE_ITEM_BYTES: usize = 1024 * 1024;
const MAX_CHECKPOINT_REPLAYABLE_ITEMS_BYTES: usize = 1024 * 1024;
const MAX_CHECKPOINT_CAS_RETRIES: usize = 3;

pub(super) struct CheckpointContext<'a, C, P: ?Sized> {
    pub(super) flow_id: &'a str,
    pub(super) top_level_step: u32,
    pub(super) progress: &'a P,
    pub(super) codec: &'a C,
}

pub(super) fn validate_replayable_for_each_items(items: &[Vec<u8>]) -> CatgaResult<()> {
    if items.len() > MAX_CHECKPOINT_REPLAYABLE_ITEMS {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "replayable for_each item count exceeds the checkpoint limit",
        ));
    }
    let mut total_bytes = 0_usize;
    for item in items {
        if item.len() > MAX_CHECKPOINT_REPLAYABLE_ITEM_BYTES {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "replayable for_each item exceeds the checkpoint size limit",
            ));
        }
        total_bytes = total_bytes.checked_add(item.len()).ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Validation,
                "replayable for_each items exceed the checkpoint size limit",
            )
        })?;
        if total_bytes > MAX_CHECKPOINT_REPLAYABLE_ITEMS_BYTES {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "replayable for_each items exceed the checkpoint size limit",
            ));
        }
    }
    Ok(())
}

pub(super) fn persist_completed_checkpoint<'a, S, C, P>(
    state: &S,
    context: &'a CheckpointContext<'a, C, P>,
) -> BoxFuture<'a, CatgaResult<()>>
where
    C: DslStateCodec<S>,
    P: DslStepProgressStore + ?Sized,
{
    let payload = context.codec.encode(state);
    Box::pin(async move { persist_checkpoint_payload(context, payload?, false).await })
}

pub(super) async fn persist_checkpoint_payload<C, P>(
    context: &CheckpointContext<'_, C, P>,
    payload: Vec<u8>,
    checkpoint_frame: bool,
) -> CatgaResult<()>
where
    P: DslStepProgressStore + ?Sized,
{
    if context
        .progress
        .create(if checkpoint_frame {
            DslStepProgress::new(context.flow_id, context.top_level_step, payload.clone())
                .checkpoint_frame(payload.clone())
        } else {
            DslStepProgress::new(context.flow_id, context.top_level_step, payload.clone())
        })
        .await?
    {
        return Ok(());
    }
    for _ in 0..MAX_CHECKPOINT_CAS_RETRIES {
        let current = context
            .progress
            .get(context.flow_id, context.top_level_step)
            .await?
            .ok_or_else(|| {
                CatgaError::new(
                    ErrorCode::Conflict,
                    "DSL checkpoint disappeared while updating its cursor",
                )
            })?;
        let expected_version = current.version();
        let next = if checkpoint_frame {
            current
                .next_version(payload.clone())?
                .checkpoint_frame(payload.clone())
        } else {
            current.completed_application_state(payload.clone())?
        };
        if context.progress.update(expected_version, next).await? {
            return Ok(());
        }
    }
    Err(CatgaError::new(
        ErrorCode::Conflict,
        "DSL checkpoint cursor update conflicted after bounded retries",
    ))
}
