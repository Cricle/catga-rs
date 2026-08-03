//! Checkpoint-aware `when_any` execution.

use crate::{
    CatgaError, CatgaResult, ErrorCode,
    flow::{
        dsl::DslFlow,
        dsl_checkpoint::{CheckpointFrame, CheckpointLevel, CheckpointWork},
        dsl_progress::{DslStateCodec, DslStepProgressStore},
        dsl_recovery::{CheckpointContext, persist_checkpoint_payload},
        dsl_step::{CloneState, MergeWinner},
    },
};
use futures::{StreamExt, future::BoxFuture, stream::FuturesUnordered};

pub(super) fn run_checkpointed_when_any<'a, S, C, P>(
    state: &'a mut S,
    branches: &'a [DslFlow<S>],
    clone_state: &'a CloneState<S>,
    merge: &'a MergeWinner<S>,
    work: Option<CheckpointWork>,
    levels: &'a [CheckpointLevel],
    context: &'a CheckpointContext<'a, C, P>,
) -> BoxFuture<'a, CatgaResult<()>>
where
    S: Send + 'a,
    C: DslStateCodec<S> + 'a,
    P: DslStepProgressStore + ?Sized + 'a,
{
    Box::pin(async move {
        if let Some(work) = work {
            let (winner, encoded_state) = match work {
                CheckpointWork::WhenAny { winner, state } => (winner, state),
                _ => {
                    return Err(CatgaError::new(
                        ErrorCode::Validation,
                        "DSL checkpoint cursor does not describe a when_any step",
                    ));
                }
            };
            let winner = usize::try_from(winner).map_err(|_| {
                CatgaError::new(
                    ErrorCode::Validation,
                    "when_any checkpoint winner index is too large",
                )
            })?;
            if branches.get(winner).is_none() {
                return Err(CatgaError::new(
                    ErrorCode::Validation,
                    "when_any checkpoint winner index is outside its branches",
                ));
            }
            let winner_state = context.codec.decode(&encoded_state).map_err(|_| {
                CatgaError::new(
                    ErrorCode::Validation,
                    "when_any checkpoint winner state is invalid",
                )
            })?;
            return merge(state, winner_state);
        }

        let mut pending = FuturesUnordered::new();
        for (index, branch) in branches.iter().enumerate() {
            let mut branch_state = clone_state(state);
            pending.push(async move {
                let result = branch.run(&mut branch_state).await;
                (index, branch_state, result)
            });
        }
        let mut last_error = None;
        while let Some((winner, winner_state, result)) = pending.next().await {
            match result {
                Ok(()) => {
                    let winner = u32::try_from(winner).map_err(|_| {
                        CatgaError::new(ErrorCode::Validation, "when_any winner index exceeds u32")
                    })?;
                    let payload = CheckpointFrame::encode(
                        levels,
                        context.codec.encode(state)?,
                        CheckpointWork::WhenAny {
                            winner,
                            state: context.codec.encode(&winner_state)?,
                        },
                    )?;
                    persist_checkpoint_payload(context, payload, true).await?;
                    return merge(state, winner_state);
                }
                Err(error) => last_error = Some(error),
            }
        }
        match last_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    })
}
