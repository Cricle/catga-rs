//! Checkpoint-aware `when_any` execution.

use crate::{DslFlow, DslStateCodec, DslStepProgressStore};
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use futures::{StreamExt, future::BoxFuture, stream::FuturesUnordered};

use crate::{
    dsl::{CloneState, MergeWinner},
    dsl_checkpoint::{CheckpointFrame, CheckpointLevel, CheckpointWork},
    dsl_recovery::{CheckpointContext, persist_checkpoint_payload},
};

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
        let Some((winner, winner_state, result)) = pending.next().await else {
            return Ok(());
        };
        result?;
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
        merge(state, winner_state)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DslStepProgress, DslStepProgressStore};
    use async_trait::async_trait;

    struct U32Codec;
    impl DslStateCodec<u32> for U32Codec {
        fn encode(&self, state: &u32) -> CatgaResult<Vec<u8>> {
            Ok(state.to_be_bytes().to_vec())
        }

        fn decode(&self, bytes: &[u8]) -> CatgaResult<u32> {
            bytes
                .try_into()
                .map(u32::from_be_bytes)
                .map_err(|_| CatgaError::new(ErrorCode::Internal, "invalid test checkpoint state"))
        }
    }

    struct UnusedStore;
    #[async_trait]
    impl DslStepProgressStore for UnusedStore {
        async fn create(&self, _: DslStepProgress) -> CatgaResult<bool> {
            Ok(false)
        }
        async fn update(&self, _: i64, _: DslStepProgress) -> CatgaResult<bool> {
            Ok(false)
        }
        async fn get(&self, _: &str, _: u32) -> CatgaResult<Option<DslStepProgress>> {
            Ok(None)
        }
        async fn delete(&self, _: &str, _: u32) -> CatgaResult<bool> {
            Ok(false)
        }
    }

    #[tokio::test]
    async fn rejects_malformed_checkpointed_winner_index_and_state() {
        let branches = [DslFlow::<u32>::new()];
        let store = UnusedStore;
        let context = CheckpointContext {
            flow_id: "when-any-test",
            top_level_step: 0,
            progress: &store,
            codec: &U32Codec,
        };
        let mut state = 0;
        let clone_state: CloneState<u32> = Clone::clone;
        let merge: MergeWinner<u32> = Box::new(|_: &mut u32, _: u32| Ok(()));

        assert_eq!(
            run_checkpointed_when_any(
                &mut state,
                &branches,
                &clone_state,
                &merge,
                Some(CheckpointWork::WhenAny {
                    winner: 1,
                    state: vec![]
                }),
                &[],
                &context,
            )
            .await
            .expect_err("winner index outside branch list")
            .code(),
            ErrorCode::Validation
        );
        assert_eq!(
            run_checkpointed_when_any(
                &mut state,
                &branches,
                &clone_state,
                &merge,
                Some(CheckpointWork::WhenAny {
                    winner: 0,
                    state: vec![]
                }),
                &[],
                &context,
            )
            .await
            .expect_err("invalid encoded winner state")
            .code(),
            ErrorCode::Validation
        );
    }
}
