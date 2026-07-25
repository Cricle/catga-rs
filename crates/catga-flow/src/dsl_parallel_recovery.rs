//! Branch-local progress for durable parallel DSL recovery.

use std::sync::Arc;

use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use futures::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use tokio::sync::Mutex;

use crate::{
    DslFlow, DslProgressKind, DslStateCodec, DslStepProgress, DslStepProgressStore,
    dsl::{CloneState, Merge},
    dsl_checkpoint::{CheckpointFrame, CheckpointLevel, CheckpointWork, ParallelBranchProgress},
    dsl_recovery::{CheckpointContext, persist_checkpoint_payload},
};

const MAX_CHECKPOINT_PARALLEL_BRANCHES: usize = 64;
const MAX_PARALLEL_BRANCH_PAYLOAD_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug)]
struct BranchProgressStore {
    progress: Arc<Mutex<Option<DslStepProgress>>>,
}

impl BranchProgressStore {
    fn from_saved(saved: Option<&ParallelBranchProgress>) -> CatgaResult<Self> {
        let progress = match saved {
            Some(ParallelBranchProgress::Completed { state }) => {
                validate_payload_size(state)?;
                None
            }
            Some(ParallelBranchProgress::InProgress {
                step_index,
                checkpoint_frame,
                payload,
            }) => {
                validate_payload_size(payload)?;
                if *checkpoint_frame && CheckpointFrame::decode(payload)?.is_none() {
                    return Err(CatgaError::new(
                        ErrorCode::Validation,
                        "parallel branch checkpoint payload has no internal frame",
                    ));
                }
                let progress = DslStepProgress::new("branch", *step_index, payload.clone());
                Some(if *checkpoint_frame {
                    progress.checkpoint_frame(payload.clone())
                } else {
                    progress
                })
            }
            None => None,
        };
        Ok(Self {
            progress: Arc::new(Mutex::new(progress)),
        })
    }

    async fn snapshot(&self) -> Option<DslStepProgress> {
        self.progress.lock().await.clone()
    }
}

#[async_trait]
impl DslStepProgressStore for BranchProgressStore {
    async fn create(&self, progress: DslStepProgress) -> CatgaResult<bool> {
        let mut current = self.progress.lock().await;
        match current.as_ref() {
            None => {
                *current = Some(progress);
                Ok(true)
            }
            Some(saved) if progress.step_index() > saved.step_index() => {
                *current = Some(progress);
                Ok(true)
            }
            Some(_) => Ok(false),
        }
    }

    async fn update(&self, expected_version: i64, next: DslStepProgress) -> CatgaResult<bool> {
        let mut current = self.progress.lock().await;
        let Some(saved) = current.as_ref() else {
            return Ok(false);
        };
        let Some(next_version) = expected_version.checked_add(1) else {
            return Ok(false);
        };
        if saved.flow_id() != next.flow_id()
            || saved.step_index() != next.step_index()
            || saved.version() != expected_version
            || next.version() != next_version
        {
            return Ok(false);
        }
        *current = Some(next);
        Ok(true)
    }

    async fn get(&self, flow_id: &str, step_index: u32) -> CatgaResult<Option<DslStepProgress>> {
        Ok(self
            .progress
            .lock()
            .await
            .as_ref()
            .filter(|saved| saved.flow_id() == flow_id && saved.step_index() == step_index)
            .cloned())
    }

    async fn delete(&self, flow_id: &str, step_index: u32) -> CatgaResult<bool> {
        let mut current = self.progress.lock().await;
        if current
            .as_ref()
            .is_some_and(|saved| saved.flow_id() == flow_id && saved.step_index() == step_index)
        {
            *current = None;
            return Ok(true);
        }
        Ok(false)
    }
}

fn validate_payload_size(payload: &[u8]) -> CatgaResult<()> {
    validate_payload_size_len(payload.len())
}

fn saved_progress(
    saved: &[Option<ParallelBranchProgress>],
    index: usize,
    progress: &DslStepProgress,
) -> CatgaResult<ParallelBranchProgress> {
    validate_replacement_payload(saved, index, progress.payload().len())?;
    Ok(ParallelBranchProgress::InProgress {
        step_index: progress.step_index(),
        checkpoint_frame: progress.kind() == DslProgressKind::CheckpointFrame,
        payload: progress.payload().to_vec(),
    })
}

async fn run_branch<S, C>(
    index: usize,
    branch: &DslFlow<S>,
    initial: S,
    store: BranchProgressStore,
    codec: &C,
) -> (usize, CatgaResult<S>, Option<DslStepProgress>)
where
    S: Send,
    C: DslStateCodec<S>,
{
    let result = branch
        .run_checkpointed("branch", initial, &store, codec)
        .await;
    let progress = store.snapshot().await;
    (index, result, progress)
}

fn validate_replacement_payload(
    saved: &[Option<ParallelBranchProgress>],
    index: usize,
    replacement_len: usize,
) -> CatgaResult<()> {
    validate_payload_size_len(replacement_len)?;
    let mut total = replacement_len;
    for (saved_index, branch) in saved.iter().enumerate() {
        if saved_index == index {
            continue;
        }
        if let Some(branch) = branch {
            total = total
                .checked_add(branch_payload(branch).len())
                .ok_or_else(|| {
                    CatgaError::new(
                        ErrorCode::Validation,
                        "parallel branch checkpoint payload size overflow",
                    )
                })?;
        }
    }
    if total > MAX_PARALLEL_BRANCH_PAYLOAD_BYTES {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "parallel branch checkpoint payloads exceed the aggregate size limit",
        ));
    }
    Ok(())
}

fn validate_payload_size_len(payload_len: usize) -> CatgaResult<()> {
    if payload_len > MAX_PARALLEL_BRANCH_PAYLOAD_BYTES {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "parallel branch checkpoint payload exceeds the size limit",
        ));
    }
    Ok(())
}

fn branch_payload(branch: &ParallelBranchProgress) -> &[u8] {
    match branch {
        ParallelBranchProgress::Completed { state }
        | ParallelBranchProgress::InProgress { payload: state, .. } => state,
    }
}

fn validate_saved_progress(saved: &[Option<ParallelBranchProgress>]) -> CatgaResult<()> {
    let mut total = 0_usize;
    for branch in saved.iter().flatten() {
        validate_payload_size(branch_payload(branch))?;
        total = total
            .checked_add(branch_payload(branch).len())
            .ok_or_else(|| {
                CatgaError::new(
                    ErrorCode::Validation,
                    "parallel branch checkpoint payload size overflow",
                )
            })?;
    }
    if total > MAX_PARALLEL_BRANCH_PAYLOAD_BYTES {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "parallel branch checkpoint payloads exceed the aggregate size limit",
        ));
    }
    Ok(())
}

fn encode_parallel_progress<S, C, P>(
    state: &S,
    saved: &[Option<ParallelBranchProgress>],
    levels: &[CheckpointLevel],
    context: &CheckpointContext<'_, C, P>,
) -> CatgaResult<Vec<u8>>
where
    C: DslStateCodec<S>,
    P: DslStepProgressStore + ?Sized,
{
    validate_saved_progress(saved)?;
    CheckpointFrame::encode(
        levels,
        context.codec.encode(state)?,
        CheckpointWork::ParallelBranches {
            branches: saved.to_vec(),
        },
    )
}

fn record_first_error(first_error: &mut Option<CatgaError>, result: CatgaResult<()>) {
    if first_error.is_none()
        && let Err(error) = result
    {
        *first_error = Some(error);
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn run_checkpointed_parallel<'a, S, C, P>(
    state: &'a mut S,
    branches: &'a [DslFlow<S>],
    clone_state: &'a CloneState<S>,
    merge: &'a Merge<S>,
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
        if branches.len() > MAX_CHECKPOINT_PARALLEL_BRANCHES {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "parallel branch count exceeds the checkpoint limit",
            ));
        }
        let saved = match work {
            Some(CheckpointWork::Parallel { states }) => states
                .into_iter()
                .map(|state| state.map(|state| ParallelBranchProgress::Completed { state }))
                .collect(),
            Some(CheckpointWork::ParallelBranches { branches }) => branches,
            Some(_) => {
                return Err(CatgaError::new(
                    ErrorCode::Validation,
                    "DSL checkpoint cursor does not describe a parallel step",
                ));
            }
            None => (0..branches.len()).map(|_| None).collect(),
        };
        if saved.len() != branches.len() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "parallel branch count does not match its saved checkpoint",
            ));
        }
        validate_saved_progress(&saved)?;

        let mut states: Vec<Option<S>> = (0..branches.len()).map(|_| None).collect();
        let mut pending = FuturesUnordered::new();
        for (index, branch) in branches.iter().enumerate() {
            match saved[index].as_ref() {
                Some(ParallelBranchProgress::Completed { state: encoded }) => {
                    validate_payload_size(encoded)?;
                    states[index] = Some(context.codec.decode(encoded).map_err(|_| {
                        CatgaError::new(
                            ErrorCode::Validation,
                            "parallel branch checkpoint state is invalid",
                        )
                    })?);
                }
                branch_progress => {
                    let store = BranchProgressStore::from_saved(branch_progress)?;
                    pending.push(run_branch(
                        index,
                        branch,
                        clone_state(state),
                        store,
                        context.codec,
                    ));
                }
            }
        }

        let mut saved = saved;
        let mut first_error = None;
        while let Some((index, result, local_progress)) = pending.next().await {
            let local_progress = local_progress
                .as_ref()
                .map(|progress| saved_progress(&saved, index, progress))
                .transpose();
            match result {
                Ok(branch_state) => match context.codec.encode(&branch_state) {
                    Ok(encoded) => {
                        if let Err(error) =
                            validate_replacement_payload(&saved, index, encoded.len())
                        {
                            record_first_error(&mut first_error, Err(error));
                        } else {
                            saved[index] =
                                Some(ParallelBranchProgress::Completed { state: encoded });
                            states[index] = Some(branch_state);
                        }
                    }
                    Err(error) => {
                        record_first_error(&mut first_error, Err(error));
                    }
                },
                Err(error) => {
                    record_first_error(&mut first_error, Err(error));
                }
            }
            match local_progress {
                Ok(local_progress) => {
                    if states[index].is_none() {
                        saved[index] = local_progress;
                    }
                }
                Err(error) => record_first_error(&mut first_error, Err(error)),
            }
            let persisted = match encode_parallel_progress(state, &saved, levels, context) {
                Ok(payload) => persist_checkpoint_payload(context, payload, true).await,
                Err(error) => Err(error),
            };
            record_first_error(&mut first_error, persisted);
        }
        if let Some(error) = first_error {
            return Err(error);
        }

        let mut completed = Vec::with_capacity(states.len());
        for branch_state in states {
            completed.push(branch_state.ok_or_else(|| {
                CatgaError::new(
                    ErrorCode::Validation,
                    "parallel checkpoint has an incomplete branch",
                )
            })?);
        }
        merge(state, completed)
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use async_trait::async_trait;
    use catga_core::{CatgaResult, ErrorCode};

    use crate::{DslFlow, DslStateCodec, DslStepProgress, DslStepProgressStore};

    use super::{
        BranchProgressStore, CheckpointContext, CloneState, MAX_PARALLEL_BRANCH_PAYLOAD_BYTES,
        Merge, ParallelBranchProgress, run_checkpointed_parallel,
    };

    #[test]
    fn branch_progress_rejects_malformed_checkpoint_frames() {
        let saved = ParallelBranchProgress::InProgress {
            step_index: 0,
            checkpoint_frame: true,
            payload: vec![0],
        };

        assert_eq!(
            BranchProgressStore::from_saved(Some(&saved))
                .expect_err("malformed local frame")
                .code(),
            ErrorCode::Validation
        );
    }

    #[test]
    fn branch_progress_rejects_oversized_payloads() {
        let saved = ParallelBranchProgress::Completed {
            state: vec![0; MAX_PARALLEL_BRANCH_PAYLOAD_BYTES.saturating_add(1)],
        };

        assert_eq!(
            BranchProgressStore::from_saved(Some(&saved))
                .expect_err("oversized local payload")
                .code(),
            ErrorCode::Validation
        );
    }

    struct SequenceCodec {
        calls: Arc<AtomicUsize>,
    }

    impl DslStateCodec<u32> for SequenceCodec {
        fn encode(&self, _: &u32) -> CatgaResult<Vec<u8>> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(vec![0; MAX_PARALLEL_BRANCH_PAYLOAD_BYTES + 1])
            } else {
                Ok(0_u32.to_be_bytes().to_vec())
            }
        }

        fn decode(&self, _: &[u8]) -> CatgaResult<u32> {
            Ok(0)
        }
    }

    struct TestStore;

    #[async_trait]
    impl DslStepProgressStore for TestStore {
        async fn create(&self, _: DslStepProgress) -> CatgaResult<bool> {
            Ok(true)
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
    async fn oversized_local_codec_payload_is_not_overwritten_by_completed_state() {
        let branch = DslFlow::<u32>::new().action(|_| Box::pin(async { Ok(()) }));
        let branches = [branch];
        let store = TestStore;
        let codec = SequenceCodec {
            calls: Arc::new(AtomicUsize::new(0)),
        };
        let context = CheckpointContext {
            flow_id: "parallel-oversized",
            top_level_step: 0,
            progress: &store,
            codec: &codec,
        };
        let clone_state: CloneState<u32> = Clone::clone;
        let merge: Merge<u32> = Box::new(|_, _| Ok(()));
        let mut state = 0;

        assert_eq!(
            run_checkpointed_parallel(
                &mut state,
                &branches,
                &clone_state,
                &merge,
                None,
                &[],
                &context,
            )
            .await
            .expect_err("oversized local codec payload")
            .code(),
            ErrorCode::Validation
        );
    }
}
