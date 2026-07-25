//! Explicit recoverable DSL step-progress contract tests.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{
    DslFlow, DslFlowLifecycleHooks, DslStateCodec, DslStepProgress, DslStepProgressStore,
};
use catga_memory::MemoryDslStepProgress;

#[path = "dsl_progress_contract.rs"]
mod dsl_progress_contract;

#[tokio::test]
async fn dsl_step_progress_uses_versions_and_keeps_payloads_per_step() {
    let store = MemoryDslStepProgress::default();
    let first = DslStepProgress::new("payment/7", 2, b"cursor:3".to_vec());
    assert!(store.create(first.clone()).await.unwrap());
    assert!(!store.create(first.clone()).await.unwrap());
    assert!(
        store
            .update(
                first.version(),
                first.clone().next_version(b"cursor:4".to_vec())
            )
            .await
            .unwrap()
    );
    assert_eq!(
        store.get("payment/7", 2).await.unwrap().unwrap().payload(),
        b"cursor:4"
    );
    assert!(store.delete("payment/7", 2).await.unwrap());
    assert!(!store.delete("payment/7", 2).await.unwrap());
}

struct U32Codec;
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

struct FailFirstCreateProgressStore {
    inner: MemoryDslStepProgress,
    creates: AtomicUsize,
}

#[async_trait]
impl DslStepProgressStore for FailFirstCreateProgressStore {
    async fn create(&self, progress: DslStepProgress) -> CatgaResult<bool> {
        if self.creates.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(CatgaError::new(
                ErrorCode::Unavailable,
                "progress unavailable",
            ));
        }
        self.inner.create(progress).await
    }

    async fn update(&self, expected_version: i64, next: DslStepProgress) -> CatgaResult<bool> {
        self.inner.update(expected_version, next).await
    }

    async fn get(&self, flow_id: &str, step_index: u32) -> CatgaResult<Option<DslStepProgress>> {
        self.inner.get(flow_id, step_index).await
    }

    async fn delete(&self, flow_id: &str, step_index: u32) -> CatgaResult<bool> {
        self.inner.delete(flow_id, step_index).await
    }
}

#[tokio::test]
async fn dsl_state_codec_round_trips_checkpoint_state() {
    assert_eq!(U32Codec.decode(&U32Codec.encode(&7).unwrap()).unwrap(), 7);
}

#[tokio::test]
async fn durable_dsl_recovery_contract_runs_against_memory() {
    dsl_progress_contract::run_durable_recovery_contracts(
        &MemoryDslStepProgress::default(),
        "payment/contract/memory",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn checkpointed_dsl_treats_a_legacy_cdf1_payload_as_application_state() {
    let store = MemoryDslStepProgress::default();
    assert!(
        store
            .create(DslStepProgress::new(
                "payment/legacy-cdf1",
                0,
                b"CDF1".to_vec()
            ))
            .await
            .unwrap()
    );
    let flow = DslFlow::new().action(|value: &mut u32| {
        Box::pin(async move {
            *value = 0;
            Ok(())
        })
    });

    assert_eq!(
        flow.run_checkpointed("payment/legacy-cdf1", 99, &store, &U32Codec)
            .await
            .unwrap(),
        u32::from_be_bytes(*b"CDF1")
    );
}

#[tokio::test]
async fn checkpointed_dsl_saves_only_successful_top_level_steps() {
    let store = MemoryDslStepProgress::default();
    let flow = DslFlow::new()
        .action(|value: &mut u32| {
            Box::pin(async move {
                *value += 1;
                Ok(())
            })
        })
        .action(|_| Box::pin(async move { Err(CatgaError::new(ErrorCode::Transient, "retry")) }));
    assert_eq!(
        flow.run_checkpointed("payment/7", 0, &store, &U32Codec)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Transient
    );
    assert_eq!(
        store.get("payment/7", 0).await.unwrap().unwrap().payload(),
        1_u32.to_be_bytes()
    );
    assert!(store.get("payment/7", 1).await.unwrap().is_none());
}

#[tokio::test]
async fn checkpointed_dsl_runs_success_hook_before_persist_and_retries_it_after_persist_failure() {
    let store = FailFirstCreateProgressStore {
        inner: MemoryDslStepProgress::default(),
        creates: AtomicUsize::new(0),
    };
    let hook_count = Arc::new(AtomicUsize::new(0));
    let action_count = Arc::new(AtomicUsize::new(0));
    let hook_count_for_hook = Arc::clone(&hook_count);
    let action_count_for_action = Arc::clone(&action_count);
    let flow = DslFlow::new()
        .with_lifecycle_hooks(DslFlowLifecycleHooks::new().on_step_succeeded(
            move |state: &u32, step_index| {
                let hook_count = Arc::clone(&hook_count_for_hook);
                Box::pin(async move {
                    assert_eq!(step_index, 0);
                    assert_eq!(*state, 1);
                    hook_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            },
        ))
        .action(move |value: &mut u32| {
            let action_count = Arc::clone(&action_count_for_action);
            Box::pin(async move {
                action_count.fetch_add(1, Ordering::SeqCst);
                *value += 1;
                Ok(())
            })
        });

    assert_eq!(
        flow.run_checkpointed("payment/hooks-before-persist", 0, &store, &U32Codec)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Unavailable
    );
    assert_eq!(hook_count.load(Ordering::SeqCst), 1);
    assert_eq!(
        flow.run_checkpointed("payment/hooks-before-persist", 0, &store, &U32Codec)
            .await
            .unwrap(),
        1
    );
    assert_eq!(action_count.load(Ordering::SeqCst), 2);
    assert_eq!(hook_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn checkpointed_dsl_restores_the_last_state_without_replaying_completed_steps() {
    let store = MemoryDslStepProgress::default();
    let flow = DslFlow::new().action(|value: &mut u32| {
        Box::pin(async move {
            *value += 1;
            Ok(())
        })
    });
    assert_eq!(
        flow.run_checkpointed("payment/8", 0, &store, &U32Codec)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        flow.run_checkpointed("payment/8", 99, &store, &U32Codec)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn checkpointed_dsl_resumes_an_if_branch_after_its_completed_child() {
    let store = MemoryDslStepProgress::default();
    let completed = Arc::new(AtomicUsize::new(0));
    let attempted = Arc::new(AtomicUsize::new(0));
    let first_child = Arc::clone(&completed);
    let second_child = Arc::clone(&attempted);
    let flow = DslFlow::new().if_else(
        |_| true,
        DslFlow::new()
            .action(move |value: &mut u32| {
                let first_child = Arc::clone(&first_child);
                Box::pin(async move {
                    first_child.fetch_add(1, Ordering::SeqCst);
                    *value += 1;
                    Ok(())
                })
            })
            .action(move |value: &mut u32| {
                let second_child = Arc::clone(&second_child);
                Box::pin(async move {
                    if second_child.fetch_add(1, Ordering::SeqCst) == 0 {
                        return Err(CatgaError::new(ErrorCode::Transient, "retry child"));
                    }
                    *value += 10;
                    Ok(())
                })
            }),
        DslFlow::new(),
    );

    assert_eq!(
        flow.run_checkpointed("payment/if", 0, &store, &U32Codec)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Transient
    );
    assert_eq!(completed.load(Ordering::SeqCst), 1);

    assert_eq!(
        flow.run_checkpointed("payment/if", 0, &store, &U32Codec)
            .await
            .unwrap(),
        11
    );
    assert_eq!(completed.load(Ordering::SeqCst), 1);
    assert_eq!(attempted.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn checkpointed_dsl_resumes_a_match_branch_after_its_completed_child() {
    let store = MemoryDslStepProgress::default();
    let completed = Arc::new(AtomicUsize::new(0));
    let attempted = Arc::new(AtomicUsize::new(0));
    let first_child = Arc::clone(&completed);
    let second_child = Arc::clone(&attempted);
    let flow = DslFlow::new().match_on(
        |_| "selected",
        [(
            "selected",
            DslFlow::new()
                .action(move |value: &mut u32| {
                    let first_child = Arc::clone(&first_child);
                    Box::pin(async move {
                        first_child.fetch_add(1, Ordering::SeqCst);
                        *value += 1;
                        Ok(())
                    })
                })
                .action(move |value: &mut u32| {
                    let second_child = Arc::clone(&second_child);
                    Box::pin(async move {
                        if second_child.fetch_add(1, Ordering::SeqCst) == 0 {
                            return Err(CatgaError::new(ErrorCode::Transient, "retry child"));
                        }
                        *value += 10;
                        Ok(())
                    })
                }),
        )],
        DslFlow::new(),
    );

    assert_eq!(
        flow.run_checkpointed("payment/match", 0, &store, &U32Codec)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Transient
    );
    assert_eq!(completed.load(Ordering::SeqCst), 1);

    assert_eq!(
        flow.run_checkpointed("payment/match", 0, &store, &U32Codec)
            .await
            .unwrap(),
        11
    );
    assert_eq!(completed.load(Ordering::SeqCst), 1);
    assert_eq!(attempted.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn checkpointed_dsl_resumes_each_completed_replayable_for_each_item() {
    let store = MemoryDslStepProgress::default();
    let first_item = Arc::new(AtomicUsize::new(0));
    let second_item = Arc::new(AtomicUsize::new(0));
    let first_item_attempts = Arc::clone(&first_item);
    let second_item_attempts = Arc::clone(&second_item);
    let flow = DslFlow::new().for_each_replayable(
        |_| vec![1_u32, 2_u32],
        move |value, item| {
            let first_item_attempts = Arc::clone(&first_item_attempts);
            let second_item_attempts = Arc::clone(&second_item_attempts);
            Box::pin(async move {
                if item == 1 {
                    first_item_attempts.fetch_add(1, Ordering::SeqCst);
                } else if second_item_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(CatgaError::new(ErrorCode::Transient, "retry item"));
                }
                *value += item;
                Ok(())
            })
        },
    );

    assert_eq!(
        flow.run_checkpointed("payment/foreach", 0, &store, &U32Codec)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Transient
    );
    assert_eq!(first_item.load(Ordering::SeqCst), 1);

    assert_eq!(
        flow.run_checkpointed("payment/foreach", 0, &store, &U32Codec)
            .await
            .unwrap(),
        3
    );
    assert_eq!(first_item.load(Ordering::SeqCst), 1);
    assert_eq!(second_item.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn checkpointed_dsl_replayable_for_each_continue_on_error_persists_callback_state() {
    let store = MemoryDslStepProgress::default();
    let attempts = Arc::new(AtomicUsize::new(0));
    let item_attempts = Arc::clone(&attempts);
    let flow = DslFlow::new().for_each_replayable_continue_on_error(
        |_| vec![1_u32, 2_u32, 3_u32],
        move |value, item| {
            let item_attempts = Arc::clone(&item_attempts);
            Box::pin(async move {
                item_attempts.fetch_add(1, Ordering::SeqCst);
                if item == 2 {
                    return Err(CatgaError::new(ErrorCode::Validation, "declined"));
                }
                *value += item;
                Ok(())
            })
        },
        |value, index, error| {
            Box::pin(async move {
                if index != 1 || error.code() != ErrorCode::Validation {
                    return Err(CatgaError::new(
                        ErrorCode::Internal,
                        "unexpected best-effort item failure",
                    ));
                }
                *value += 100;
                Ok(())
            })
        },
    );

    assert_eq!(
        flow.run_checkpointed("payment/replayable-best-effort", 0, &store, &U32Codec)
            .await
            .unwrap(),
        104
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 3);

    assert_eq!(
        flow.run_checkpointed("payment/replayable-best-effort", 0, &store, &U32Codec)
            .await
            .unwrap(),
        104
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn checkpointed_dsl_rejects_legacy_for_each_without_a_replayable_snapshot() {
    let store = MemoryDslStepProgress::default();
    let flow = DslFlow::new().for_each(
        |_| vec![1_u32],
        |value, item| {
            Box::pin(async move {
                *value += item;
                Ok(())
            })
        },
    );

    assert_eq!(
        flow.run_checkpointed("payment/legacy-foreach", 0, &store, &U32Codec)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
}

#[tokio::test]
async fn checkpointed_dsl_replayable_for_each_uses_the_original_items_after_selector_reorders() {
    let store = MemoryDslStepProgress::default();
    let first_item = Arc::new(AtomicUsize::new(0));
    let second_item = Arc::new(AtomicUsize::new(0));
    let first_item_attempts = Arc::clone(&first_item);
    let second_item_attempts = Arc::clone(&second_item);
    let flow = DslFlow::new().for_each_replayable(
        |value| {
            if *value == 0 {
                vec![1_u32, 2_u32]
            } else {
                vec![2_u32, 1_u32]
            }
        },
        move |value, item| {
            let first_item_attempts = Arc::clone(&first_item_attempts);
            let second_item_attempts = Arc::clone(&second_item_attempts);
            Box::pin(async move {
                if item == 1 {
                    first_item_attempts.fetch_add(1, Ordering::SeqCst);
                } else if second_item_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(CatgaError::new(ErrorCode::Transient, "retry item"));
                }
                *value += item;
                Ok(())
            })
        },
    );

    assert_eq!(
        flow.run_checkpointed("payment/replayable-reorder", 0, &store, &U32Codec)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Transient
    );
    assert_eq!(first_item.load(Ordering::SeqCst), 1);

    assert_eq!(
        flow.run_checkpointed("payment/replayable-reorder", 0, &store, &U32Codec)
            .await
            .unwrap(),
        3
    );
    assert_eq!(first_item.load(Ordering::SeqCst), 1);
    assert_eq!(second_item.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn checkpointed_dsl_restores_completed_parallel_branches_in_declaration_order() {
    let store = MemoryDslStepProgress::default();
    let first_branch = Arc::new(AtomicUsize::new(0));
    let second_branch = Arc::new(AtomicUsize::new(0));
    let first_branch_attempts = Arc::clone(&first_branch);
    let second_branch_attempts = Arc::clone(&second_branch);
    let flow = DslFlow::new().parallel(
        [
            DslFlow::new().action(move |value: &mut u32| {
                let first_branch_attempts = Arc::clone(&first_branch_attempts);
                Box::pin(async move {
                    first_branch_attempts.fetch_add(1, Ordering::SeqCst);
                    *value = 1;
                    Ok(())
                })
            }),
            DslFlow::new().action(move |value: &mut u32| {
                let second_branch_attempts = Arc::clone(&second_branch_attempts);
                Box::pin(async move {
                    if second_branch_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        return Err(CatgaError::new(ErrorCode::Transient, "retry branch"));
                    }
                    *value = 2;
                    Ok(())
                })
            }),
        ],
        |value, branches| {
            *value = branches[0] * 10 + branches[1];
            Ok(())
        },
    );

    assert_eq!(
        flow.run_checkpointed("payment/parallel", 0, &store, &U32Codec)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Transient
    );
    assert_eq!(first_branch.load(Ordering::SeqCst), 1);

    assert_eq!(
        flow.run_checkpointed("payment/parallel", 0, &store, &U32Codec)
            .await
            .unwrap(),
        12
    );
    assert_eq!(first_branch.load(Ordering::SeqCst), 1);
    assert_eq!(second_branch.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn checkpointed_dsl_parallel_resumes_inside_a_multi_step_branch() {
    let store = MemoryDslStepProgress::default();
    let first_action = Arc::new(AtomicUsize::new(0));
    let second_action = Arc::new(AtomicUsize::new(0));
    let first_attempts = Arc::clone(&first_action);
    let second_attempts = Arc::clone(&second_action);
    let flow = DslFlow::new().parallel(
        [DslFlow::new()
            .action(move |value: &mut u32| {
                let first_attempts = Arc::clone(&first_attempts);
                Box::pin(async move {
                    first_attempts.fetch_add(1, Ordering::SeqCst);
                    *value = 1;
                    Ok(())
                })
            })
            .action(move |value: &mut u32| {
                let second_attempts = Arc::clone(&second_attempts);
                Box::pin(async move {
                    if second_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        return Err(CatgaError::new(ErrorCode::Transient, "retry branch"));
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

    assert_eq!(
        flow.run_checkpointed("payment/parallel-child", 0, &store, &U32Codec)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Transient
    );
    assert_eq!(first_action.load(Ordering::SeqCst), 1);
    assert_eq!(
        flow.run_checkpointed("payment/parallel-child", 0, &store, &U32Codec)
            .await
            .unwrap(),
        2
    );
    assert_eq!(first_action.load(Ordering::SeqCst), 1);
    assert_eq!(second_action.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn checkpointed_dsl_parallel_resumes_replayable_for_each_inside_a_branch() {
    let store = MemoryDslStepProgress::default();
    let first_item = Arc::new(AtomicUsize::new(0));
    let second_item = Arc::new(AtomicUsize::new(0));
    let first_attempts = Arc::clone(&first_item);
    let second_attempts = Arc::clone(&second_item);
    let flow = DslFlow::new().parallel(
        [DslFlow::new().for_each_replayable(
            |_| vec![1_u32, 2_u32],
            move |value, item| {
                let first_attempts = Arc::clone(&first_attempts);
                let second_attempts = Arc::clone(&second_attempts);
                Box::pin(async move {
                    if item == 1 {
                        first_attempts.fetch_add(1, Ordering::SeqCst);
                    } else if second_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        return Err(CatgaError::new(ErrorCode::Transient, "retry branch item"));
                    }
                    *value += item;
                    Ok(())
                })
            },
        )],
        |value, branches| {
            *value = branches[0];
            Ok(())
        },
    );

    assert_eq!(
        flow.run_checkpointed("payment/parallel-foreach", 0, &store, &U32Codec)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Transient
    );
    assert_eq!(first_item.load(Ordering::SeqCst), 1);
    assert_eq!(
        flow.run_checkpointed("payment/parallel-foreach", 0, &store, &U32Codec)
            .await
            .unwrap(),
        3
    );
    assert_eq!(first_item.load(Ordering::SeqCst), 1);
    assert_eq!(second_item.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn checkpointed_dsl_parallel_rejects_stream_and_concurrent_for_each_branches() {
    let store = MemoryDslStepProgress::default();
    let stream = DslFlow::new().parallel(
        [DslFlow::new().for_each_stream(
            |_| Box::pin(futures::stream::iter([1_u32])),
            |value, item| {
                Box::pin(async move {
                    *value += item;
                    Ok(())
                })
            },
        )],
        |_, _| Ok(()),
    );
    assert_eq!(
        stream
            .run_checkpointed("payment/parallel-stream", 0, &store, &U32Codec)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );

    let concurrent = DslFlow::new().parallel(
        [DslFlow::new()
            .for_each_concurrent(
                1,
                |_| vec![1_u32],
                |_, item| Box::pin(async move { Ok(item) }),
                |_, _| Ok(()),
            )
            .unwrap()],
        |_, _| Ok(()),
    );
    assert_eq!(
        concurrent
            .run_checkpointed("payment/parallel-concurrent", 0, &store, &U32Codec)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
}

#[tokio::test]
async fn checkpointed_dsl_restores_the_single_when_any_winner_without_rerunning_it() {
    let store = MemoryDslStepProgress::default();
    let first_branch = Arc::new(AtomicUsize::new(0));
    let second_branch = Arc::new(AtomicUsize::new(0));
    let merge_attempts = Arc::new(AtomicUsize::new(0));
    let first_attempts = Arc::clone(&first_branch);
    let second_attempts = Arc::clone(&second_branch);
    let merge_attempts_for_flow = Arc::clone(&merge_attempts);
    let flow = DslFlow::new().when_any(
        [
            DslFlow::new().action(move |value: &mut u32| {
                let first_attempts = Arc::clone(&first_attempts);
                Box::pin(async move {
                    first_attempts.fetch_add(1, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                    *value = 1;
                    Ok(())
                })
            }),
            DslFlow::new().action(move |value: &mut u32| {
                let second_attempts = Arc::clone(&second_attempts);
                Box::pin(async move {
                    second_attempts.fetch_add(1, Ordering::SeqCst);
                    *value = 2;
                    Ok(())
                })
            }),
        ],
        move |value, winner| {
            if merge_attempts_for_flow.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(CatgaError::new(ErrorCode::Transient, "retry merge"));
            }
            *value = winner;
            Ok(())
        },
    );

    assert_eq!(
        flow.run_checkpointed("payment/when-any", 0, &store, &U32Codec)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Transient
    );
    assert_eq!(first_branch.load(Ordering::SeqCst), 1);
    assert_eq!(second_branch.load(Ordering::SeqCst), 1);

    assert_eq!(
        flow.run_checkpointed("payment/when-any", 0, &store, &U32Codec)
            .await
            .unwrap(),
        2
    );
    assert_eq!(first_branch.load(Ordering::SeqCst), 1);
    assert_eq!(second_branch.load(Ordering::SeqCst), 1);
    assert_eq!(merge_attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn checkpointed_dsl_rejects_stream_for_each_without_a_replay_cursor() {
    let store = MemoryDslStepProgress::default();
    let flow = DslFlow::new().for_each_stream(
        |_| Box::pin(futures::stream::iter([1_u32])),
        |value, item| {
            Box::pin(async move {
                *value += item;
                Ok(())
            })
        },
    );

    assert_eq!(
        flow.run_checkpointed("payment/stream", 0, &store, &U32Codec)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
}

#[tokio::test]
async fn checkpointed_dsl_resumes_replayable_for_each_inside_an_if_branch() {
    let store = MemoryDslStepProgress::default();
    let first_item = Arc::new(AtomicUsize::new(0));
    let second_item = Arc::new(AtomicUsize::new(0));
    let first_attempts = Arc::clone(&first_item);
    let second_attempts = Arc::clone(&second_item);
    let flow = DslFlow::new().if_else(
        |_| true,
        DslFlow::new().for_each_replayable(
            |_| vec![1_u32, 2_u32],
            move |value, item| {
                let first_attempts = Arc::clone(&first_attempts);
                let second_attempts = Arc::clone(&second_attempts);
                Box::pin(async move {
                    if item == 1 {
                        first_attempts.fetch_add(1, Ordering::SeqCst);
                    } else if second_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        return Err(CatgaError::new(ErrorCode::Transient, "retry nested item"));
                    }
                    *value += item;
                    Ok(())
                })
            },
        ),
        DslFlow::new(),
    );

    assert!(
        flow.run_checkpointed("payment/nested-foreach", 0, &store, &U32Codec)
            .await
            .is_err()
    );
    assert_eq!(first_item.load(Ordering::SeqCst), 1);
    assert_eq!(
        flow.run_checkpointed("payment/nested-foreach", 0, &store, &U32Codec)
            .await
            .unwrap(),
        3
    );
    assert_eq!(first_item.load(Ordering::SeqCst), 1);
    assert_eq!(second_item.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn checkpointed_dsl_resumes_parallel_inside_a_match_branch() {
    let store = MemoryDslStepProgress::default();
    let first_branch = Arc::new(AtomicUsize::new(0));
    let second_branch = Arc::new(AtomicUsize::new(0));
    let first_attempts = Arc::clone(&first_branch);
    let second_attempts = Arc::clone(&second_branch);
    let parallel = DslFlow::new().parallel(
        [
            DslFlow::new().action(move |value: &mut u32| {
                let first_attempts = Arc::clone(&first_attempts);
                Box::pin(async move {
                    first_attempts.fetch_add(1, Ordering::SeqCst);
                    *value = 1;
                    Ok(())
                })
            }),
            DslFlow::new().action(move |value: &mut u32| {
                let second_attempts = Arc::clone(&second_attempts);
                Box::pin(async move {
                    if second_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        return Err(CatgaError::new(ErrorCode::Transient, "retry nested branch"));
                    }
                    *value = 2;
                    Ok(())
                })
            }),
        ],
        |value, branches| {
            *value = branches[0] * 10 + branches[1];
            Ok(())
        },
    );
    let flow = DslFlow::new().match_on(|_| 1_u32, [(1_u32, parallel)], DslFlow::new());

    assert!(
        flow.run_checkpointed("payment/nested-parallel", 0, &store, &U32Codec)
            .await
            .is_err()
    );
    assert_eq!(first_branch.load(Ordering::SeqCst), 1);
    assert_eq!(
        flow.run_checkpointed("payment/nested-parallel", 0, &store, &U32Codec)
            .await
            .unwrap(),
        12
    );
    assert_eq!(first_branch.load(Ordering::SeqCst), 1);
    assert_eq!(second_branch.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn checkpointed_dsl_rejects_concurrent_for_each_without_result_cursor() {
    let store = MemoryDslStepProgress::default();
    let flow = DslFlow::new()
        .for_each_concurrent(
            1,
            |_| vec![1_u32],
            |_, item| Box::pin(async move { Ok(item) }),
            |_, _| Ok(()),
        )
        .unwrap();

    assert_eq!(
        flow.run_checkpointed("payment/concurrent", 0, &store, &U32Codec)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
}
