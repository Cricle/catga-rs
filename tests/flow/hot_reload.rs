use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use catga_flow::{
    FlowDefinition, FlowRegistry, FlowStepOutcome, FlowVersionManager, MemoryFlowScheduler,
    RegistryFlowRuntime, WaitCondition, WaitPolicy,
};
use catga_memory::MemorySuspendedFlows;

#[tokio::test]
async fn registry_reloads_an_immutable_definition_and_notifies_subscribers() {
    let registry = FlowRegistry::default();
    registry
        .register(
            FlowDefinition::new("payment")
                .step("reserve", |_| async { Ok(FlowStepOutcome::complete()) }),
        )
        .expect("initial definition registers");
    let mut reloads = registry.subscribe();

    let reloaded = registry
        .reload(
            FlowDefinition::new("payment")
                .step("capture", |_| async { Ok(FlowStepOutcome::complete()) }),
        )
        .expect("replacement definition reloads");
    let event = reloads.recv().await.expect("reload event is delivered");
    let current = registry.get("payment").expect("current definition exists");

    assert_eq!(reloaded.old_version(), 0);
    assert_eq!(reloaded.new_version(), 1);
    assert_eq!(event, reloaded);
    assert_eq!(current.version(), 1);
    assert!(current.definition().has_step("capture"));
    assert!(!current.definition().has_step("reserve"));
}

#[test]
fn version_manager_sets_and_increments_independent_flow_versions() {
    let versions = FlowVersionManager::default();

    assert_eq!(versions.current("payment"), 0);
    versions.set("payment", 7).expect("version is set");
    assert_eq!(versions.current("payment"), 7);
    assert_eq!(
        versions.increment("payment").expect("version increments"),
        8
    );
}

#[tokio::test]
async fn resumed_flow_uses_the_definition_visible_after_hot_reload() {
    let store = Arc::new(MemorySuspendedFlows::default());
    let scheduler = Arc::new(MemoryFlowScheduler::default());
    let registry = Arc::new(FlowRegistry::default());
    registry
        .register(
            FlowDefinition::new("payment")
                .step("wait", |_| async {
                    Ok(FlowStepOutcome::wait(WaitCondition::new(
                        "payment-wait",
                        WaitPolicy::All,
                        1,
                        SystemTime::now(),
                        Duration::from_secs(30),
                    )))
                })
                .step("finish", |_| async {
                    Ok(FlowStepOutcome::Fail(catga_core::CatgaError::new(
                        catga_core::ErrorCode::Internal,
                        "obsolete definition executed",
                    )))
                }),
        )
        .expect("initial definition registers");
    let runtime = RegistryFlowRuntime::new(store, scheduler, Arc::clone(&registry), "node-a");

    assert!(
        runtime
            .start("payment-1", "payment", b"input".to_vec())
            .await
            .expect("flow starts")
            .is_suspended()
    );

    registry
        .reload(
            FlowDefinition::new("payment")
                .step("wait", |_| async {
                    Ok(FlowStepOutcome::wait(WaitCondition::new(
                        "payment-wait",
                        WaitPolicy::All,
                        1,
                        SystemTime::now(),
                        Duration::from_secs(30),
                    )))
                })
                .step("finish", |_| async { Ok(FlowStepOutcome::complete()) }),
        )
        .expect("definition reloads");

    assert!(
        runtime
            .record_wait_success("payment-1", "child-1", b"ok".to_vec())
            .await
            .expect("flow resumes")
            .is_success()
    );
}
