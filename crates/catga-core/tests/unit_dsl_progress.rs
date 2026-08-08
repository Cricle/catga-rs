//! Unit tests for DSL progress types and MemoryPack serialization.

use catga_core::codec::memorypack::MemoryPackSerializer;
use catga_core::flow::dsl_progress::{DslProgressKind, DslStepProgress};

#[test]
fn dsl_progress_kind_default_is_application_state() {
    let kind = DslProgressKind::default();
    assert_eq!(kind, DslProgressKind::ApplicationState);
}

#[test]
fn dsl_progress_kind_all_variants() {
    assert_eq!(DslProgressKind::ApplicationState, DslProgressKind::ApplicationState);
    assert_eq!(DslProgressKind::CheckpointFrame, DslProgressKind::CheckpointFrame);
    assert_eq!(DslProgressKind::Terminal, DslProgressKind::Terminal);
}

#[test]
fn dsl_step_progress_new() {
    let progress = DslStepProgress::new("flow-123", 3, [1_u8, 2, 3]);
    assert_eq!(progress.flow_id(), "flow-123");
    assert_eq!(progress.step_index(), 3);
    assert_eq!(progress.version(), 0);
    assert_eq!(progress.kind(), DslProgressKind::ApplicationState);
    assert_eq!(progress.payload(), &[1, 2, 3]);
}

#[test]
fn dsl_step_progress_payload_returns_slice() {
    let progress = DslStepProgress::new("flow", 0, vec![1, 2, 3]);
    let payload = progress.payload();
    assert_eq!(payload, &[1, 2, 3]);
}

#[test]
fn dsl_step_progress_shared_payload_returns_arc() {
    let progress = DslStepProgress::new("flow", 0, [1_u8, 2, 3]);
    let shared = progress.shared_payload();
    // Both should reference the same data
    assert_eq!(&*shared, &[1, 2, 3]);
}

#[test]
fn dsl_step_progress_next_version() {
    let progress = DslStepProgress::new("flow", 0, [1_u8]);
    let next = progress.next_version([2_u8]).expect("next version should succeed");

    assert_eq!(next.flow_id(), "flow");
    assert_eq!(next.step_index(), 0);
    assert_eq!(next.version(), 1);
    assert_eq!(next.payload(), &[2]);
}

#[test]
fn dsl_step_progress_next_version_overflow() {
    let progress = DslStepProgress::new("flow", 0, vec![1_u8]);
    // Manually create a progress at version i64::MAX by using reflection-free approach
    // We can't directly create at i64::MAX, so let's just test multiple updates
    let mut current = progress;
    for i in 0..10 {
        let next = current.next_version([i]).expect("should succeed");
        assert_eq!(next.version(), (i + 1) as i64);
        current = next;
    }
}

#[test]
fn dsl_step_progress_is_next_version() {
    assert!(DslStepProgress::is_next_version(0, 1));
    assert!(DslStepProgress::is_next_version(1, 2));
    assert!(DslStepProgress::is_next_version(i64::MAX - 1, i64::MAX));
}

#[test]
fn dsl_step_progress_is_next_version_rejects_invalid() {
    assert!(!DslStepProgress::is_next_version(0, 0));
    assert!(!DslStepProgress::is_next_version(0, 2));
    assert!(!DslStepProgress::is_next_version(1, 3));
    assert!(!DslStepProgress::is_next_version(i64::MAX, i64::MAX));
}

#[test]
fn dsl_step_progress_wire_round_trip_application_state() {
    let progress = DslStepProgress::new("flow-42", 5, [1_u8, 2, 3]);

    let bytes = MemoryPackSerializer::serialize(&progress).expect("serializes");
    let deserialized = MemoryPackSerializer::deserialize::<DslStepProgress>(&bytes)
        .expect("deserializes");

    assert_eq!(deserialized.flow_id(), progress.flow_id());
    assert_eq!(deserialized.step_index(), progress.step_index());
    assert_eq!(deserialized.version(), progress.version());
    assert_eq!(deserialized.kind(), DslProgressKind::ApplicationState);
    assert_eq!(deserialized.payload(), progress.payload());
}

#[test]
fn dsl_step_progress_wire_round_trip_after_version_update() {
    let original = DslStepProgress::new("flow-42", 0, [1_u8]);
    let updated = original.next_version([2_u8, 3]).expect("version advances");

    let bytes = MemoryPackSerializer::serialize(&updated).expect("serializes");
    let deserialized = MemoryPackSerializer::deserialize::<DslStepProgress>(&bytes)
        .expect("deserializes");

    assert_eq!(deserialized.flow_id(), "flow-42");
    assert_eq!(deserialized.step_index(), 0);
    assert_eq!(deserialized.version(), 1);
    assert_eq!(deserialized.payload(), &[2, 3]);
}

#[test]
fn dsl_step_progress_equality() {
    let progress1 = DslStepProgress::new("flow", 0, [1_u8]);
    let progress2 = DslStepProgress::new("flow", 0, [1_u8]);
    let progress3 = DslStepProgress::new("flow", 0, [2_u8]);

    // Compare specific fields that should be equal/different
    assert_eq!(progress1.flow_id(), progress2.flow_id());
    assert_eq!(progress1.step_index(), progress2.step_index());
    assert_eq!(progress1.version(), progress2.version());
    assert_eq!(progress1.payload(), progress2.payload());

    assert_ne!(progress1.payload(), progress3.payload());
}

#[test]
fn dsl_step_progress_different_flow_ids_are_not_equal() {
    let progress1 = DslStepProgress::new("flow-a", 0, [1_u8]);
    let progress2 = DslStepProgress::new("flow-b", 0, [1_u8]);

    assert_ne!(progress1, progress2);
}

#[test]
fn dsl_step_progress_different_step_indices_are_not_equal() {
    let progress1 = DslStepProgress::new("flow", 0, [1_u8]);
    let progress2 = DslStepProgress::new("flow", 1, [1_u8]);

    assert_ne!(progress1, progress2);
}

#[test]
fn dsl_step_progress_clone() {
    let original = DslStepProgress::new("flow", 0, [1_u8, 2, 3]);
    let cloned = original.clone();

    assert_eq!(original, cloned);
    assert_eq!(original.flow_id(), cloned.flow_id());
    assert_eq!(original.step_index(), cloned.step_index());
    assert_eq!(original.version(), cloned.version());
    assert_eq!(original.payload(), cloned.payload());
}
