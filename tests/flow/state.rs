//! Flow state construction helpers.

use catga_flow::{FlowState, FlowStatus};

#[test]
fn flow_state_starts_running_with_immutable_identity_and_cas_version() {
    let state = FlowState::new("flow-7", "payment", b"input".to_vec(), "node-a");

    assert_eq!(state.id(), "flow-7");
    assert_eq!(state.flow_type(), "payment");
    assert_eq!(state.status(), FlowStatus::Running);
    assert_eq!(state.version(), 0);
    assert_eq!(state.owner(), Some("node-a"));
}
