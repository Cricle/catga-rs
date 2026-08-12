//! Contract tests for the public Raft error taxonomy: every error type must
//! render a stable message, delegate `source` correctly, and convert from its
//! underlying error types.

use std::error::Error;
use std::io;

use catga_cluster::{
    RaftNodeError, RaftRuntimeError, RaftStateMachineError, RaftStateMachineRuntimeError,
    RaftTransportError, TaskError,
};
use catga_core::{CatgaError, ErrorCode};

#[test]
fn node_error_display_names_each_validation_failure() {
    assert_eq!(
        RaftNodeError::EmptyMembers.to_string(),
        "a Raft cluster needs at least one member"
    );
    assert_eq!(
        RaftNodeError::ZeroMemberId.to_string(),
        "Raft member id zero is reserved"
    );
    assert_eq!(
        RaftNodeError::DuplicateMemberId(9).to_string(),
        "duplicate Raft member id 9"
    );
    assert_eq!(
        RaftNodeError::LocalMemberMissing(3).to_string(),
        "local Raft member 3 is missing"
    );
    assert_eq!(
        RaftNodeError::LocalEndpointMismatch {
            member: "http://member".into(),
            local: "http://local".into(),
        }
        .to_string(),
        "local endpoint http://local does not match configured member endpoint http://member"
    );
    assert_eq!(
        RaftNodeError::ZeroPendingCommitCapacity.to_string(),
        "Raft pending application commit capacity must be non-zero"
    );
    assert_eq!(
        RaftNodeError::PendingCommitCapacity { capacity: 64 }.to_string(),
        "Raft pending application commit capacity of 64 has been reached"
    );
    assert_eq!(
        RaftNodeError::NoEntriesAvailable {
            start_index: 5,
            last_committed_index: 2,
        }
        .to_string(),
        "no Raft entries available: requested start index 5, last committed index 2"
    );
}

#[test]
fn node_error_delegates_wrapped_errors_and_sources() {
    let raft_error = RaftNodeError::from(raft::Error::ProposalDropped);
    assert_eq!(
        raft_error.to_string(),
        raft::Error::ProposalDropped.to_string()
    );
    assert!(raft_error.source().is_some());

    let engine_error = RaftNodeError::from(raft_engine::Error::Corruption("bad tail".to_owned()));
    assert_eq!(
        engine_error.to_string(),
        raft_engine::Error::Corruption("bad tail".to_owned()).to_string()
    );
    assert!(engine_error.source().is_some());

    assert!(RaftNodeError::EmptyMembers.source().is_none());
    assert!(RaftNodeError::ZeroMemberId.source().is_none());
    assert!(RaftNodeError::DuplicateMemberId(1).source().is_none());
    assert!(RaftNodeError::LocalMemberMissing(1).source().is_none());
    assert!(
        RaftNodeError::LocalEndpointMismatch {
            member: "a".into(),
            local: "b".into(),
        }
        .source()
        .is_none()
    );
    assert!(RaftNodeError::ZeroPendingCommitCapacity.source().is_none());
    assert!(
        RaftNodeError::PendingCommitCapacity { capacity: 1 }
            .source()
            .is_none()
    );
    assert!(
        RaftNodeError::NoEntriesAvailable {
            start_index: 1,
            last_committed_index: 0,
        }
        .source()
        .is_none()
    );
}

#[test]
fn transport_error_classifies_retryable_and_fatal_failures() {
    let retryable = RaftTransportError::retryable(io::Error::other("queue full"));
    assert!(retryable.is_retryable());
    assert_eq!(retryable.to_string(), "queue full");
    assert!(retryable.source().is_some());

    let fatal = RaftTransportError::fatal(io::Error::other("invalid peer configuration"));
    assert!(!fatal.is_retryable());
    assert_eq!(fatal.to_string(), "invalid peer configuration");
    assert!(fatal.source().is_some());
}

#[test]
fn task_error_reports_cancellation_and_panic() {
    let cancelled = TaskError::cancelled();
    assert!(cancelled.is_cancelled());
    assert!(!cancelled.is_panic());
    assert_eq!(cancelled.to_string(), "task cancelled");
    assert!(cancelled.source().is_none());

    let panicked = TaskError::panic();
    assert!(!panicked.is_cancelled());
    assert!(panicked.is_panic());
    assert_eq!(panicked.to_string(), "task panicked");
}

#[test]
fn runtime_error_display_and_source_cover_every_variant() {
    assert_eq!(
        RaftRuntimeError::InvalidTickInterval.to_string(),
        "Raft runtime tick interval must be non-zero"
    );
    assert!(RaftRuntimeError::InvalidTickInterval.source().is_none());

    assert_eq!(
        RaftRuntimeError::Stopped.to_string(),
        "Raft runtime stopped"
    );
    assert!(RaftRuntimeError::Stopped.source().is_none());

    let raft_error = RaftRuntimeError::Raft(raft::Error::ProposalDropped);
    assert_eq!(
        raft_error.to_string(),
        raft::Error::ProposalDropped.to_string()
    );
    assert!(raft_error.source().is_some());

    let node_error = RaftRuntimeError::Node(RaftNodeError::ZeroMemberId);
    assert_eq!(
        node_error.to_string(),
        RaftNodeError::ZeroMemberId.to_string()
    );
    assert!(node_error.source().is_some());

    let transport_error =
        RaftRuntimeError::Transport(RaftTransportError::fatal(io::Error::other("link down")));
    assert_eq!(transport_error.to_string(), "link down");
    assert!(transport_error.source().is_some());

    let task_error = RaftRuntimeError::Task(TaskError::panic());
    assert_eq!(task_error.to_string(), "task panicked");
    assert!(task_error.source().is_some());
}

#[test]
fn state_machine_error_display_and_source_cover_every_variant() {
    let application = RaftStateMachineError::from(CatgaError::new(ErrorCode::Internal, "boom"));
    assert_eq!(application.to_string(), "application state machine: boom");
    assert!(application.source().is_none());

    let raft_error = RaftStateMachineError::from(raft::Error::ProposalDropped);
    assert_eq!(
        raft_error.to_string(),
        raft::Error::ProposalDropped.to_string()
    );
    assert!(raft_error.source().is_some());

    let node_error = RaftStateMachineError::from(RaftNodeError::ZeroMemberId);
    assert_eq!(
        node_error.to_string(),
        RaftNodeError::ZeroMemberId.to_string()
    );
    assert!(node_error.source().is_some());

    assert_eq!(
        RaftStateMachineError::NothingApplied.to_string(),
        "cannot checkpoint before applying a Raft command"
    );
    assert!(RaftStateMachineError::NothingApplied.source().is_none());
}

#[test]
fn state_machine_runtime_error_display_and_source_cover_every_variant() {
    assert_eq!(
        RaftStateMachineRuntimeError::InvalidTickInterval.to_string(),
        "Raft state-machine runtime tick interval must be non-zero"
    );
    assert!(
        RaftStateMachineRuntimeError::InvalidTickInterval
            .source()
            .is_none()
    );

    assert_eq!(
        RaftStateMachineRuntimeError::Stopped.to_string(),
        "Raft state-machine runtime stopped"
    );
    assert!(RaftStateMachineRuntimeError::Stopped.source().is_none());

    let raft_error = RaftStateMachineRuntimeError::Raft(raft::Error::ProposalDropped);
    assert_eq!(
        raft_error.to_string(),
        raft::Error::ProposalDropped.to_string()
    );
    assert!(raft_error.source().is_some());

    let node_error = RaftStateMachineRuntimeError::Node(RaftNodeError::ZeroMemberId);
    assert_eq!(
        node_error.to_string(),
        RaftNodeError::ZeroMemberId.to_string()
    );
    assert!(node_error.source().is_some());

    let machine_error =
        RaftStateMachineRuntimeError::StateMachine(RaftStateMachineError::NothingApplied);
    assert_eq!(
        machine_error.to_string(),
        "cannot checkpoint before applying a Raft command"
    );
    assert!(machine_error.source().is_some());

    let transport_error = RaftStateMachineRuntimeError::Transport(RaftTransportError::retryable(
        io::Error::other("peer busy"),
    ));
    assert_eq!(transport_error.to_string(), "peer busy");
    assert!(transport_error.source().is_some());
}
