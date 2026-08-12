//! Contract tests for the `catga-core` consensus bridge: the state-machine
//! adapter, the coordinator adapter, and the runtime error mapping.

#[path = "common/members.rs"]
mod members;
#[path = "common/recording_machine.rs"]
mod recording_machine;
#[path = "common/sink_transport.rs"]
mod sink_transport;

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::time::Duration;

use async_trait::async_trait;
use catga_cluster::{
    CoreStateMachine, RaftCommittedEntry, RaftMessage, RaftNode, RaftStateMachine,
    RaftStateMachineDriver, RaftStateMachineRuntime, RaftTransport, RaftTransportError,
    RaftTransportResult,
};
use catga_core::{CatgaError, CatgaResult, ConsensusRuntime, ConsensusStateMachine, ErrorCode};
use members::single_member;
use recording_machine::RecordingMachine;
use sink_transport::SinkTransport;

/// A state machine that records the last applied index; used through the adapter.
#[derive(Default)]
struct LastIndex(u64);

impl RaftStateMachine for LastIndex {
    fn apply(&mut self, entry: &RaftCommittedEntry) -> CatgaResult<()> {
        self.0 = entry.index;
        Ok(())
    }

    fn snapshot(&self) -> CatgaResult<Vec<u8>> {
        Ok(self.0.to_le_bytes().to_vec())
    }

    fn restore(&mut self, bytes: &[u8]) -> CatgaResult<()> {
        let raw: [u8; 8] = bytes.try_into().unwrap_or_default();
        self.0 = u64::from_le_bytes(raw);
        Ok(())
    }
}

#[test]
fn core_state_machine_adapts_apply_snapshot_and_restore() -> CatgaResult<()> {
    let mut machine = CoreStateMachine::new(LastIndex::default());

    ConsensusStateMachine::apply(&mut machine, 7, b"set a=1")?;
    assert_eq!(machine.inner().0, 7);

    let snapshot = ConsensusStateMachine::snapshot(&machine)?;
    let mut restored = CoreStateMachine::new(LastIndex::default());
    ConsensusStateMachine::restore(&mut restored, &snapshot)?;
    assert_eq!(restored.inner().0, 7);
    assert_eq!(restored.into_inner().0, 7);
    Ok(())
}

#[test]
fn core_state_machine_propagates_application_failures() {
    struct Rejecting;

    impl RaftStateMachine for Rejecting {
        fn apply(&mut self, _entry: &RaftCommittedEntry) -> CatgaResult<()> {
            Err(CatgaError::new(ErrorCode::Validation, "rejected"))
        }

        fn snapshot(&self) -> CatgaResult<Vec<u8>> {
            Err(CatgaError::new(ErrorCode::Internal, "no snapshot"))
        }

        fn restore(&mut self, _bytes: &[u8]) -> CatgaResult<()> {
            Err(CatgaError::new(ErrorCode::Internal, "no restore"))
        }
    }

    let mut machine = CoreStateMachine::new(Rejecting);
    assert!(matches!(
        ConsensusStateMachine::apply(&mut machine, 1, b"x"),
        Err(ref error) if error.code() == ErrorCode::Validation
    ));
    assert!(matches!(
        ConsensusStateMachine::snapshot(&machine),
        Err(ref error) if error.code() == ErrorCode::Internal
    ));
    assert!(matches!(
        ConsensusStateMachine::restore(&mut machine, b"x"),
        Err(ref error) if error.code() == ErrorCode::Internal
    ));
}

fn current_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test Tokio runtime must build")
}

fn spawn_single_node_runtime() -> RaftStateMachineRuntime {
    let driver = RaftStateMachineDriver::new(
        RaftNode::new(1, "http://node-1", single_member()).expect("node must construct"),
        RecordingMachine::new(Arc::new(AtomicU64::new(0)), Arc::new(AtomicUsize::new(0))),
    )
    .expect("driver must construct");
    RaftStateMachineRuntime::spawn(driver, Arc::new(SinkTransport), Duration::from_millis(1))
        .expect("runtime must start")
}

#[test]
fn consensus_runtime_covers_the_full_single_node_lifecycle() {
    current_thread_runtime().block_on(async {
        let runtime = spawn_single_node_runtime();

        // Proposing before the election is a retryable routine refusal.
        let early = ConsensusRuntime::propose(&runtime, 1_u64.to_le_bytes().to_vec()).await;
        assert!(matches!(
            early,
            Err(ref error) if error.code() == ErrorCode::Unavailable
        ));
        assert!(ConsensusRuntime::is_alive(&runtime));

        runtime
            .campaign()
            .await
            .expect("single node must elect itself");
        ConsensusRuntime::propose(&runtime, 2_u64.to_le_bytes().to_vec())
            .await
            .expect("the leader accepts proposals");

        let applied = ConsensusRuntime::applied_index(&runtime)
            .await
            .expect("applied index is available");
        assert!(
            applied >= 2,
            "the applied index must advance, got {applied}"
        );

        let coordinator = ConsensusRuntime::coordinator(&runtime);
        assert_eq!(coordinator.node_id(), "1");
        assert!(coordinator.is_leader());
        assert_eq!(
            coordinator.leader_endpoint().as_deref(),
            Some("http://node-1")
        );
        assert_eq!(coordinator.member_endpoints().len(), 1);

        ConsensusRuntime::shutdown(&runtime);
        ConsensusRuntime::join(runtime)
            .await
            .expect("graceful shutdown must join cleanly");
    });
}

#[test]
fn consensus_membership_prevalidation_maps_to_caller_errors() {
    current_thread_runtime().block_on(async {
        let runtime = spawn_single_node_runtime();
        runtime
            .campaign()
            .await
            .expect("single node must elect itself");

        let zero_add = ConsensusRuntime::add_member(&runtime, 0, "http://node-0".to_owned()).await;
        assert!(matches!(
            zero_add,
            Err(ref error) if error.code() == ErrorCode::Validation
        ));

        let duplicate = ConsensusRuntime::add_member(&runtime, 1, "http://node-1".to_owned()).await;
        assert!(matches!(
            duplicate,
            Err(ref error) if error.code() == ErrorCode::Conflict
        ));

        let empty_endpoint = ConsensusRuntime::add_member(&runtime, 2, String::new()).await;
        assert!(matches!(
            empty_endpoint,
            Err(ref error) if error.code() == ErrorCode::Conflict
        ));

        let zero_remove = ConsensusRuntime::remove_member(&runtime, 0).await;
        assert!(matches!(
            zero_remove,
            Err(ref error) if error.code() == ErrorCode::Validation
        ));

        let outsider = ConsensusRuntime::remove_member(&runtime, 9).await;
        assert!(matches!(
            outsider,
            Err(ref error) if error.code() == ErrorCode::Conflict
        ));

        runtime.shutdown();
        runtime.join().await.expect("owner must stop cleanly");
    });
}

#[test]
fn consensus_requests_after_shutdown_are_unavailable() {
    current_thread_runtime().block_on(async {
        let runtime = spawn_single_node_runtime();
        runtime.shutdown();

        let result = ConsensusRuntime::propose(&runtime, 1_u64.to_le_bytes().to_vec()).await;
        assert!(matches!(
            result,
            Err(ref error) if error.code() == ErrorCode::Unavailable
        ));

        runtime.join().await.expect("owner must stop cleanly");
    });
}

#[test]
fn consensus_join_passes_application_failures_through_unchanged() {
    struct FailingMachine;

    impl RaftStateMachine for FailingMachine {
        fn apply(&mut self, _entry: &RaftCommittedEntry) -> CatgaResult<()> {
            Err(CatgaError::new(ErrorCode::Validation, "command rejected"))
        }

        fn snapshot(&self) -> CatgaResult<Vec<u8>> {
            Ok(Vec::new())
        }

        fn restore(&mut self, _bytes: &[u8]) -> CatgaResult<()> {
            Ok(())
        }
    }

    current_thread_runtime().block_on(async {
        let driver = RaftStateMachineDriver::new(
            RaftNode::new(1, "http://node-1", single_member()).expect("node must construct"),
            FailingMachine,
        )
        .expect("driver must construct");
        let runtime = RaftStateMachineRuntime::spawn(
            driver,
            Arc::new(SinkTransport),
            Duration::from_millis(1),
        )
        .expect("runtime must start");

        runtime
            .campaign()
            .await
            .expect("single node must elect itself");
        // The proposal is accepted; the apply failure stops the owner task.
        let _ = ConsensusRuntime::propose(&runtime, 1_u64.to_le_bytes().to_vec()).await;

        let result = ConsensusRuntime::join(runtime).await;
        assert!(matches!(
            result,
            Err(ref error)
                if error.code() == ErrorCode::Validation
                    && error.to_string().contains("command rejected")
        ));
    });
}

struct FatalTransport;

#[async_trait]
impl RaftTransport for FatalTransport {
    async fn send(&self, _message: RaftMessage) -> RaftTransportResult {
        Err(RaftTransportError::fatal(io::Error::other("link down")))
    }
}

#[test]
fn consensus_join_maps_terminal_transport_failures() {
    current_thread_runtime().block_on(async {
        let members = vec![
            catga_cluster::RaftMember::new(1, "http://node-1"),
            catga_cluster::RaftMember::new(2, "http://node-2"),
        ];
        let driver = RaftStateMachineDriver::new(
            RaftNode::new(1, "http://node-1", members).expect("node must construct"),
            RecordingMachine::new(Arc::new(AtomicU64::new(0)), Arc::new(AtomicUsize::new(0))),
        )
        .expect("driver must construct");
        let runtime = RaftStateMachineRuntime::spawn(
            driver,
            Arc::new(FatalTransport),
            Duration::from_millis(1),
        )
        .expect("runtime must start");

        let _ = runtime.campaign().await;

        let result = ConsensusRuntime::join(runtime).await;
        assert!(matches!(
            result,
            Err(ref error) if error.code() == ErrorCode::TransportFailed
        ));
    });
}
