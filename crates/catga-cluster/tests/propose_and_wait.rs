//! Contract tests for `RaftStateMachineRuntime::propose_and_wait` and the
//! `ConsensusRuntime::propose_and_wait` bridge override: a leader resolves
//! with the applied index, a follower refuses fast, a stopped runtime reports
//! `Stopped`, and an entry that cannot commit hits the caller's deadline.

#[path = "common/channel_transport.rs"]
mod channel_transport;
#[path = "common/members.rs"]
mod members;
#[path = "common/recording_machine.rs"]
mod recording_machine;
#[path = "common/sink_transport.rs"]
mod sink_transport;

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use catga_cluster::{
    ClusterCoordinator, RaftMember, RaftMessage, RaftNode, RaftStateMachineDriver,
    RaftStateMachineRuntime, RaftStateMachineRuntimeError, RaftTransport, RaftTransportError,
    RaftTransportResult,
};
use catga_core::{ConsensusRuntime, ErrorCode};
use channel_transport::ChannelTransport;
use members::single_member;
use raft::prelude::MessageType;
use recording_machine::RecordingMachine;
use sink_transport::SinkTransport;

fn current_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test Tokio runtime must build")
}

fn two_members() -> Vec<RaftMember> {
    vec![
        RaftMember::new(1, "http://node-1"),
        RaftMember::new(2, "http://node-2"),
    ]
}

fn spawn_runtime<T>(
    id: u64,
    members: Vec<RaftMember>,
    transport: Arc<T>,
    applied: Arc<AtomicU64>,
) -> RaftStateMachineRuntime
where
    T: RaftTransport + 'static,
{
    let driver = RaftStateMachineDriver::new(
        RaftNode::new(id, format!("http://node-{id}"), members).expect("node must construct"),
        RecordingMachine::new(applied, Arc::new(AtomicUsize::new(0))),
    )
    .expect("driver must construct");
    RaftStateMachineRuntime::spawn(driver, transport, Duration::from_millis(1))
        .expect("runtime must start")
}

#[test]
fn propose_and_wait_on_the_leader_resolves_with_the_applied_index() {
    current_thread_runtime().block_on(async {
        let applied = Arc::new(AtomicU64::new(0));
        let runtime = spawn_runtime(
            1,
            single_member(),
            Arc::new(SinkTransport),
            Arc::clone(&applied),
        );
        runtime
            .campaign()
            .await
            .expect("single node must elect itself");

        // The election no-op occupies log index 1, so the first command lands at 2.
        let first = runtime
            .propose_and_wait(7_u64.to_le_bytes().to_vec(), Duration::from_secs(2))
            .await
            .expect("the leader applies its own proposal");
        assert_eq!(first, 2);
        assert_eq!(
            applied.load(Ordering::Acquire),
            7,
            "the entry is observable in the machine when the call resolves"
        );

        let second = runtime
            .propose_and_wait(9_u64.to_le_bytes().to_vec(), Duration::from_secs(2))
            .await
            .expect("the second proposal applies in order");
        assert_eq!(second, 3);
        assert_eq!(applied.load(Ordering::Acquire), 16);
        assert_eq!(
            runtime.applied_index().await.expect("applied index read"),
            3
        );

        runtime.shutdown();
        runtime.join().await.expect("owner must stop cleanly");
    });
}

#[test]
fn consensus_runtime_propose_and_wait_uses_the_push_path() {
    current_thread_runtime().block_on(async {
        let applied = Arc::new(AtomicU64::new(0));
        let runtime = spawn_runtime(
            1,
            single_member(),
            Arc::new(SinkTransport),
            Arc::clone(&applied),
        );
        runtime
            .campaign()
            .await
            .expect("single node must elect itself");

        let index = ConsensusRuntime::propose_and_wait(
            &runtime,
            5_u64.to_le_bytes().to_vec(),
            Duration::from_secs(2),
        )
        .await
        .expect("the bridge resolves with the applied index");
        assert_eq!(index, 2);
        assert_eq!(applied.load(Ordering::Acquire), 5);

        runtime.shutdown();
        runtime.join().await.expect("owner must stop cleanly");
    });
}

#[test]
fn propose_and_wait_without_leadership_fails_fast() {
    current_thread_runtime().block_on(async {
        let runtime = spawn_runtime(
            1,
            two_members(),
            Arc::new(SinkTransport),
            Arc::new(AtomicU64::new(0)),
        );

        let started = Instant::now();
        let result = runtime
            .propose_and_wait(1_u64.to_le_bytes().to_vec(), Duration::from_secs(30))
            .await;
        assert!(
            matches!(
                result,
                Err(RaftStateMachineRuntimeError::Raft(
                    raft::Error::ProposalDropped
                ))
            ),
            "a node without leadership must refuse the proposal, got {result:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the routine refusal must not wait for the deadline"
        );
        assert!(
            runtime.is_alive(),
            "a routine refusal keeps the runtime alive"
        );

        runtime.shutdown();
        runtime.join().await.expect("owner must stop cleanly");
    });
}

#[test]
fn propose_and_wait_on_a_stopped_runtime_returns_stopped() {
    current_thread_runtime().block_on(async {
        let runtime = spawn_runtime(
            1,
            single_member(),
            Arc::new(SinkTransport),
            Arc::new(AtomicU64::new(0)),
        );
        runtime.shutdown();

        let result = runtime
            .propose_and_wait(1_u64.to_le_bytes().to_vec(), Duration::from_secs(30))
            .await;
        assert!(
            matches!(result, Err(RaftStateMachineRuntimeError::Stopped)),
            "a stopped runtime must report Stopped, got {result:?}"
        );

        runtime.join().await.expect("owner must stop cleanly");
    });
}

/// Routes through the channel hub, but while stalled it refuses entry-bearing
/// appends: heartbeats keep confirming quorum so the leader stays elected, yet
/// no new entry can replicate — a deterministic commit stall.
#[derive(Clone, Default)]
struct CommitStallTransport {
    hub: ChannelTransport,
    stalled: Arc<AtomicBool>,
    intercepted: Arc<AtomicU64>,
}

#[async_trait]
impl RaftTransport for CommitStallTransport {
    async fn send(&self, message: RaftMessage) -> RaftTransportResult {
        if self.stalled.load(Ordering::Acquire)
            && message.get_msg_type() == MessageType::MsgAppend
            && !message.entries.is_empty()
        {
            self.intercepted.fetch_add(1, Ordering::AcqRel);
            return Err(RaftTransportError::retryable(io::Error::other(
                "test stalls entry replication",
            )));
        }
        self.hub.send(message).await
    }
}

#[test]
fn propose_and_wait_times_out_while_the_entry_cannot_commit() {
    current_thread_runtime().block_on(async {
        let transport = CommitStallTransport::default();
        let applied = Arc::new(AtomicU64::new(0));
        let one = spawn_runtime(
            1,
            two_members(),
            Arc::new(transport.clone()),
            Arc::clone(&applied),
        );
        let two = spawn_runtime(
            2,
            two_members(),
            Arc::new(transport.clone()),
            Arc::new(AtomicU64::new(0)),
        );
        transport.hub.register(&one).await;
        transport.hub.register(&two).await;

        one.campaign().await.expect("campaign must start");
        // A committed baseline entry proves replication works before the stall.
        let baseline = one
            .propose_and_wait(1_u64.to_le_bytes().to_vec(), Duration::from_secs(5))
            .await
            .expect("the connected cluster commits the baseline entry");
        assert_eq!(baseline, 2);
        assert!(one.coordinator().is_leader());

        transport.stalled.store(true, Ordering::Release);
        let started = Instant::now();
        let result = one
            .propose_and_wait(2_u64.to_le_bytes().to_vec(), Duration::from_millis(150))
            .await;
        assert!(
            matches!(result, Err(RaftStateMachineRuntimeError::Timeout)),
            "an uncommittable entry must hit the deadline, got {result:?}"
        );
        assert!(
            started.elapsed() >= Duration::from_millis(150),
            "the wait must last the full deadline"
        );
        assert!(
            transport.intercepted.load(Ordering::Acquire) > 0,
            "the stall must actually intercept entry replication"
        );
        assert_eq!(
            applied.load(Ordering::Acquire),
            1,
            "the stalled entry never reaches the state machine"
        );
        assert!(one.is_alive(), "a deadline expiry keeps the runtime alive");
        assert!(
            RaftStateMachineRuntimeError::Timeout
                .to_string()
                .contains("deadline")
        );

        // The bridge maps the same deadline to ErrorCode::Timeout.
        let bridged = ConsensusRuntime::propose_and_wait(
            &one,
            3_u64.to_le_bytes().to_vec(),
            Duration::from_millis(50),
        )
        .await;
        assert!(matches!(
            bridged,
            Err(ref error) if error.code() == ErrorCode::Timeout
        ));

        one.shutdown();
        two.shutdown();
        one.join().await.expect("node one must stop cleanly");
        two.join().await.expect("node two must stop cleanly");
    });
}
