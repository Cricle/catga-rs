//! Dynamic membership contracts for the Raft state-machine runtime.
//!
//! These tests cover joint-consensus-free, one-at-a-time voter changes: adding
//! a voter to a running single-node cluster, removing it again, and the durable
//! precedence rule that a persisted membership wins over the bootstrap
//! configuration after a restart.

#[path = "common/members.rs"]
mod members;
#[path = "common/recording_machine.rs"]
mod recording_machine;
#[path = "common/sink_transport.rs"]
mod sink_transport;

use std::{
    collections::HashMap,
    io,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use catga_cluster::{
    ClusterCoordinator, RaftMember, RaftMessage, RaftNode, RaftNodeError, RaftStateMachineDriver,
    RaftStateMachineRuntime, RaftStateMachineRuntimeError, RaftTransport, RaftTransportError,
    RaftTransportResult,
};
use tokio::sync::{RwLock, mpsc};

use members::single_member;
use recording_machine::RecordingMachine;
use sink_transport::SinkTransport;

const WAIT_TIMEOUT: Duration = Duration::from_secs(5);

fn two_members() -> Vec<RaftMember> {
    vec![
        RaftMember::new(1, "http://node-1"),
        RaftMember::new(2, "http://node-2"),
    ]
}

fn current_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test Tokio runtime must build")
}

/// Routes Raft messages between runtimes over in-process channels, keyed by
/// the destination member identifier.
#[derive(Clone, Default)]
struct ChannelTransport {
    routes: Arc<RwLock<HashMap<u64, mpsc::Sender<RaftMessage>>>>,
}

impl ChannelTransport {
    async fn register(&self, runtime: &RaftStateMachineRuntime) {
        self.routes
            .write()
            .await
            .insert(runtime.id(), runtime.inbox());
    }
}

#[async_trait]
impl RaftTransport for ChannelTransport {
    async fn send(&self, message: RaftMessage) -> RaftTransportResult {
        let route = self
            .routes
            .read()
            .await
            .get(&message.to)
            .cloned()
            .ok_or_else(|| RaftTransportError::fatal(io::Error::other("unknown peer")))?;
        route
            .send(message)
            .await
            .map_err(|_| RaftTransportError::retryable(io::Error::other("peer stopped")))
    }
}

fn spawn_persistent_runtime(
    directory: &std::path::Path,
    applied: Arc<AtomicU64>,
    transport: &ChannelTransport,
) -> RaftStateMachineRuntime {
    let node = RaftNode::open_persistent(1, "http://node-1", single_member(), directory)
        .expect("persistent node must open");
    let driver = RaftStateMachineDriver::new(
        node,
        RecordingMachine::new(applied, Arc::new(AtomicUsize::new(0))),
    )
    .expect("driver must construct");
    RaftStateMachineRuntime::spawn(
        driver,
        Arc::new(transport.clone()),
        Duration::from_millis(1),
    )
    .expect("runtime must start")
}

fn spawn_in_memory_runtime(
    id: u64,
    endpoint: &str,
    members: Vec<RaftMember>,
    applied: Arc<AtomicU64>,
    transport: &ChannelTransport,
) -> RaftStateMachineRuntime {
    let node = RaftNode::new(id, endpoint, members).expect("node must construct");
    let driver = RaftStateMachineDriver::new(
        node,
        RecordingMachine::new(applied, Arc::new(AtomicUsize::new(0))),
    )
    .expect("driver must construct");
    RaftStateMachineRuntime::spawn(
        driver,
        Arc::new(transport.clone()),
        Duration::from_millis(1),
    )
    .expect("runtime must start")
}

async fn wait_until(description: &str, mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(WAIT_TIMEOUT, async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {description}"));
}

fn endpoints_of(runtime: &RaftStateMachineRuntime) -> Vec<String> {
    let mut endpoints: Vec<String> = runtime
        .coordinator()
        .member_endpoints()
        .iter()
        .map(|endpoint| endpoint.to_string())
        .collect();
    endpoints.sort();
    endpoints
}

#[test]
fn add_voter_replicates_and_restart_preserves_the_new_membership() {
    current_thread_runtime().block_on(async {
        let directory = tempfile::tempdir().expect("temporary raft directory");
        let transport = ChannelTransport::default();

        let applied_one = Arc::new(AtomicU64::new(0));
        let runtime_one =
            spawn_persistent_runtime(directory.path(), Arc::clone(&applied_one), &transport);
        transport.register(&runtime_one).await;
        runtime_one
            .campaign()
            .await
            .expect("single node must elect itself");

        // The joining node is spawned with the full intended membership; it
        // becomes a voter only once the committed conf change reaches it.
        let applied_two = Arc::new(AtomicU64::new(0));
        let runtime_two = spawn_in_memory_runtime(
            2,
            "http://node-2",
            two_members(),
            Arc::clone(&applied_two),
            &transport,
        );
        transport.register(&runtime_two).await;

        runtime_one
            .add_voter(2, "http://node-2".to_string())
            .await
            .expect("the leader must accept an add_voter proposal");
        wait_until("both coordinators to observe both members", || {
            endpoints_of(&runtime_one) == ["http://node-1", "http://node-2"]
                && endpoints_of(&runtime_two) == ["http://node-1", "http://node-2"]
        })
        .await;

        runtime_one
            .propose(7_u64.to_le_bytes())
            .await
            .expect("proposal must be accepted by the leader");
        wait_until("the write to replicate to both state machines", || {
            applied_one.load(Ordering::Acquire) == 7 && applied_two.load(Ordering::Acquire) == 7
        })
        .await;

        // Checkpointing compacts the conf-change entry away, so the restart
        // below also proves the durable snapshot carries the new membership.
        runtime_one
            .checkpoint()
            .await
            .expect("applied state must checkpoint through the owner");

        // Restart node 1 with the unchanged bootstrap configuration; the
        // persisted membership must win over the configured single member.
        runtime_one.shutdown();
        runtime_one.join().await.expect("owner must stop cleanly");
        let restarted_one = Arc::new(AtomicU64::new(0));
        let runtime_one =
            spawn_persistent_runtime(directory.path(), Arc::clone(&restarted_one), &transport);
        transport.register(&runtime_one).await;
        assert_eq!(
            endpoints_of(&runtime_one),
            ["http://node-1", "http://node-2"],
            "restarted node must expose the persisted membership, not the bootstrap config"
        );
        assert_eq!(
            restarted_one.load(Ordering::Acquire),
            7,
            "recovery must restore the checkpointed state"
        );

        runtime_one
            .campaign()
            .await
            .expect("restarted node must be able to campaign");
        let leader = tokio::time::timeout(WAIT_TIMEOUT, async {
            loop {
                if runtime_one.coordinator().is_leader() {
                    break &runtime_one;
                }
                if runtime_two.coordinator().is_leader() {
                    break &runtime_two;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the two-voter cluster must elect a leader after the restart");
        leader
            .propose(5_u64.to_le_bytes())
            .await
            .expect("proposal must be accepted after the restart");
        wait_until("the post-restart write to replicate to both nodes", || {
            restarted_one.load(Ordering::Acquire) == 12 && applied_two.load(Ordering::Acquire) == 12
        })
        .await;

        runtime_one.shutdown();
        runtime_one.join().await.expect("owner must stop cleanly");
        runtime_two.shutdown();
        runtime_two.join().await.expect("owner must stop cleanly");
    });
}

#[test]
fn remove_voter_restores_single_node_and_restart_keeps_it() {
    current_thread_runtime().block_on(async {
        let directory = tempfile::tempdir().expect("temporary raft directory");
        let transport = ChannelTransport::default();

        let applied_one = Arc::new(AtomicU64::new(0));
        let runtime_one =
            spawn_persistent_runtime(directory.path(), Arc::clone(&applied_one), &transport);
        transport.register(&runtime_one).await;
        runtime_one
            .campaign()
            .await
            .expect("single node must elect itself");

        let applied_two = Arc::new(AtomicU64::new(0));
        let runtime_two = spawn_in_memory_runtime(
            2,
            "http://node-2",
            two_members(),
            Arc::clone(&applied_two),
            &transport,
        );
        transport.register(&runtime_two).await;

        runtime_one
            .add_voter(2, "http://node-2".to_string())
            .await
            .expect("the leader must accept an add_voter proposal");
        wait_until("both coordinators to observe both members", || {
            endpoints_of(&runtime_one).len() == 2 && endpoints_of(&runtime_two).len() == 2
        })
        .await;

        runtime_one
            .remove_voter(2)
            .await
            .expect("the leader must accept a remove_voter proposal");
        // The remaining cluster observes the removal. The removed node itself
        // never learns the final commit: raft-rs deletes its replication
        // progress when the removal applies, so it stops receiving messages.
        wait_until(
            "the surviving coordinator to drop the removed member",
            || endpoints_of(&runtime_one) == ["http://node-1"],
        )
        .await;

        // A cluster reduced to one voter commits new writes on its own.
        runtime_two.shutdown();
        runtime_two.join().await.expect("owner must stop cleanly");
        runtime_one
            .propose(3_u64.to_le_bytes())
            .await
            .expect("single voter must accept proposals again");
        wait_until("the single-voter write to apply", || {
            applied_one.load(Ordering::Acquire) == 3
        })
        .await;

        runtime_one.shutdown();
        runtime_one.join().await.expect("owner must stop cleanly");
        let restarted_one = Arc::new(AtomicU64::new(0));
        let runtime_one =
            spawn_persistent_runtime(directory.path(), Arc::clone(&restarted_one), &transport);
        assert_eq!(
            endpoints_of(&runtime_one),
            ["http://node-1"],
            "restarted node must keep the reduced persisted membership"
        );
        assert!(
            runtime_one
                .coordinator()
                .wait_for_leadership(WAIT_TIMEOUT)
                .await,
            "single-voter cluster must elect itself after a restart"
        );
        runtime_one
            .propose(4_u64.to_le_bytes())
            .await
            .expect("restarted single voter must accept proposals");
        wait_until("the post-restart write to apply", || {
            restarted_one.load(Ordering::Acquire) == 7
        })
        .await;

        runtime_one.shutdown();
        runtime_one.join().await.expect("owner must stop cleanly");
    });
}

#[test]
fn conf_change_proposals_on_a_non_leader_are_routine_caller_errors() {
    current_thread_runtime().block_on(async {
        let node = RaftNode::new(1, "http://node-1", two_members()).expect("node must construct");
        let driver = RaftStateMachineDriver::new(
            node,
            RecordingMachine::new(Arc::new(AtomicU64::new(0)), Arc::new(AtomicUsize::new(0))),
        )
        .expect("driver must construct");
        let runtime = RaftStateMachineRuntime::spawn(
            driver,
            Arc::new(SinkTransport),
            Duration::from_millis(1),
        )
        .expect("runtime must start");

        let result = runtime.add_voter(3, "http://node-3".to_string()).await;
        assert!(
            matches!(
                result,
                Err(RaftStateMachineRuntimeError::Raft(
                    raft::Error::ProposalDropped
                ))
            ),
            "add_voter without leadership must surface ProposalDropped, got {result:?}"
        );
        assert!(
            runtime.is_alive(),
            "a routine caller error must not stop the owner"
        );
        assert!(runtime.stop_reason().is_none());

        let result = runtime.remove_voter(2).await;
        assert!(
            matches!(
                result,
                Err(RaftStateMachineRuntimeError::Raft(
                    raft::Error::ProposalDropped
                ))
            ),
            "remove_voter without leadership must surface ProposalDropped, got {result:?}"
        );
        assert!(runtime.is_alive());

        let result = runtime.add_voter(0, "http://node-0".to_string()).await;
        assert!(
            matches!(
                result,
                Err(RaftStateMachineRuntimeError::Node(
                    RaftNodeError::ZeroMemberId
                ))
            ),
            "member id zero must be rejected as a caller error, got {result:?}"
        );
        assert!(runtime.is_alive());

        let result = runtime.add_voter(2, "http://node-2".to_string()).await;
        assert!(
            matches!(
                result,
                Err(RaftStateMachineRuntimeError::Raft(
                    raft::Error::ConfChangeError(_)
                ))
            ),
            "adding an existing voter must be rejected as a caller error, got {result:?}"
        );
        assert!(runtime.is_alive());

        // The owner task remains responsive to unrelated commands.
        runtime
            .applied_index()
            .await
            .expect("owner must keep serving routine commands");

        runtime.shutdown();
        runtime.join().await.expect("owner must stop cleanly");
    });
}
