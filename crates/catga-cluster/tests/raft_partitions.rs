//! Abnormal-network scenario contracts for multi-voter Raft clusters.
//!
//! A scripted fault transport partitions voter sets, drops every Nth message
//! to one voter, and delays delivery to one voter — all deterministically.
//! Across these faults the quorum side must keep electing and committing, cut
//! or slowed voters must converge once healed, every entry applied before a
//! mid-replication leader kill must survive the re-election, and voter
//! add/remove under a write stream must leave every member equally applied.

#[path = "common/channel_transport.rs"]
mod channel_transport;
#[path = "common/fault_transport.rs"]
mod fault_transport;
#[path = "common/recording_machine.rs"]
mod recording_machine;
#[path = "common/sequence_machine.rs"]
mod sequence_machine;

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use catga_cluster::{
    ClusterCoordinator, RaftMember, RaftNode, RaftStateMachine, RaftStateMachineDriver,
    RaftStateMachineRuntime, RaftStateMachineRuntimeError,
};

use channel_transport::ChannelTransport;
use fault_transport::FaultTransport;
use recording_machine::RecordingMachine;
use sequence_machine::SequenceMachine;

const TICK: Duration = Duration::from_millis(10);
const APPLY_BUDGET: Duration = Duration::from_secs(12);

fn voters(count: u64) -> Vec<RaftMember> {
    (1..=count)
        .map(|id| RaftMember::new(id, format!("http://node-{id}")))
        .collect()
}

fn current_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test Tokio runtime must build")
}

fn spawn_node<M>(
    id: u64,
    members: &[RaftMember],
    transport: &FaultTransport,
    machine: M,
) -> RaftStateMachineRuntime
where
    M: RaftStateMachine + Send + 'static,
{
    let node = RaftNode::new(id, format!("http://node-{id}"), members.to_vec())
        .expect("node must construct");
    let driver = RaftStateMachineDriver::new(node, machine).expect("driver must construct");
    RaftStateMachineRuntime::spawn(driver, Arc::new(transport.clone()), TICK)
        .expect("runtime must start")
}

fn recording_machine(applied: &Arc<AtomicU64>) -> RecordingMachine {
    RecordingMachine::new(Arc::clone(applied), Arc::new(AtomicUsize::new(0)))
}

async fn boot_cluster(
    transport: &FaultTransport,
    count: u64,
) -> (
    Vec<RaftMember>,
    Vec<RaftStateMachineRuntime>,
    Vec<Arc<AtomicU64>>,
) {
    let members = voters(count);
    let applied: Vec<Arc<AtomicU64>> = (0..count).map(|_| Arc::new(AtomicU64::new(0))).collect();
    let mut runtimes = Vec::with_capacity(members.len());
    for (index, member) in members.iter().enumerate() {
        let runtime = spawn_node(
            member.id(),
            &members,
            transport,
            recording_machine(&applied[index]),
        );
        transport.register(&runtime).await;
        runtimes.push(runtime);
    }
    (members, runtimes, applied)
}

async fn stop_all(nodes: Vec<RaftStateMachineRuntime>) {
    for runtime in &nodes {
        runtime.shutdown();
    }
    for runtime in nodes {
        runtime.join().await.expect("owner must stop cleanly");
    }
}

async fn campaign_first_node(runtimes: &[RaftStateMachineRuntime]) {
    runtimes[0]
        .campaign()
        .await
        .expect("node 1 starts the election");
    let converged = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if runtimes.iter().all(|runtime| {
                runtime.coordinator().leader_endpoint().as_deref() == Some("http://node-1")
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(converged.is_ok(), "all voters must converge on the leader");
}

async fn wait_for_leader(runtimes: &[RaftStateMachineRuntime], budget: Duration) -> usize {
    let elected = tokio::time::timeout(budget, async {
        loop {
            for (index, runtime) in runtimes.iter().enumerate() {
                if runtime.coordinator().is_leader() {
                    return index;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    elected.expect("a leader must emerge within the budget")
}

fn leader_index(runtimes: &[RaftStateMachineRuntime]) -> Option<usize> {
    runtimes
        .iter()
        .position(|runtime| runtime.coordinator().is_leader())
}

fn applied_sums(applied: &[Arc<AtomicU64>]) -> Vec<u64> {
    applied
        .iter()
        .map(|sum| sum.load(Ordering::Acquire))
        .collect()
}

async fn wait_for_applied(applied: &[Arc<AtomicU64>], expected: u64, budget: Duration) {
    let reached = tokio::time::timeout(budget, async {
        loop {
            if applied
                .iter()
                .all(|sum| sum.load(Ordering::Acquire) == expected)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(
        reached.is_ok(),
        "every observed voter must apply the expected sum {expected}, got {:?}",
        applied_sums(applied)
    );
}

async fn wait_for_sequence(sequence: &Arc<Mutex<Vec<u64>>>, expected: &[u64], budget: Duration) {
    let reached = tokio::time::timeout(budget, async {
        loop {
            if sequence
                .lock()
                .expect("applied sequence poisoned")
                .as_slice()
                == expected
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(
        reached.is_ok(),
        "the exact applied sequence {expected:?} must appear, got {:?}",
        sequence.lock().expect("applied sequence poisoned")
    );
}

async fn wait_for_sequences_to_match(
    left: &Arc<Mutex<Vec<u64>>>,
    right: &Arc<Mutex<Vec<u64>>>,
    budget: Duration,
) {
    let reached = tokio::time::timeout(budget, async {
        loop {
            if *left.lock().expect("applied sequence poisoned")
                == *right.lock().expect("applied sequence poisoned")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(
        reached.is_ok(),
        "surviving voters must converge on one applied sequence, got {:?} vs {:?}",
        left.lock().expect("applied sequence poisoned"),
        right.lock().expect("applied sequence poisoned")
    );
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

async fn wait_for_membership(runtimes: &[&RaftStateMachineRuntime], expected: usize) {
    let reached = tokio::time::timeout(APPLY_BUDGET, async {
        loop {
            if runtimes
                .iter()
                .all(|runtime| endpoints_of(runtime).len() == expected)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(
        reached.is_ok(),
        "every observed coordinator must converge on {expected} members"
    );
}

#[test]
fn partitioned_majority_keeps_writing_and_healed_minority_catches_up() {
    current_thread_runtime().block_on(async {
        let transport = FaultTransport::new(ChannelTransport::default());
        let (_members, runtimes, applied) = boot_cluster(&transport, 5).await;
        campaign_first_node(&runtimes).await;

        for value in 1..=5_u64 {
            runtimes[0]
                .propose(value.to_le_bytes())
                .await
                .expect("leader proposal must succeed");
        }
        wait_for_applied(&applied, 15, APPLY_BUDGET).await;

        // Cut {1,2,3} away from {4,5}: the leader sits in the majority side.
        transport.partition_between(&[1, 2, 3], &[4, 5]);
        for value in 6..=10_u64 {
            runtimes[0]
                .propose(value.to_le_bytes())
                .await
                .expect("the majority-side leader must keep accepting writes");
        }
        wait_for_applied(&applied[..3], 55, APPLY_BUDGET).await;

        // Even after a generous settle window the minority must stay
        // leaderless (pre-vote cannot reach quorum) and commit nothing new.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            runtimes[3..]
                .iter()
                .all(|runtime| !runtime.coordinator().is_leader()),
            "a two-voter minority must never elect a leader"
        );
        assert!(
            applied_sums(&applied[3..]).iter().all(|sum| *sum == 15),
            "the minority must not commit writes made across the cut, got {:?}",
            applied_sums(&applied[3..])
        );
        assert!(
            runtimes[0].coordinator().is_leader(),
            "the majority side must retain its leader through the partition"
        );
        assert!(
            transport.severed_messages() > 0,
            "the partition must actually sever cross-set traffic"
        );

        // Healing lets the leader's probes reach the minority again; the
        // heartbeat response resumes its progress and it replays the gap.
        transport.heal();
        wait_for_applied(&applied, 55, APPLY_BUDGET).await;

        let leader = wait_for_leader(&runtimes, Duration::from_secs(5)).await;
        runtimes[leader]
            .propose(100_u64.to_le_bytes())
            .await
            .expect("post-heal proposal must succeed");
        wait_for_applied(&applied, 155, APPLY_BUDGET).await;

        stop_all(runtimes).await;
    });
}

#[test]
fn losing_every_third_message_to_one_voter_still_converges() {
    current_thread_runtime().block_on(async {
        let transport =
            FaultTransport::new(ChannelTransport::default()).dropping_every_nth_to(3, 3);
        let (_members, runtimes, applied) = boot_cluster(&transport, 3).await;
        campaign_first_node(&runtimes).await;

        for value in 1..=10_u64 {
            runtimes[0]
                .propose(value.to_le_bytes())
                .await
                .expect("leader proposal must succeed");
        }

        // Quorum (two of three) never depends on the lossy voter.
        wait_for_applied(&applied[..2], 55, APPLY_BUDGET).await;
        // The lossy voter catches up through heartbeat-driven retries.
        wait_for_applied(&applied, 55, APPLY_BUDGET).await;
        assert!(
            transport.dropped_messages() > 0,
            "the deterministic loss must actually drop traffic to node 3"
        );

        stop_all(runtimes).await;
    });
}

#[test]
fn delayed_follower_lags_but_the_leader_commits_at_quorum_speed() {
    current_thread_runtime().block_on(async {
        let transport = FaultTransport::new(ChannelTransport::default())
            .delaying_sends_to(3, Duration::from_millis(100));
        let (_members, runtimes, applied) = boot_cluster(&transport, 3).await;
        campaign_first_node(&runtimes).await;

        let started = Instant::now();
        for value in 1..=10_u64 {
            runtimes[0]
                .propose(value.to_le_bytes())
                .await
                .expect("leader proposal must succeed");
        }
        wait_for_applied(&applied[..2], 55, Duration::from_secs(5)).await;
        let quorum_elapsed = started.elapsed();
        assert!(
            quorum_elapsed < Duration::from_secs(2),
            "quorum commits must not wait on the delayed follower, took {quorum_elapsed:.1?}"
        );

        // Every send toward node 3 is held by 100ms, so at the moment the
        // quorum finishes, node 3 structurally trails it by at least the last
        // replication round; the margin is far wider than the polling loop.
        let delayed_sum = applied[2].load(Ordering::Acquire);
        assert!(
            delayed_sum < 55,
            "the delayed follower must lag the quorum, got {delayed_sum}"
        );

        wait_for_applied(&applied, 55, APPLY_BUDGET).await;
        assert!(
            transport.delayed_messages() > 0,
            "the delay fault must actually hold traffic to node 3"
        );

        stop_all(runtimes).await;
    });
}

#[test]
fn writes_applied_before_a_mid_replication_leader_kill_survive_re_election() {
    current_thread_runtime().block_on(async {
        let transport = FaultTransport::new(ChannelTransport::default());
        let members = voters(3);
        let sequences: Vec<Arc<Mutex<Vec<u64>>>> = (0..3)
            .map(|_| Arc::new(Mutex::new(Vec::new())))
            .collect();
        let mut runtimes: Vec<_> = members
            .iter()
            .enumerate()
            .map(|(index, member)| {
                spawn_node(
                    member.id(),
                    &members,
                    &transport,
                    SequenceMachine::new(Arc::clone(&sequences[index])),
                )
            })
            .collect();
        for runtime in &runtimes {
            transport.register(runtime).await;
        }
        campaign_first_node(&runtimes).await;

        // Committed baseline: applied on the leader means a quorum holds it.
        for value in 1..=5_u64 {
            runtimes[0]
                .propose(value.to_le_bytes())
                .await
                .expect("leader proposal must succeed");
        }
        wait_for_sequence(&sequences[0], &[1, 2, 3, 4, 5], APPLY_BUDGET).await;
        let baseline: Vec<u64> = sequences[0]
            .lock()
            .expect("applied sequence poisoned")
            .clone();

        // More writes stream while the leader dies mid-replication: each was
        // accepted locally, but any of them may vanish with the dead leader.
        for value in 6..=15_u64 {
            let _ = runtimes[0].propose(value.to_le_bytes()).await;
        }
        let dead_leader = runtimes.remove(0);
        dead_leader.shutdown();
        dead_leader.join().await.expect("leader owner must stop cleanly");

        let new_leader = wait_for_leader(&runtimes, Duration::from_secs(10)).await;
        wait_for_sequences_to_match(&sequences[1], &sequences[2], APPLY_BUDGET).await;

        // No committed data loss: the pre-kill baseline survives as a prefix
        // of whatever the survivors converged on.
        for sequence in &sequences[1..] {
            let applied = sequence.lock().expect("applied sequence poisoned");
            assert!(
                applied.len() >= baseline.len() && applied[..baseline.len()] == baseline[..],
                "a survivor lost entries applied before the kill: baseline {baseline:?}, got {applied:?}"
            );
        }

        runtimes[new_leader]
            .propose(100_u64.to_le_bytes())
            .await
            .expect("the re-elected leader must accept writes");
        wait_for_sequences_to_match(&sequences[1], &sequences[2], APPLY_BUDGET).await;
        for sequence in &sequences[1..] {
            let applied = sequence.lock().expect("applied sequence poisoned");
            assert!(
                applied.len() > baseline.len() && applied[..baseline.len()] == baseline[..],
                "the post-election sequence must extend the pre-kill baseline, got {applied:?}"
            );
            assert_eq!(
                applied.last(),
                Some(&100),
                "the post-election write must apply on every survivor, got {applied:?}"
            );
        }

        stop_all(runtimes).await;
    });
}

#[test]
fn voter_changes_under_write_load_keep_every_voter_consistent() {
    current_thread_runtime().block_on(async {
        let transport = FaultTransport::new(ChannelTransport::default());
        let (_members, runtimes, applied) = boot_cluster(&transport, 5).await;
        campaign_first_node(&runtimes).await;
        let runtimes = Arc::new(runtimes);

        // The client streams writes through the whole scenario. Leadership
        // conflicts are routine caller errors and retried after re-resolving
        // the leader; any other error is client-visible and fails the test.
        let writer_runtimes = Arc::clone(&runtimes);
        let writer = tokio::spawn(async move {
            for value in 1..=40_u64 {
                loop {
                    let Some(leader) = leader_index(&writer_runtimes) else {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    };
                    match writer_runtimes[leader].propose(value.to_le_bytes()).await {
                        Ok(()) => break,
                        Err(RaftStateMachineRuntimeError::Raft(
                            raft::Error::ProposalDropped,
                        )) => {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                        Err(error) => panic!(
                            "writes must never surface a client error under membership churn, got {error:?}"
                        ),
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        // The sixth node boots with the full intended membership and becomes
        // a voter only once the committed conf change reaches it.
        let six_members = voters(6);
        let applied_six = Arc::new(AtomicU64::new(0));
        let runtime_six = spawn_node(
            6,
            &six_members,
            &transport,
            recording_machine(&applied_six),
        );
        transport.register(&runtime_six).await;

        // One change at a time: wait until the add is observable everywhere
        // before issuing the remove, per the documented conf-change discipline.
        loop {
            let Some(leader) = leader_index(&runtimes) else {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            };
            match runtimes[leader]
                .add_voter(6, "http://node-6".to_string())
                .await
            {
                Ok(()) => break,
                Err(RaftStateMachineRuntimeError::Raft(raft::Error::ProposalDropped)) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => panic!("add_voter must only fail as a routine conflict, got {error:?}"),
            }
        }
        let mut observed: Vec<&RaftStateMachineRuntime> = runtimes.iter().collect();
        observed.push(&runtime_six);
        wait_for_membership(&observed, 6).await;

        loop {
            let Some(leader) = leader_index(&runtimes) else {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            };
            match runtimes[leader].remove_voter(6).await {
                Ok(()) => break,
                Err(RaftStateMachineRuntimeError::Raft(raft::Error::ProposalDropped)) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => {
                    panic!("remove_voter must only fail as a routine conflict, got {error:?}")
                }
            }
        }
        // The removed voter never learns the final commit, so only the five
        // surviving coordinators are observed shrinking back to five members.
        let survivors: Vec<&RaftStateMachineRuntime> = runtimes.iter().collect();
        wait_for_membership(&survivors, 5).await;

        writer.await.expect("the writer task must complete");

        // 1 + ... + 40 = 820; every surviving voter applies the same stream.
        wait_for_applied(&applied, 820, APPLY_BUDGET).await;
        assert!(
            applied_six.load(Ordering::Acquire) > 0,
            "the sixth voter must have replicated part of the stream while a member"
        );

        runtime_six.shutdown();
        runtime_six
            .join()
            .await
            .expect("removed voter must stop cleanly");
        let runtimes =
            Arc::into_inner(runtimes).expect("the writer released its handle on the runtimes");
        stop_all(runtimes).await;
    });
}
