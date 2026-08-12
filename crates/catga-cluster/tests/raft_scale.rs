//! Scale contracts: ten- and fifty-voter clusters must converge on one leader,
//! replicate every committed write to all voters, keep electing after losing
//! the leader, refuse writes below quorum, and heal when a wiped node rejoins.

#[path = "common/channel_transport.rs"]
mod channel_transport;
#[path = "common/recording_machine.rs"]
mod recording_machine;

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use catga_cluster::{
    ClusterCoordinator, RaftMember, RaftNode, RaftStateMachineDriver, RaftStateMachineRuntime,
};

use channel_transport::ChannelTransport;
use recording_machine::RecordingMachine;

const TICK: Duration = Duration::from_millis(10);

fn voters(count: u64) -> Vec<RaftMember> {
    (1..=count)
        .map(|id| RaftMember::new(id, format!("http://node-{id}")))
        .collect()
}

fn spawn_node(
    id: u64,
    members: &[RaftMember],
    transport: &ChannelTransport,
    applied: Arc<AtomicU64>,
) -> RaftStateMachineRuntime {
    let node = RaftNode::new(id, format!("http://node-{id}"), members.to_vec())
        .expect("node must construct");
    let driver = RaftStateMachineDriver::new(
        node,
        RecordingMachine::new(applied, Arc::new(AtomicUsize::new(0))),
    )
    .expect("driver must construct");
    RaftStateMachineRuntime::spawn(driver, Arc::new(transport.clone()), TICK)
        .expect("runtime must start")
}

async fn stop_all(nodes: Vec<RaftStateMachineRuntime>) {
    for runtime in &nodes {
        runtime.shutdown();
    }
    for runtime in nodes {
        runtime.join().await.expect("owner must stop cleanly");
    }
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

async fn wait_for_applied(applied: &[Arc<AtomicU64>], expected: u64) {
    let reached = tokio::time::timeout(Duration::from_secs(15), async {
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
        "every voter must apply the expected sum {expected}, got {:?}",
        applied
            .iter()
            .map(|sum| sum.load(Ordering::Acquire))
            .collect::<Vec<_>>()
    );
}

async fn boot_cluster(
    count: u64,
) -> (
    ChannelTransport,
    Vec<RaftMember>,
    Vec<RaftStateMachineRuntime>,
    Vec<Arc<AtomicU64>>,
) {
    let transport = ChannelTransport::default();
    let members = voters(count);
    let applied: Vec<Arc<AtomicU64>> = (0..count).map(|_| Arc::new(AtomicU64::new(0))).collect();
    let runtimes: Vec<_> = members
        .iter()
        .enumerate()
        .map(|(index, member)| {
            spawn_node(
                member.id(),
                &members,
                &transport,
                Arc::clone(&applied[index]),
            )
        })
        .collect();
    for runtime in &runtimes {
        transport.register(runtime).await;
    }
    (transport, members, runtimes, applied)
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

#[test]
fn ten_voters_elect_replicate_and_reelect_after_leader_loss() {
    let test_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test Tokio runtime must build");

    test_runtime.block_on(async {
        let (_transport, _members, mut runtimes, applied) = boot_cluster(10).await;
        campaign_first_node(&runtimes).await;

        for value in 1..=5_u64 {
            runtimes[0]
                .propose(value.to_le_bytes())
                .await
                .expect("leader proposal must succeed");
        }
        wait_for_applied(&applied, 15).await;

        stop_all(vec![runtimes.remove(0)]).await;

        let started = Instant::now();
        let leader_index = wait_for_leader(&runtimes, Duration::from_secs(10)).await;
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(5),
            "ten-voter failover took {elapsed:.1?}, far beyond the election timeout"
        );

        runtimes[leader_index]
            .propose(10_u64.to_le_bytes())
            .await
            .expect("new leader proposal must succeed");
        wait_for_applied(&applied[1..], 25).await;

        stop_all(runtimes).await;
    });
}

#[test]
fn fifty_voters_replicate_within_a_latency_budget() {
    let test_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test Tokio runtime must build");

    test_runtime.block_on(async {
        let (_transport, _members, runtimes, applied) = boot_cluster(50).await;
        campaign_first_node(&runtimes).await;

        // Sequential single-client writes, matching the measured-baseline style
        // in the changelog; quorum here is 26 of 50 acknowledgements.
        let started = Instant::now();
        for value in 1..=20_u64 {
            runtimes[0]
                .propose(value.to_le_bytes())
                .await
                .expect("leader proposal must succeed");
        }
        let average = started.elapsed() / 20;
        println!("fifty-voter average propose latency: {average:.1?}");
        assert!(
            average < Duration::from_secs(1),
            "fifty-voter propose latency {average:.1?} blew the generous budget"
        );

        wait_for_applied(&applied, 210).await;
        stop_all(runtimes).await;
    });
}

#[test]
fn fifty_voters_failover_quorum_loss_and_wiped_node_rejoin() {
    let test_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test Tokio runtime must build");

    test_runtime.block_on(async {
        let (transport, members, mut runtimes, mut applied) = boot_cluster(50).await;
        campaign_first_node(&runtimes).await;

        for value in 1..=5_u64 {
            runtimes[0]
                .propose(value.to_le_bytes())
                .await
                .expect("leader proposal must succeed");
        }
        wait_for_applied(&applied, 15).await;

        // The leader dies: 49 survivors (quorum is 26) re-elect promptly.
        stop_all(vec![runtimes.remove(0)]).await;
        let mut alive_applied: Vec<_> = applied.split_off(1);
        let started = Instant::now();
        let leader_index = wait_for_leader(&runtimes, Duration::from_secs(10)).await;
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "fifty-voter failover must stay within a few election timeouts"
        );
        runtimes[leader_index]
            .propose(10_u64.to_le_bytes())
            .await
            .expect("new leader proposal must succeed");
        wait_for_applied(&alive_applied, 25).await;

        // 24 more die: 25 survivors are one short of quorum. check_quorum makes
        // a surviving leader step down and pre-vote blocks a new election, so
        // the cluster must become safely unavailable instead of split-brain.
        let doomed: Vec<_> = runtimes.drain(0..24).collect();
        alive_applied.drain(0..24);
        stop_all(doomed).await;
        let leaderless = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if runtimes.iter().all(|r| !r.coordinator().is_leader()) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(
            leaderless.is_ok(),
            "below quorum every voter must step down from leadership"
        );
        // propose() Ok only means "locally accepted"; the safety property is
        // that nothing commits below quorum, so every survivor's applied sum
        // must stay frozen through the window.
        let _ = runtimes[0].propose(1_u64.to_le_bytes()).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            alive_applied
                .iter()
                .all(|sum| sum.load(Ordering::Acquire) == 25),
            "below quorum no write may commit, got {:?}",
            alive_applied
                .iter()
                .map(|sum| sum.load(Ordering::Acquire))
                .collect::<Vec<_>>()
        );

        // Two wiped replacements rejoin with their old ids: 27 votes are back,
        // one above quorum. Rejoining just one would leave the cluster at
        // exactly quorum (26 of 50), where every election needs a unanimous
        // vote and concurrent candidates split-fatally — an inherent Raft
        // zero-margin livelock, so recovery must always restore margin. The
        // fresh nodes replay the entire log to catch up.
        for id in [2_u64, 3] {
            let replacement_applied = Arc::new(AtomicU64::new(0));
            let replacement =
                spawn_node(id, &members, &transport, Arc::clone(&replacement_applied));
            transport.register(&replacement).await;
            runtimes.push(replacement);
            alive_applied.push(replacement_applied);
        }

        let healed_leader = wait_for_leader(&runtimes, Duration::from_secs(15)).await;
        runtimes[healed_leader]
            .propose(100_u64.to_le_bytes())
            .await
            .expect("healed cluster proposal must succeed");
        // Two wiped rejoiners replay the whole log while the leader replicates
        // the healed write; allow ample time under shared-CPU load.
        let healed = tokio::time::timeout(Duration::from_secs(45), async {
            loop {
                if alive_applied
                    .iter()
                    .all(|sum| sum.load(Ordering::Acquire) == 125)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(
            healed.is_ok(),
            "healed cluster must converge every voter to 125, got {:?}",
            alive_applied
                .iter()
                .map(|sum| sum.load(Ordering::Acquire))
                .collect::<Vec<_>>()
        );

        stop_all(runtimes).await;
    });
}
