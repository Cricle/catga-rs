//! Failover latency contract: surviving voters must elect a new leader promptly
//! after the leader stops, without waiting for the dead node to return.

#[path = "common/channel_transport.rs"]
mod channel_transport;
#[path = "common/recording_machine.rs"]
mod recording_machine;

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize},
    },
    time::{Duration, Instant},
};

use catga_cluster::{
    ClusterCoordinator, RaftNode, RaftStateMachineDriver, RaftStateMachineRuntime,
};

use channel_transport::ChannelTransport;
use recording_machine::RecordingMachine;

/// Routes live sends through the channel hub but hangs sends to node 1 once armed,
/// mimicking a dead peer whose pooled connection stalls instead of refusing fast.
#[derive(Clone)]
struct HangingPeerTransport {
    hub: ChannelTransport,
    hang_dead_peer: Arc<std::sync::atomic::AtomicBool>,
}

#[async_trait::async_trait]
impl catga_cluster::RaftTransport for HangingPeerTransport {
    async fn send(
        &self,
        message: catga_cluster::RaftMessage,
    ) -> catga_cluster::RaftTransportResult {
        if message.to == 1
            && self
                .hang_dead_peer
                .load(std::sync::atomic::Ordering::Acquire)
        {
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
        self.hub.send(message).await
    }
}

fn three_members() -> Vec<catga_cluster::RaftMember> {
    vec![
        catga_cluster::RaftMember::new(1, "http://node-1"),
        catga_cluster::RaftMember::new(2, "http://node-2"),
        catga_cluster::RaftMember::new(3, "http://node-3"),
    ]
}

fn spawn_node(
    id: u64,
    members: &[catga_cluster::RaftMember],
    transport: &ChannelTransport,
) -> RaftStateMachineRuntime {
    let node = RaftNode::new(id, format!("http://node-{id}"), members.to_vec())
        .expect("node must construct");
    let driver = RaftStateMachineDriver::new(
        node,
        RecordingMachine::new(Arc::new(AtomicU64::new(0)), Arc::new(AtomicUsize::new(0))),
    )
    .expect("driver must construct");
    RaftStateMachineRuntime::spawn(
        driver,
        Arc::new(transport.clone()),
        Duration::from_millis(10),
    )
    .expect("runtime must start")
}

#[test]
fn survivors_elect_a_new_leader_promptly_after_leader_stops() {
    let test_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test Tokio runtime must build");

    test_runtime.block_on(async {
        let transport = ChannelTransport::default();
        let members = three_members();
        let mut runtimes: Vec<_> = members
            .iter()
            .map(|member| spawn_node(member.id(), &members, &transport))
            .collect();
        for runtime in &runtimes {
            transport.register(runtime).await;
        }

        runtimes[0]
            .campaign()
            .await
            .expect("node 1 starts the election");
        let established = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if runtimes
                    .iter()
                    .all(|r| r.coordinator().leader_endpoint().as_deref() == Some("http://node-1"))
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(established.is_ok(), "initial election completes");

        // The leader dies: its runtime stops and its inbox closes.
        let leader = runtimes.remove(0);
        leader.shutdown();
        leader.join().await.expect("leader owner stops cleanly");

        // The two survivors must elect within a few election timeouts, not tens
        // of seconds.
        let started = Instant::now();
        let elected = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                for runtime in &runtimes {
                    if runtime.coordinator().is_leader() {
                        return runtime.id();
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        let elapsed = started.elapsed();
        assert!(
            elected.is_ok(),
            "survivors must elect a new leader, none after {elapsed:.1?}"
        );
        assert!(
            elapsed < Duration::from_secs(3),
            "failover election took {elapsed:.1?}, far beyond the election timeout"
        );

        while let Some(runtime) = runtimes.pop() {
            runtime.shutdown();
            runtime.join().await.expect("owner must stop cleanly");
        }
    });
}

#[test]
fn election_survives_a_peer_whose_sends_hang() {
    let test_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test Tokio runtime must build");

    test_runtime.block_on(async {
        let hub = ChannelTransport::default();
        let hang_dead_peer = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let transport = HangingPeerTransport {
            hub: hub.clone(),
            hang_dead_peer: Arc::clone(&hang_dead_peer),
        };
        let members = three_members();
        // Node 1 exists so the initial election can complete; its sends hang
        // afterwards, exactly like a killed peer on a stalled pooled connection.
        let mut runtimes: Vec<_> = members
            .iter()
            .map(|member| {
                let node = RaftNode::new(
                    member.id(),
                    format!("http://node-{}", member.id()),
                    members.clone(),
                )
                .expect("node must construct");
                let driver = RaftStateMachineDriver::new(
                    node,
                    RecordingMachine::new(
                        Arc::new(AtomicU64::new(0)),
                        Arc::new(AtomicUsize::new(0)),
                    ),
                )
                .expect("driver must construct");
                RaftStateMachineRuntime::spawn(
                    driver,
                    Arc::new(transport.clone()),
                    Duration::from_millis(10),
                )
                .expect("runtime must start")
            })
            .collect();
        for runtime in &runtimes {
            hub.register(runtime).await;
        }

        runtimes[0]
            .campaign()
            .await
            .expect("node 1 starts the election");
        let established = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if runtimes
                    .iter()
                    .all(|r| r.coordinator().leader_endpoint().as_deref() == Some("http://node-1"))
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(established.is_ok(), "initial election completes");

        // Node 1 dies: its runtime stops, its inbox closes, and from now on every
        // send toward it stalls for 30s (the HangingPeerTransport hangs on to==1).
        let leader = runtimes.remove(0);
        leader.shutdown();
        leader.join().await.expect("leader owner stops cleanly");
        hang_dead_peer.store(true, std::sync::atomic::Ordering::Release);

        // Without the non-blocking dispatch, each stalled send would freeze the
        // survivors' Raft clocks and the election would take tens of seconds.
        let started = Instant::now();
        let elected = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                for runtime in &runtimes {
                    if runtime.coordinator().is_leader() {
                        return runtime.id();
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        let elapsed = started.elapsed();
        assert!(
            elected.is_ok(),
            "survivors must elect despite sends to the dead leader hanging"
        );
        assert!(
            elapsed < Duration::from_secs(3),
            "election with a hanging dead peer took {elapsed:.1?}"
        );

        while let Some(runtime) = runtimes.pop() {
            runtime.shutdown();
            runtime.join().await.expect("owner must stop cleanly");
        }
    });
}
