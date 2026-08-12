//! End-to-end smoke test for the sorock backend: a three-node in-process
//! cluster on one shared shard, write replication to every state machine, and
//! continued service after one node is killed.

#[path = "common/harness.rs"]
mod harness;
#[path = "common/probe.rs"]
mod probe;
#[path = "common/recording_machine.rs"]
mod recording_machine;

use std::sync::atomic::Ordering;

use catga_core::ConsensusRuntime;
use catga_sorock::SorockRuntime;
use harness::{loopback_config, wait_until};
use probe::MachineProbe;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_replicates_writes_and_survives_single_node_loss() {
    let probes: Vec<MachineProbe> = (0..3).map(|_| MachineProbe::default()).collect();

    // 1. Start three nodes; ports are OS-assigned via bind :0.
    let mut runtimes = Vec::new();
    for (i, probe) in probes.iter().enumerate() {
        let config = loopback_config(&format!("node-{}", i + 1));
        let runtime = SorockRuntime::start(config, probe.machine())
            .await
            .expect("sorock runtime must start");
        runtimes.push(runtime);
    }
    let uris: Vec<String> = runtimes
        .iter()
        .map(|rt| rt.advertised_uri().to_owned())
        .collect();

    // 2. Bootstrap: the first node adds itself, then joins the other two,
    // one membership change at a time.
    runtimes[0]
        .add_member(1, uris[0].clone())
        .await
        .expect("self add must bootstrap the group");
    runtimes[0]
        .add_member(2, uris[1].clone())
        .await
        .expect("second node must join through the leader");
    runtimes[0]
        .add_member(3, uris[2].clone())
        .await
        .expect("third node must join through the leader");

    // 3. Propose writes on the bootstrap node and wait until every machine
    // applied them.
    let first_batch: u64 = 5;
    for value in 1..=first_batch {
        runtimes[0]
            .propose(value.to_le_bytes().to_vec())
            .await
            .expect("proposal must succeed on a healthy group");
    }
    let expected: u64 = (1..=first_batch).sum();
    for (i, probe) in probes.iter().enumerate() {
        wait_until(&format!("node {} to apply the first batch", i + 1), || {
            probe.sum.load(Ordering::Acquire) == expected
        })
        .await;
    }

    // The leader-side applied counter must cover every proposed entry.
    let applied_index = runtimes[0]
        .applied_index()
        .await
        .expect("applied index must be readable");
    assert!(applied_index > first_batch);

    // 4. Kill the third node; the remaining two keep quorum.
    let killed = runtimes.pop().expect("third runtime exists");
    drop(killed);

    let second_batch: u64 = 3;
    for value in 1..=second_batch {
        runtimes[0]
            .propose((10 + value).to_le_bytes().to_vec())
            .await
            .expect("proposal must succeed with two of three nodes");
    }
    let expected_after: u64 = expected + (11..=10 + second_batch).sum::<u64>();
    for (i, probe) in probes.iter().take(2).enumerate() {
        wait_until(
            &format!("node {} to apply the second batch after the kill", i + 1),
            || probe.sum.load(Ordering::Acquire) == expected_after,
        )
        .await;
    }

    // 5. Graceful stop of the survivors.
    for runtime in runtimes {
        assert!(runtime.is_alive());
        runtime.shutdown();
        runtime
            .join()
            .await
            .expect("graceful shutdown must succeed");
    }
}
