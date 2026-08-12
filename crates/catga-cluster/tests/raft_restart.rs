//! Regression tests for persistent Raft node restart after a tip checkpoint.
//!
//! A node that checkpoints at its committed log tip and then stops must reopen,
//! campaign, and keep applying new commands. This covers the rolling-restart
//! path where the durable snapshot index equals the durable commit index.

#[path = "common/members.rs"]
mod members;
#[path = "common/recording_machine.rs"]
mod recording_machine;
#[path = "common/sink_transport.rs"]
mod sink_transport;

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use catga_cluster::{
    ClusterCoordinator, RaftNode, RaftStateMachineDriver, RaftStateMachineRuntime,
};

use members::single_member;
use recording_machine::RecordingMachine;
use sink_transport::SinkTransport;

#[test]
fn persistent_node_recovers_after_tip_checkpoint_and_keeps_applying() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test Tokio runtime must build");

    runtime.block_on(async {
        let directory = tempfile::tempdir().expect("temporary raft directory");
        let applied = Arc::new(AtomicU64::new(0));
        let snapshot_calls = Arc::new(AtomicUsize::new(0));

        {
            let node =
                RaftNode::open_persistent(1, "http://node-1", single_member(), directory.path())
                    .expect("persistent node must open");
            let driver = RaftStateMachineDriver::new(
                node,
                RecordingMachine::new(Arc::clone(&applied), Arc::clone(&snapshot_calls)),
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
            runtime
                .propose(4_u64.to_le_bytes())
                .await
                .expect("first proposal must succeed");
            assert_eq!(applied.load(Ordering::Acquire), 4);
            runtime
                .checkpoint()
                .await
                .expect("applied tip must checkpoint");
            assert_eq!(snapshot_calls.load(Ordering::Acquire), 1);

            runtime.shutdown();
            runtime.join().await.expect("owner must stop cleanly");
        }

        {
            let node =
                RaftNode::open_persistent(1, "http://node-1", single_member(), directory.path())
                    .expect("persistent node must reopen");
            let driver = RaftStateMachineDriver::new(
                node,
                RecordingMachine::new(Arc::clone(&applied), Arc::clone(&snapshot_calls)),
            )
            .expect("driver must recover from the durable snapshot");
            assert_eq!(
                applied.load(Ordering::Acquire),
                4,
                "snapshot restore must materialize the checkpointed state"
            );
            let runtime = RaftStateMachineRuntime::spawn(
                driver,
                Arc::new(SinkTransport),
                Duration::from_millis(1),
            )
            .expect("runtime must restart");

            // No explicit campaign: a campaign commits an election no-op entry before the
            // commit-queue refill runs, which masks the empty-page recovery path. Letting
            // the election fire through ticks exercises the refill exactly as a restarted
            // follower (or a pre-vote-losing node) hits it.
            let coordinator = runtime.coordinator();
            assert!(
                coordinator
                    .wait_for_leadership(Duration::from_secs(5))
                    .await,
                "restarted node must elect itself through ticks"
            );
            runtime
                .propose(5_u64.to_le_bytes())
                .await
                .expect("second proposal must succeed after restart");
            assert_eq!(
                applied.load(Ordering::Acquire),
                9,
                "each command must be applied exactly once across the restart"
            );

            runtime.shutdown();
            runtime.join().await.expect("owner must stop cleanly");
        }
    });
}
