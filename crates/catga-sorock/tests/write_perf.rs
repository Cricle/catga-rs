//! Write-path latency for the sorock backend.
//!
//! `sequential_writes_stay_off_the_poll_floor` is a non-flaky regression
//! guard: sequential leader proposals must complete well under the latency
//! of sorock's internal poll/heartbeat cadence (100ms+ per stage).
//!
//! `write_latency_probe` is an ignored measurement harness (3-node
//! in-process cluster, p50/p99 report for both the leader propose path and
//! follower-apply visibility); run it explicitly with
//! `cargo test -p catga-sorock --test write_perf -- --ignored --nocapture`.

#[path = "common/harness.rs"]
mod harness;
#[path = "common/probe.rs"]
mod probe;
#[path = "common/recording_machine.rs"]
mod recording_machine;

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use catga_core::ConsensusRuntime;
use catga_sorock::SorockRuntime;
use harness::{loopback_config, wait_until};
use probe::MachineProbe;

/// Starts a three-node in-process cluster and bootstraps the group on the
/// first node. Returns the runtimes (node 0 is the leader) and their probes.
async fn three_node_cluster(node_id: &str) -> (Vec<SorockRuntime>, Vec<MachineProbe>) {
    let probes: Vec<MachineProbe> = (0..3).map(|_| MachineProbe::default()).collect();
    let mut runtimes = Vec::new();
    for probe in &probes {
        let config = loopback_config(node_id);
        let runtime = SorockRuntime::start(config, probe.machine())
            .await
            .expect("sorock runtime must start");
        runtimes.push(runtime);
    }
    let uris: Vec<String> = runtimes
        .iter()
        .map(|rt| rt.advertised_uri().to_owned())
        .collect();
    runtimes[0]
        .add_member(1, uris[0].clone())
        .await
        .expect("self add must bootstrap the group");
    runtimes[0]
        .add_member(2, uris[1].clone())
        .await
        .expect("second node must join");
    runtimes[0]
        .add_member(3, uris[2].clone())
        .await
        .expect("third node must join");
    (runtimes, probes)
}

async fn shutdown_all(runtimes: Vec<SorockRuntime>) {
    for runtime in runtimes {
        runtime.shutdown();
        runtime
            .join()
            .await
            .expect("graceful shutdown must succeed");
    }
}

/// Regression guard for the leader write path: 100 sequential proposals must
/// finish far below sorock's poll-floor latency (~0.3s per write if the
/// event-driven replication/commit/apply chain ever degenerated into its
/// 100ms poll fallbacks). Locally the batch completes in well under a second;
/// the bound leaves orders of magnitude of headroom for loaded CI hosts.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn sequential_writes_stay_off_the_poll_floor() {
    let (runtimes, _probes) = three_node_cluster("perf-guard").await;

    // Warm up elections, replication streams, and connection pools.
    for value in 0..3_u64 {
        runtimes[0]
            .propose(value.to_le_bytes().to_vec())
            .await
            .expect("warmup proposals must succeed");
    }

    let started = Instant::now();
    for value in 0..100_u64 {
        runtimes[0]
            .propose((1000 + value).to_le_bytes().to_vec())
            .await
            .expect("proposal must succeed on a healthy group");
    }
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(30),
        "100 sequential writes must stay far under the 100ms-per-stage poll floor: {elapsed:?}"
    );

    shutdown_all(runtimes).await;
}

fn percentile(sorted: &[u128], pct: usize) -> u128 {
    let idx = (sorted.len() * pct / 100).min(sorted.len() - 1);
    sorted[idx]
}

fn report(label: &str, mut micros: Vec<u128>) {
    micros.sort_unstable();
    eprintln!(
        "{label}: n={} p50={}us p99={}us max={}us",
        micros.len(),
        percentile(&micros, 50),
        percentile(&micros, 99),
        micros[micros.len() - 1],
    );
}

/// Number of sequential proposals timed by the leader-path probe.
const PROBE_WRITES: usize = 200;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "perf probe; run explicitly"]
async fn write_latency_probe() {
    let (runtimes, probes) = three_node_cluster("perf-probe").await;

    // Warm up: a few proposals so elections, streams, and connection pools
    // are all established before timing starts.
    for value in 0..5_u64 {
        runtimes[0]
            .propose(value.to_le_bytes().to_vec())
            .await
            .expect("warmup proposals must succeed");
    }
    let warm_sum: u64 = (0..5).sum();
    wait_until("warmup to replicate everywhere", || {
        probes
            .iter()
            .all(|p| p.sum.load(Ordering::Acquire) == warm_sum)
    })
    .await;

    // Probe 1: leader propose latency (the `ConsensusRuntime::propose`
    // contract — committed and applied on the leader).
    let mut micros = Vec::with_capacity(PROBE_WRITES);
    let start = Instant::now();
    for value in 0..PROBE_WRITES as u64 {
        let t0 = Instant::now();
        runtimes[0]
            .propose((1000 + value).to_le_bytes().to_vec())
            .await
            .expect("timed proposal must succeed");
        micros.push(t0.elapsed().as_micros());
    }
    let total = start.elapsed();
    eprintln!(
        "leader batch throughput: {:.1} writes/s",
        PROBE_WRITES as f64 / total.as_secs_f64(),
    );
    report("leader propose latency", micros);

    // Probe 2: submit on the leader, then wait until the entry is applied on
    // a follower's state machine — the latency a client colocated with a
    // follower observes. Let the follower fully drain first so its counter is
    // quiescent before the timed iterations start.
    let drained_sum: u64 = warm_sum + (0..PROBE_WRITES as u64).map(|v| 1000 + v).sum::<u64>();
    let follower_sum: Arc<AtomicU64> = Arc::clone(&probes[2].sum);
    wait_until("follower to drain the timed batch", || {
        follower_sum.load(Ordering::Acquire) == drained_sum
    })
    .await;
    let mut follower_micros = Vec::with_capacity(50);
    for value in 0..50_u64 {
        let want = drained_sum + (0..=value).map(|v| 1_000_000 + v).sum::<u64>();
        let t0 = Instant::now();
        runtimes[0]
            .propose((1_000_000 + value).to_le_bytes().to_vec())
            .await
            .expect("proposal must succeed");
        while follower_sum.load(Ordering::Acquire) < want {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        follower_micros.push(t0.elapsed().as_micros());
    }
    report(
        "follower-apply latency (propose + follower apply)",
        follower_micros,
    );

    shutdown_all(runtimes).await;
}
