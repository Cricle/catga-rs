//! distributed-kv: a three-node Raft-replicated key-value store built on Catga.
//!
//! ```bash
//! distributed-kv --node 0 --nodes 3 --base-port 9100
//! ```
//!
//! This example replicates with catga-raft (TiKV-style multi-Raft over
//! gRPC). Port layout for node `i` of `N` (`base` = `--base-port`):
//!
//! | Plane | Local mode | Kubernetes (per pod) |
//! |---|---|---|
//! | raft gRPC transport | `base + i*100` | 10100 |
//! | HTTP API | `base + N*100 + i*100` | 9100 |
//! | KV gRPC (fast path) | `base + 2*N*100 + i*100` | 10500 |
//!
//! Locally the three planes occupy three contiguous `N*100`-wide bands
//! (raft, HTTP, KV gRPC), so they never collide regardless of cluster size;
//! Kubernetes pods each own their address space, so fixed per-pod ports
//! suffice (KV gRPC = raft port 10100 + 400).
//!
//! The KV application (state machine, routes, bench) depends only on the
//! `catga-core` consensus traits. Two client surfaces serve the same
//! linearizable semantics: HTTP/JSON and the gRPC fast path.
//!
//! Writes: `POST /kv` puts one key; `POST /kv/batch` puts up to 1024 keys in
//! one request (`{"items": [{"key", "value"}, ...]}`), amortizing the HTTP,
//! JSON, and propose overhead over the whole batch. The gRPC `Kv` service
//! (`Put`/`PutBatch`/`Get`) is the binary equivalent on the raft port band.
//!
//! Bench mode posts writes against one node, either sequentially or from `C`
//! concurrent tasks; `--bench-batch B` sends `B` keys per request and
//! reports throughput in keys/s. `--bench-mode http|grpc` (default `http`)
//! picks the client stack:
//!
//! ```bash
//! distributed-kv --bench-writes 500 --bench-addr 127.0.0.1:9400 --bench-concurrency 16 --bench-batch 8
//! distributed-kv --bench-writes 500 --bench-addr 127.0.0.1:9700 --bench-concurrency 16 --bench-batch 8 --bench-mode grpc
//! ```
//!
//! Kubernetes mode needs no CLI flags: with `POD_NAME` (downward API),
//! `KV_CLUSTER_NAME`, and `KV_REPLICAS` set, member endpoints derive from
//! headless-service DNS (`http://<name>-<i>.<name>-headless:9100` for the
//! HTTP API and port 10100 for the raft gRPC transport) and state persists
//! on the pod PVC at `/data`.

mod messages;
mod node;
mod state;

use catga_core::{CatgaError, CatgaResult, ErrorCode};

use crate::node::BenchMode;

pub(crate) struct Args {
    pub node: u64,
    pub nodes: u64,
    pub base_port: u16,
    pub bench_writes: Option<u32>,
    pub bench_addr: Option<String>,
    pub bench_concurrency: usize,
    pub bench_batch: usize,
    pub bench_mode: BenchMode,
    pub probe_grpc: Option<(String, String)>,
}

/// Offset from a pod's HTTP API port to its raft gRPC port.
pub(crate) const GRPC_PORT_OFFSET: u16 = 1000;

/// Kubernetes deployment topology derived from the pod environment.
pub(crate) struct K8sTopology {
    pub grpc_port: u16,
    pub cluster_name: String,
    pub replicas: u64,
    ordinal: u64,
}

impl K8sTopology {
    /// Zero-based StatefulSet ordinal of this pod.
    pub(crate) fn ordinal(&self) -> u64 {
        self.ordinal
    }

    /// gRPC URI one pod advertises to raft peers: same headless-service
    /// hostname as the API endpoint, on the derived gRPC port.
    pub(crate) fn grpc_uri(&self, ordinal: u64) -> String {
        format!(
            "http://{}-{}.{}-headless:{}",
            self.cluster_name, ordinal, self.cluster_name, self.grpc_port
        )
    }
}

const K8S_API_PORT: u16 = 9100;
const K8S_GRPC_PORT: u16 = K8S_API_PORT + GRPC_PORT_OFFSET;

pub(crate) fn k8s_topology_from_env() -> CatgaResult<Option<K8sTopology>> {
    let (Ok(pod_name), Ok(cluster_name)) =
        (std::env::var("POD_NAME"), std::env::var("KV_CLUSTER_NAME"))
    else {
        return Ok(None);
    };
    let replicas: u64 = std::env::var("KV_REPLICAS")
        .map_err(|_| invalid("KV_REPLICAS must be set in Kubernetes mode"))?
        .parse()
        .map_err(|_| invalid("KV_REPLICAS must be a positive integer"))?;
    let ordinal: u64 = pod_name
        .rsplit('-')
        .next()
        .and_then(|suffix| suffix.parse().ok())
        .ok_or_else(|| invalid(format!("POD_NAME has no ordinal suffix: {pod_name}")))?;
    if replicas == 0 || ordinal >= replicas {
        return Err(invalid("pod ordinal must be in [0, KV_REPLICAS)"));
    }
    Ok(Some(K8sTopology {
        grpc_port: K8S_GRPC_PORT,
        cluster_name,
        replicas,
        ordinal,
    }))
}

fn invalid(message: impl Into<String>) -> CatgaError {
    CatgaError::new(ErrorCode::Validation, message.into())
}

fn parse_value<T: std::str::FromStr>(name: &str, value: Option<String>) -> CatgaResult<T> {
    value
        .ok_or_else(|| invalid(format!("--{name} requires a value")))?
        .parse()
        .map_err(|_| invalid(format!("--{name} has an invalid value")))
}

fn parse_args(k8s_mode: bool) -> CatgaResult<Args> {
    let mut node = None;
    let mut nodes = None;
    let mut base_port = 9100u16;
    let mut bench_writes = None;
    let mut bench_addr = None;
    let mut bench_concurrency = 1usize;
    let mut bench_batch = 1usize;
    let mut bench_mode = BenchMode::Http;
    let mut probe_grpc = None;
    let mut iter = std::env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--node" => node = Some(parse_value("node", iter.next())?),
            "--nodes" => nodes = Some(parse_value("nodes", iter.next())?),
            "--base-port" => base_port = parse_value("base-port", iter.next())?,
            "--bench-writes" => bench_writes = Some(parse_value("bench-writes", iter.next())?),
            "--bench-addr" => bench_addr = Some(parse_value("bench-addr", iter.next())?),
            "--bench-concurrency" => {
                bench_concurrency = parse_value("bench-concurrency", iter.next())?
            }
            "--bench-batch" => bench_batch = parse_value("bench-batch", iter.next())?,
            "--bench-mode" => {
                let raw = parse_value::<String>("bench-mode", iter.next())?;
                bench_mode = match raw.as_str() {
                    "http" => BenchMode::Http,
                    "grpc" => BenchMode::Grpc,
                    other => {
                        return Err(invalid(format!(
                            "--bench-mode must be http or grpc, got {other}"
                        )));
                    }
                };
            }
            "--probe-grpc" => {
                let addr = parse_value::<String>("probe-grpc <addr>", iter.next())?;
                let key = parse_value::<String>("probe-grpc <key>", iter.next())?;
                probe_grpc = Some((addr, key));
            }
            other => return Err(invalid(format!("unknown argument: {other}"))),
        }
    }
    if probe_grpc.is_some() || bench_writes.is_some() || k8s_mode {
        if bench_concurrency == 0 {
            return Err(invalid("--bench-concurrency must be at least 1"));
        }
        if bench_batch == 0 || bench_batch > messages::MAX_BATCH_ITEMS {
            return Err(invalid(format!(
                "--bench-batch must be in [1, {}]",
                messages::MAX_BATCH_ITEMS
            )));
        }
        return Ok(Args {
            node: 0,
            nodes: 0,
            base_port,
            bench_writes,
            bench_addr,
            bench_concurrency,
            bench_batch,
            bench_mode,
            probe_grpc,
        });
    }
    let node = node.ok_or_else(|| invalid("--node is required"))?;
    let nodes = nodes.ok_or_else(|| invalid("--nodes is required"))?;
    if nodes == 0 || node >= nodes {
        return Err(invalid("--node must be in [0, --nodes)"));
    }
    // Raft, HTTP API, and KV gRPC each take a nodes*100 port band above base.
    if u64::from(base_port) + nodes * 300 > u64::from(u16::MAX) + 1 {
        return Err(invalid(
            "--base-port leaves no room for all raft + API + gRPC ports",
        ));
    }
    Ok(Args {
        node,
        nodes,
        base_port,
        bench_writes,
        bench_addr,
        bench_concurrency,
        bench_batch,
        bench_mode,
        probe_grpc,
    })
}

#[tokio::main]
async fn main() -> CatgaResult<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    let k8s = k8s_topology_from_env()?;
    let args = parse_args(k8s.is_some())?;
    if let Some((addr, key)) = args.probe_grpc {
        return node::kv_grpc::probe(&addr, &key).await;
    }
    if let Some(writes) = args.bench_writes {
        let addr = args
            .bench_addr
            .clone()
            .ok_or_else(|| invalid("--bench-writes requires --bench-addr"))?;
        return node::bench(
            &addr,
            writes,
            args.bench_concurrency,
            args.bench_batch,
            args.bench_mode,
        )
        .await;
    }
    node::run(args, k8s).await
}
