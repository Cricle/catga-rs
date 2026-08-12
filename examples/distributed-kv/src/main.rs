//! distributed-kv: a three-node Raft-replicated key-value store built on Catga.
//!
//! ```bash
//! distributed-kv --node 0 --nodes 3 --base-port 9100 [--backend raft|sorock]
//! ```
//!
//! `--backend` (env fallback `KV_BACKEND`, default `raft`) selects the
//! consensus backend: `raft` is raft-rs over HTTP (catga-cluster +
//! catga-axum); `sorock` is the sorock multi-Raft backend over gRPC
//! (catga-sorock), with each node's gRPC port derived as its API port + 1000.
//! The KV application itself (state machine, HTTP routes, bench) is identical
//! on both and depends only on the `catga-core` consensus traits.
//!
//! Kubernetes mode needs no CLI flags: with `POD_NAME` (downward API),
//! `KV_CLUSTER_NAME`, and `KV_REPLICAS` set, member endpoints derive from
//! headless-service DNS (`http://<name>-<i>.<name>-headless:9100`, and port
//! 10100 for the sorock gRPC transport) and state persists on the pod PVC at
//! `/data`.
//!
//! Backend notes: on sorock, `/status` reports `"leader_endpoint": null` and
//! `"is_leader": false` because sorock 0.12 exposes no leadership query (see
//! the catga-sorock crate docs); `/healthz` reflects runtime liveness on both
//! backends.

mod messages;
mod node;
mod state;

use catga_cluster::RaftClusterConfig;
use catga_core::{CatgaError, CatgaResult, ErrorCode};

pub(crate) struct Args {
    pub node: u64,
    pub nodes: u64,
    pub base_port: u16,
    pub backend: Backend,
    pub bench_writes: Option<u32>,
    pub bench_addr: Option<String>,
}

/// Consensus backend selected by `--backend` / `KV_BACKEND` (default: raft).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Backend {
    /// raft-rs over HTTP (catga-cluster + catga-axum).
    Raft,
    /// sorock multi-Raft over gRPC (catga-sorock).
    Sorock,
}

impl Backend {
    fn parse(raw: &str) -> CatgaResult<Self> {
        match raw {
            "raft" => Ok(Self::Raft),
            "sorock" => Ok(Self::Sorock),
            other => Err(invalid(format!(
                "unknown backend: {other} (expected raft|sorock)"
            ))),
        }
    }
}

/// Offset from a node's API port to its sorock gRPC port.
pub(crate) const GRPC_PORT_OFFSET: u16 = 1000;

/// Kubernetes deployment topology derived from the pod environment.
pub(crate) struct K8sTopology {
    pub raft_node_id: u64,
    pub api_port: u16,
    pub grpc_port: u16,
    pub cluster_name: String,
    pub replicas: u64,
    pub config: RaftClusterConfig,
}

impl K8sTopology {
    /// Zero-based StatefulSet ordinal of this pod.
    pub(crate) fn ordinal(&self) -> u64 {
        self.raft_node_id - 1
    }

    /// gRPC URI one pod advertises to sorock peers: same headless-service
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
    let endpoint_of =
        |i: u64| format!("http://{cluster_name}-{i}.{cluster_name}-headless:{K8S_API_PORT}");
    let members: Vec<serde_json::Value> = (0..replicas)
        .filter(|i| *i != ordinal)
        .map(|i| serde_json::json!({ "id": i + 1, "endpoint": endpoint_of(i) }))
        .collect();
    let config: RaftClusterConfig = serde_json::from_value(serde_json::json!({
        "nodeId": ordinal + 1,
        "localNodeEndpoint": endpoint_of(ordinal),
        "members": members,
        "persistentStatePath": "/data"
    }))
    .map_err(|error| invalid(format!("invalid derived cluster config: {error}")))?;
    Ok(Some(K8sTopology {
        raft_node_id: ordinal + 1,
        api_port: K8S_API_PORT,
        grpc_port: K8S_GRPC_PORT,
        cluster_name,
        replicas,
        config,
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
    let mut backend = match std::env::var("KV_BACKEND") {
        Ok(raw) => Backend::parse(&raw)?,
        Err(_) => Backend::Raft,
    };
    let mut bench_writes = None;
    let mut bench_addr = None;
    let mut iter = std::env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--node" => node = Some(parse_value("node", iter.next())?),
            "--nodes" => nodes = Some(parse_value("nodes", iter.next())?),
            "--base-port" => base_port = parse_value("base-port", iter.next())?,
            "--backend" => {
                backend = Backend::parse(&parse_value::<String>("backend", iter.next())?)?
            }
            "--bench-writes" => bench_writes = Some(parse_value("bench-writes", iter.next())?),
            "--bench-addr" => bench_addr = Some(parse_value("bench-addr", iter.next())?),
            other => return Err(invalid(format!("unknown argument: {other}"))),
        }
    }
    if bench_writes.is_some() || k8s_mode {
        return Ok(Args {
            node: 0,
            nodes: 0,
            base_port,
            backend,
            bench_writes,
            bench_addr,
        });
    }
    let node = node.ok_or_else(|| invalid("--node is required"))?;
    let nodes = nodes.ok_or_else(|| invalid("--nodes is required"))?;
    if nodes == 0 || node >= nodes {
        return Err(invalid("--node must be in [0, --nodes)"));
    }
    if u64::from(base_port) + nodes > u64::from(u16::MAX) + 1 {
        return Err(invalid("--base-port leaves no room for all nodes"));
    }
    if backend == Backend::Sorock
        && u64::from(base_port) + u64::from(GRPC_PORT_OFFSET) + nodes > u64::from(u16::MAX) + 1
    {
        return Err(invalid(
            "--base-port leaves no room for the sorock gRPC ports (api port + 1000)",
        ));
    }
    Ok(Args {
        node,
        nodes,
        base_port,
        backend,
        bench_writes,
        bench_addr,
    })
}

#[tokio::main]
async fn main() -> CatgaResult<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    let k8s = k8s_topology_from_env()?;
    let args = parse_args(k8s.is_some())?;
    if let Some(writes) = args.bench_writes {
        let addr = args
            .bench_addr
            .clone()
            .ok_or_else(|| invalid("--bench-writes requires --bench-addr"))?;
        return node::bench(&addr, writes).await;
    }
    node::run(args, k8s).await
}
