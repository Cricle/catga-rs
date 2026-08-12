# sorock Multi-Raft Backend

## Overview

`catga-sorock` is a multi-Raft consensus backend built on [sorock](https://crates.io/crates/sorock) 0.12: it adapts one shard of a sorock Raft process to the backend-agnostic consensus contracts in `catga-core` (`ConsensusStateMachine` / `ConsensusRuntime` / `ConsensusCoordinator`, see [Cluster Mode](./cluster.md#backend-agnostic-consensus-abstraction-catga-core)). Unlike `catga-cluster` (raft-rs over HTTP, where the application brings its own transport and storage), sorock ships with a gRPC transport (tonic) and redb storage — you write no transport code at all.

```toml
[dependencies]
catga-sorock = "0.2"
```

> Building requires `protoc` (sorock's build script compiles the raft service protobuf definitions). Runnable reference: the `--backend sorock` branch of [`examples/distributed-kv`](../../examples/distributed-kv).

## Type map

- `SorockNodeConfig` — per-node configuration; start from `SorockNodeConfig::new(node_id, bind_addr)` and adjust the public fields.
- `SorockNode` — one running node: gRPC server + sorock `RaftNode` + redb storage.
- `SorockRuntime` — the `ConsensusRuntime` implementation: proposals, membership, progress, lifecycle, issued through a gRPC client connected to the **local** node; sorock forwards them to the current leader internally, so the runtime behaves identically on leaders and followers.
- `SorockCoordinator` — the `ConsensusCoordinator` implementation: the local member view and the (restricted) leadership view, see "Leadership is not observable" below.
- `SorockApp` — adapts an application `ConsensusStateMachine` to sorock's `RaftApp` trait (including its byte-stream snapshot protocol); normally constructed indirectly via `SorockRuntime::start`.

## Configuration

Defaults of `SorockNodeConfig::new(node_id, bind_addr)`: shard 0, in-memory storage, snapshots disabled, `request_timeout = 2s` (`DEFAULT_REQUEST_TIMEOUT`, tuned down from 10s in 0.2 — see "Failover behavior and tuning"), `propose_retry = { max_attempts: 10, backoff: 200ms }`, `failover_watchdog = false`. Fields:

- `node_id` — stable node identifier, also used as the prefix of proposal request ids (which sorock deduplicates on).
- `bind_addr` — address the gRPC server binds to; port `0` asks the OS for a free port, and the effective address is available through `SorockNode::local_addr()`.
- `public_uri` — URI advertised to peers (scheme included, e.g. `http://raft-1.internal:7000`); defaults to `http://{bound address}` with an unspecified bind IP replaced by `127.0.0.1`. **This is the server id used in `add_member` calls.**
- `members` — member endpoints known at startup; the list only seeds the coordinator's member view, actual membership is established through `add_member` calls.
- `shard` — the primary shard: the shard this node's first Raft process attaches to; every node of a group must use the same shard index (default `DEFAULT_SHARD_INDEX = 0`); additional shards are attached later through `SorockRuntime::attach_shard` — see "Multi-shard model".
- `storage` — `SorockStorage::InMemory` (default, volatile) or `SorockStorage::RedbFile(path)` (a redb database file, created when missing).
- `snapshot_interval` — applied user entries between application-driven snapshots; `0` (the default) disables snapshotting entirely. **Keep it at `0` for file-backed nodes** — see "Snapshot model".
- `request_timeout` — deadline applied to each client-side gRPC call, **and the total budget of one `propose` call including its internal retries**; sorock's client API has no built-in timeout and a write on a leaderless group would otherwise wait indefinitely, so a budget is mandatory.
- `propose_retry` (`SorockProposeRetry`) — the retry policy of `propose` for failures that typically resolve once the group re-elects: exactly the leader-transient classifications `Transient` / `Unavailable` / `Cancelled` are retried; every other classification stays single-shot. A retried proposal carries the same request id, which sorock deduplicates on, so retries cannot apply the entry twice. `max_attempts = 1` disables retries. The backoff is clamped to the remaining `request_timeout` budget, so a `propose` call never outlives its deadline by more than one in-flight attempt.
- `failover_watchdog` — the failover watchdog (off by default): when a `propose` attempt fails with a dead-leader pattern, the runtime sends sorock's `TimeoutNow` RPC to a live member to force-promote a survivor, cutting the election from the phi-detector window to roughly two round trips. **Caveat:** the trigger cannot distinguish a dead leader from a slow or partitioned one — firing on a partitioned-but-alive leader costs an extra term (sorock's pre-vote bounds the disruption to a term bump); clients that cannot tolerate that churn should keep it disabled.

## Bootstrap order (important)

sorock forms groups imperatively; there is no static membership list:

1. Start every node with `SorockRuntime::start(config, machine)` (must be called from within a Tokio runtime — sorock spawns its Raft threads with `tokio::spawn`). At this point each node only serves its own shard and knows no peers.
2. On the **first node's own runtime**, call `add_member` with the **first node's own advertised URI**. A self-add on an empty membership is a single-node bootstrap: sorock immediately elects itself leader.
3. Add every remaining node with one `add_member(id, uri)` call per node, issued in sequence. Membership changes are one-at-a-time: wait until a change is observable (for example through the coordinator's member view) before issuing the next one.

After bootstrap, any node may issue membership changes; sorock forwards them to the current leader internally.

```rust
let runtime = SorockRuntime::start(config, machine).await?;
// On the bootstrap node (node 1):
runtime.add_member(1, runtime.advertised_uri().to_owned()).await?; // self-add = bootstrap
runtime.add_member(2, "http://node-2:7000".into()).await?;
runtime.add_member(3, "http://node-3:7000".into()).await?;
```

## Membership retry semantics

sorock accepts only one membership change at a time: while a previous change (including the bootstrap configuration) is still committing, a new one is rejected and the client observes a dropped stream (`Cancelled`/`Unknown`, or `Unavailable` while the group has no leader). `SorockRuntime` retries these three rejections transparently every 100ms until the `request_timeout` budget expires — membership changes are idempotent (adding an existing server or removing an absent one converges to the same voter set), so retrying is safe and callers may issue changes back to back.

`remove_member(id)` needs the member's endpoint, but the trait only passes the numeric id: the runtime resolves ids through a local registry of previous `add_member` calls; removing an id that was never added through this runtime fails with `ErrorCode::NotFound`.

## Multi-shard model

sorock 0.12 is a true multi-Raft engine: one node process can host many independent Raft groups, called *shards*. This crate exposes that through `SorockRuntime::attach_shard(shard, machine)` (and the lower-level `SorockNode::attach_shard(shard, SorockApp)`); using `SorockRuntime::start` alone keeps the node single-shard, exactly as before.

- **Every shard is an independent Raft group** with its own log, ballot, membership, leader election, and state machine. A write proposed on one shard is never visible on another, and every shard elects and fails over on its own.
- **Membership is per shard**: `add_member` / `remove_member` carry the runtime's shard index (sorock's `AddServer`/`RemoveServer` RPCs take a `shard_index`), so different shards of the same process may have different voter sets. The bootstrap order above applies to each shard independently: bootstrap every shard with a self-add on that shard's runtime.
- **Storage is per node, namespaced per shard**: each shard's log and ballot live in dedicated redb tables (`log-{shard}` / `ballot-{shard}`) inside the node's single database, so a `RedbFile` node holds all its shards in one file.
- **The coordinator view is per runtime**, hence per shard: each shard runtime tracks only the members added through it.
- **Failover tuning is inherited per shard**: a runtime produced by `attach_shard` keeps the originating runtime's `request_timeout` / `propose_retry` / `failover_watchdog`; the watchdog's `TimeoutNow` RPC carries the shard index and only force-promotes within that shard.
- **Lifecycle is reference-counted over the shared node**: shutting down, joining, or dropping one shard runtime detaches only that shard's Raft process; the gRPC server keeps serving the remaining shards and stops when the last shard runtime shuts down. Attaching the same shard twice fails with `ErrorCode::Validation`; `SorockNode::detach_shard(shard)` detaches a shard (its log and ballot stay in storage, so a later re-attach of the same index resumes from the persisted state), and `attached_shards()` lists the current shards.

```rust
let runtime = SorockRuntime::start(config, machine_a).await?;      // primary shard (config.shard)
let shard1 = runtime.attach_shard(1, machine_b).await?;            // a second Raft group in the same process
// every shard bootstraps independently: self-add on its own runtime, then add the peers one by one
shard1.add_member(self_id, runtime.advertised_uri().to_owned()).await?;
```

## Snapshot model

sorock 0.12 never asks the application to *take* a snapshot; instead the application advertises one through `get_latest_snapshot` and sorock folds it into the log. `SorockApp` therefore snapshots the machine every `snapshot_interval` applied entries into an **in-memory** store, serves the bytes to followers as a 256KiB chunk stream, and restores the machine on `install_snapshot`. Log index 1 is the implicit genesis snapshot sorock seeds every fresh log with; it never carries application bytes.

> **Keep `snapshot_interval = 0` on file-backed nodes**: snapshots live in memory, so a restarted node whose log was compacted past its in-memory snapshots cannot restore them. distributed-kv's sorock branch keeps the default (disabled).

## Leadership is not observable (known limitation)

sorock 0.12 exposes **no public API to observe leadership**: the election state and ballot are internal to the Raft process, and the gRPC request type needed to poll node state is not exported from the crate. Therefore:

- `SorockCoordinator::is_leader()` **always returns `false`** and `leader_endpoint()` **always returns `None`** — both are conservative and never claim leadership that does not exist;
- the `/status` probe of distributed-kv's sorock branch accordingly reports `"is_leader": false` and `"leader_endpoint": null` (node id, member view, liveness, and `applied_index` work normally);
- applications that need leader-only semantics must **fence externally** (a distributed lock, a storage lease, or checking proposal success) — never rely on the coordinator's leadership view.

`member_endpoints()` returns the endpoints this node *locally knows*: the configured seed members plus every endpoint added or removed through the owning runtime. It is not a live query against the group and can lag behind membership changes issued on other nodes.

Other sorock 0.12 limitations: no learners and no joint consensus — membership changes replace the voter set in a single log entry, and a brand-new node must catch up through log replication (or a snapshot) before it can vote.

## Write semantics

- sorock's write RPC resolves once the entry is **committed and applied on the leader**, so `propose` here is stronger than the minimal fire-and-forget contract.
- Every write carries a monotonically increasing, node-unique request id (`{node_id}-{seq}`) which sorock deduplicates on: a retried proposal with the same id is applied at most once.
- `ConsensusStateMachine` has no read path: `process_read` is not supported — reads return an empty payload without touching the machine. Issue linearizable reads through your own application layer if you need them.

## Failover behavior and tuning

When the leader dies, sorock's own machinery is fast: the phi-accrual failure detector (threshold 12.0 against 300ms heartbeats) plus the election tick lands a new leader in roughly 1.5–3s — a **floor built into the dependency** (sorock 0.12's heartbeat cadence is hardcoded at 300ms and not configurable). The previously measured 10–18s was dominated by the **client side**: followers keep forwarding writes to the dead leader (`ballot.voted_for`), and a forwarded write only resolves once the transport gives up (sorock's internal h2 pong watchdog defaults to 20s) or the client deadline fires. The 0.2 tuning tightens the client side:

- `request_timeout` defaults **10s → 2s**, capping every client-visible attempt;
- `propose_retry` retries leader-transient failures inside that budget (default 10 attempts × 200ms backoff), so one `propose` call rides out the election instead of failing immediately;
- `failover_watchdog` (optional, off by default) force-promotes a survivor through `TimeoutNow` on a dead-leader pattern, compressing the election to roughly two round trips.

**Measured** (2026-08-12, Windows dev machine, loopback, three nodes, debug build; kill-leader through distributed-kv's HTTP write path, timed until a write succeeds again, median of 3 runs):

| Configuration | Failover (kill leader → write succeeds) | Notes |
| --- | --- | --- |
| sorock balanced defaults | **median ~3.0s** (runs: 2.9 / 3.0 / 9.2s) | the first post-kill attempt typically hangs until the 2s client deadline (the stream forwarded to the dead leader only resolves at the timeout), then the retry loop rides out the phi window |
| sorock + watchdog | ~2.1s (crate-level test) | about one request budget plus an election round trip; the distributed-kv example does not expose the flag, so this number comes from `catga-sorock/tests/failover_tuning.rs` (runtime-level loopback; same file measures ~4.2s balanced) |
| raft backend, for comparison | median ~3.5s (runs: 3.1 / 3.5 / 6.2s) | same example, raft branch, forwarded follower writes with HTTP retry |

> **Election livelock under sustained write pressure (observed)**: in 1 of the 4 sorock measurement runs above, the group took **over 4 minutes** to re-elect after the kill — two survivors started at the same instant run their election ticks in phase, each holding sorock 0.12's `vote_sequencer` (capacity 1) for the whole `try_promote` round, so the other's vote RPC fails at `try_acquire` (the service handler panics with "no permits available") and pre-votes starve each other; the randomized election sleep (0–900ms) eventually de-phases the cycles, but the window can be long. **Operational guidance**: clients should retry with backoff during the failover window rather than hammering; deployments sensitive to election latency should enable `failover_watchdog`.

> **Ghost heartbeats (operational red line)**: sorock 0.12's per-peer heartbeat threads (tasks `tokio::spawn`ed on the Tokio runtime the node started on) survive via an Arc cycle (Voter→Peers→peer_threads) — `detach_process`/dropping a Raft process does **not** stop them, and a "detached" node keeps heartbeating, which blocks elections (peers treat the dead leader as alive). **The only reliable node retirement is process exit** (or tearing down the whole Tokio runtime, as tests do with `shutdown_background`); `SorockRuntime::shutdown` + `join` stops the gRPC server and detaches the shard, but a detached shard's leaked heartbeat tasks can still outlive it inside the same Tokio runtime. Treat sorock node membership changes as process-level events operationally; merely dropping a runtime/node inside a live process always leaves ghost heartbeats behind.

## Performance characteristics

- **Write latency** (re-measured 2026-08-12, same loopback three-node debug-build setup, 300 sequential writes through distributed-kv's HTTP API): **median throughput ~106 writes/s, p50 ≈ 9.0ms, p99 ≈ 13.9ms** — on par with the raft backend (~114 writes/s, p50 ≈ 8.6ms). The crate-level probe (`catga-sorock/tests/write_perf.rs`, run explicitly with `--ignored`) measures p50 ≈ 8.8ms. The 456ms write latency seen before 0.2 traced to follower commit visibility gated by the heartbeat cadence and was eliminated in the write-path optimization.
- **Follower read-back lag**: after the leader acknowledges a write, a follower applies it only once the leader's commit index arrives with a heartbeat — measured read-back through a follower GET at a median of ~230ms (214–300ms). sorock 0.12 hardcodes the 300ms cadence (a per-follower heartbeat queue at 300ms, a multiplexer draining every 300ms, and commit/apply threads polling on a 100ms fallback) — a floor of the dependency, not of this adapter. Applications that need read-your-writes should pin reads to the proposing node.
- **Positioning**: a fit for correctness/simplicity-first workloads rather than minimal write latency.

## When to pick sorock vs the raft-rs backend

| Dimension | `catga-cluster` (raft-rs over HTTP) | `catga-sorock` (sorock 0.12 over gRPC) |
| --- | --- | --- |
| Transport/storage | application-provided (`RaftTransport` trait + storage of your choice) | built-in tonic gRPC + redb, zero transport code |
| Leadership observability | full (`is_leader`/`leader_endpoint`/subscription) | **not observable** (conservative false/None; fence externally) |
| Write routing | application-level forwarding pipeline (`ForwardToLeaderBehavior`) | sorock forwards internally, no pipeline needed |
| Failover (loopback, measured 2026-08-12) | median ~3.5s (write recovery) | balanced defaults median ~3.0s; watchdog ~2.1s (crate test); floor = phi detector window (hardcoded 300ms heartbeat cadence) |
| Membership | joint consensus, one member per step | voter set replaced in a single log entry; no learners/joint |
| Snapshots | raft-engine persistence + application checkpoint | in-memory snapshots; must stay disabled on file-backed nodes |
| Sharding | single Raft group | multi-Raft (shard model built in) |

Rule of thumb: pick the raft-rs backend when you need leadership routing/fencing, an observable election view, or an HTTP stack; pick sorock when you want minimal wiring with built-in storage and transport, multi-Raft shards, and can accept seconds-scale failover and external fencing. Both implement the same catga-core contracts, so application code (like distributed-kv) can switch at the `--backend` level.
