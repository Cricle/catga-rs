# Benchmark: distributed-kv vs TiKV (same host)

Date: 2026-08-16. Host: one WSL2 VM (16 cores, 15GB RAM), identical disk for
both stacks. Workload: small key-value puts (`bench-*` keys, ~20-byte values),
sequential and concurrent clients, per-write latency measured client-side.

## Setup

| | TiKV | distributed-kv (catga-raft) |
|---|---|---|
| Version | v8.5.7 (tiup playground) | workspace HEAD |
| Topology | 1 PD + 3 TiKV | 3 raft voters |
| Client API | raw KV gRPC (python `tikv-client`, no batching) | HTTP + JSON (`--bench-writes/--bench-concurrency`) |
| Consistency | raw API (no txn layer) | linearizable writes (raft commit + apply wait) + ReadIndex reads |
| Persistence | raft-engine (RocksDB-backed state) | raft-engine WAL + redb group commit |
| Footprint | ~GB-scale RAM across PD/TiKV | 14MB image, ~50MB RAM per node |

## Results

| Workload | TiKV throughput | TiKV p50 | Catga throughput | Catga p50 |
|---|---:|---:|---:|---:|
| 1 client, 100 writes | 268/s | 2.57ms | 91/s | 10.2ms |
| 16 clients, 600 writes | 1103/s | 9.79ms | **1292/s** | 11.2ms |
| 64 clients, 600 writes | 1249/s | 36.2ms | **2329/s** | 22.6ms |

## Reading the numbers

- Under concurrency, catga matches or beats TiKV on this host even though its
  path carries two extra layers (HTTP + JSON) plus a strict apply-wait before
  acknowledging writes. TiKV's python client does no batching and one
  connection per worker; TiKV's published numbers use go-ycsb with batch APIs,
  so this comparison understates TiKV.
- Sequential latency favors TiKV (2.6ms vs 10.2ms): our path pays HTTP + JSON,
  the raft replication round-trip, the redb group-commit window, and the
  apply-wait; TiKV's raw put skips all of those.
- Scaling shape differs: TiKV's throughput flattened by c=64 in this setup
  (client-bound python loop), while catga kept scaling (group commit amortizes
  fsync as concurrency grows).

## Reproduce

TiKV: `tiup playground --pd 1 --kv 3 --db 0 --tiflash 0 --without-monitor`,
then `python tikv_bench.py` (raw `put` loop, threads with per-thread clients).

Catga: `scripts/local-cluster.sh 3 9100` for correctness, then
`distributed-kv --bench-writes 600 --bench-addr 127.0.0.1:<leader> --bench-concurrency {16,64}`.

## Next levers for catga

1. Batch write API (one HTTP request carrying N puts, amortizing HTTP + raft propose).
2. Faster apply notification (direct notify instead of the group-commit poll window).
3. go/ycsb-grade client for an apples-to-apples TiKV comparison.


---

## Update 2 (after the full optimization plan)

All audit-driven optimizations landed: pipeline backpressure accounting
fix (no more halt), attributed propose_and_wait, pending-read bounds,
8MB wire alignment, zero-copy payloads, breaker recovery fixes,
empty-ready persist skip, leadership de-churn, single-task send
fan-out with read-lock pools, pre-vote + check_quorum, real snapshots,
membership change, log compaction.

Final numbers on the same box (3 nodes, persistent raft-engine + redb
group commit, HTTP+JSON stack):

| Workload | Session start | Final | Gain |
|---|---:|---:|---:|
| sequential, 200 writes | 45/s, p50 10.2ms | **157/s, p50 5.7ms** | 3.5x |
| 16 clients, 600 writes | 1292/s | 1347/s | ~1.0x |
| 64 clients, 600 writes | 2329/s | 1992/s | 0.86x (more machinery per write) |
| 16 clients, batch=8 | 8785 keys/s | **8266 keys/s** | ~0.94x |

Reading the delta: sequential latency nearly halved (persist path
off the critical loop, immediate flush wakeups). Peak concurrency
throughput is flat-to-slightly-lower than the leaner mid-session
build because every write now carries real durability (raft-engine
fsync pipeline), linearizable-read machinery, and attribution
bookkeeping — the earlier headline number predated some of that. The
batch API remains the dominant mode: ~8.3k keys/s with 16 HTTP
clients on one 3-node cluster.

---

## TiKV proper-client benchmark (go-ycsb)

Date: 2026-08-16, same host as every number in this file. The python
`tikv-client` results above understated TiKV: that driver issues one
synchronous unbatched put per call. This section re-measures TiKV with
go-ycsb, the standard batch-capable client (lever #3 from "Next levers").

### Environment

- Same WSL2 VM: 16 cores, 15 GiB RAM, Debian 13 (trixie), kernel
  6.18.33.2-microsoft-standard-WSL2, same ext4 volume.
- TiKV v8.5.7, topology 1 PD + 3 TiKV, raw-KV API:
  `tiup playground v8.5.7 --pd 1 --kv 3 --db 0 --tiflash 0 --without-monitor`
  (readiness: polled `/pd/api/v1/stores` until count=3 and all stores `Up`, ~18 s).
- go-ycsb master (commit f030f99) built with Go 1.24.4; tikv driver uses the
  raw API with client-go transport batching (128 gRPC connections,
  MaxBatchSize=128).
- YCSB defaults: ~1 KB records (10 fields x 100 bytes) — note catga's numbers
  above use ~20-byte values.

### Exact commands

```
go-ycsb load tikv -p tikv.pd="127.0.0.1:2379" -p recordcount=100000 -p threadcount=32
go-ycsb run  tikv -p tikv.pd="127.0.0.1:2379" -p operationcount=100000 -p threadcount=32 \
    -p readproportion=0   -p updateproportion=1   -p insertproportion=0    # write-only
go-ycsb run  tikv -p tikv.pd="127.0.0.1:2379" -p operationcount=100000 -p threadcount=32 \
    -p readproportion=0.5 -p updateproportion=0.5 -p insertproportion=0    # mixed 50/50
go-ycsb run  tikv -p tikv.pd="127.0.0.1:2379" -p operationcount=1024000 -p threadcount=32 \
    -p readproportion=0 -p updateproportion=1 -p insertproportion=0 -p batch.size=128
```

### Results (32 threads)

| Workload | Ops | Throughput | mean | p50 | p99 |
|---|---:|---:|---:|---:|---:|
| load, single-op Insert | 100,000 | **4,389/s** | 7.26ms | 6.95ms | 13.5ms |
| run, write-only (single-op Update = Get+Put) | 100,000 | **4,361/s** | 7.30ms | 7.23ms | 12.6ms |
| run, mixed 50/50 (total) | 100,000 | **7,130/s** | 4.42ms | 3.82ms | 13.1ms |
| · reads | 49,914 | 3,555/s | 0.90ms | 0.78ms | 2.5ms |
| · updates | 50,086 | 3,572/s | 7.93ms | 7.56ms | 17.1ms |
| run, write-only, `batch.size=128` (128-key BatchPut) | 1,024,000 keys = 8,000 batches | **~99,800 keys/s** (780 batches/s) | 40.7ms/batch | 40.7ms | 61.1ms |

The batched figure is from the longest run (8,000 batches in 10.3 s); shorter
runs of the same shape landed between ~54k and ~180k keys/s depending on
cluster warmup and region-split state, so treat ~10^5 keys/s as the order of
magnitude.

### Python cross-check (same playground session)

`/opt/tikvbench/bin/python /mnt/c/tmp/tikv_bench.py` (unbatched raw puts,
~20-byte values), to confirm the playground behaved like earlier sessions:

| Concurrency | This run | Published earlier |
|---:|---:|---:|
| 16 threads, 592 puts | 1,346/s (p50 9.7ms, p99 20.4ms) | 1,103/s |
| 64 threads, 576 puts | 1,090/s (p50 22.8ms, p99 54.2ms) | 1,249/s |

Same order of magnitude — the playground is consistent with the earlier runs.

### Comparison vs catga

catga reference on this box: **8,266 keys/s** with 16 HTTP clients x batch=8,
linearizable writes (raft commit + apply wait), HTTP+JSON, ~20-byte values.

| Stack | Throughput |
|---|---:|
| TiKV, python client (unbatched) | ~1.1–1.3k/s |
| catga, batch=8 x 16 HTTP clients | 8,266 keys/s |
| TiKV, go-ycsb single-op (gRPC + transport batching) | ~4.4k ops/s |
| TiKV, go-ycsb 128-key BatchPut | ~100k keys/s |

Reading the numbers:

- The original table in this file was unfair to TiKV. With a proper client,
  TiKV does ~4.4k single-op puts/s (3.5x the python figure) and ~100k keys/s
  once the client batches 128 keys per RPC.
- Against catga's batch mode, TiKV's single-op path (~4.4k/s) is still ~0.5x,
  but TiKV's explicit 128-key batch path is ~12x catga's 8,266 keys/s. Raw
  write throughput is not where catga wins today.
- The stacks are not directly comparable: both ack writes only after 3-replica
  raft commit, but catga's number additionally pays HTTP + JSON, an explicit
  apply wait for linearizable semantics, and uses ~20-byte values, while the
  YCSB workload uses ~1 KB records (which favor byte throughput) and client-go
  pools 128 gRPC connections with transport-layer batching. TiKV's raw API has
  no txn layer.
- Levers for catga, in priority order: bigger request batches (batch=8 -> 32+),
  deeper group-commit amortization, and trimming the HTTP/JSON path (binary
  encoding). Closing the ~12x gap to TiKV's batched path is a batching and
  serialization problem, not a raft problem.


---

## Update 3: batch-depth scaling (HTTP stack, all optimizations incl. apply worker)

Same box, 3 nodes, 2048 keys per run, persistent raft-engine + redb group
commit + apply worker, HTTP/JSON stack:

| batch/req | 16 clients | 32 clients |
|---:|---:|---:|
| 8 | 5,798 keys/s | 7,492 keys/s |
| 32 | 15,072 keys/s | 16,905 keys/s |
| 128 | **17,620 keys/s** | 16,959 keys/s |

Throughput saturates around 17k keys/s: the bottleneck has moved out of
consensus/storage into the HTTP+JSON layer, which motivates the gRPC
client path (task: KV gRPC service, proto prepared at
examples/distributed-kv/proto/kv.proto).

---

## gRPC fast path

Date: 2026-08-16, same host as every number in this file. The KV node now
serves a binary gRPC API next to the HTTP/JSON one (same linearizable
semantics: writes ack after raft commit + local apply, `Get` is a
ReadIndex-barriered read): `Kv.Put`, `Kv.PutBatch`, `Kv.Get` from
`proto/kv.proto`. Locally the gRPC port is the third `nodes*100` band
(`raft + 2*nodes*100`, i.e. 9700/9800/9900 for a 3-node cluster on base
9100); in Kubernetes it is the raft port + 400 (10500).

The bench tool grew `--bench-mode grpc`: one tonic channel per worker task,
`PutBatch` when `--bench-batch > 1` else `Put`, identical key scheme and
summary format to the HTTP mode.

### Commands

```
scripts/local-cluster.sh 3 9100          # or start 3 nodes by hand; leader found via /status
distributed-kv --bench-writes 4096 --bench-addr 127.0.0.1:<leader-grpc> \
    --bench-concurrency 16 --bench-batch 128 --bench-mode grpc
distributed-kv --bench-writes 4096 --bench-addr 127.0.0.1:<leader-http> \
    --bench-concurrency 16 --bench-batch 128 --bench-mode http
```

### Results (grpc vs http, same day, same 3-node cluster, leader node)

2048 keys per run for batch 1/8, 4096 for batch 32/128; two passes back to
back, run-to-run spread on this box is ~±10%:

| batch x clients | gRPC keys/s | HTTP keys/s | GRPC mean / p50 / p99 |
|---|---:|---:|---|
| 1 x 16 | **2,154** (2nd pass 1,900) | 1,502 (1,479) | 464µs / 6.98ms / 16.5ms |
| 8 x 16 | **12,944** (11,911) | 10,988 (9,760) | 618µs / 9.25ms / 12.0ms |
| 32 x 32 | 17,553 (16,186) | 17,939 (15,357) | 1.82ms / 53.0ms / 66.0ms |
| 128 x 16 | **21,655** (20,639) | 19,888 (22,342) | 5.91ms / 72.4ms / 112.5ms |
| 128 x 32 | 18,767 (18,813) | — | 6.82ms / 104.7ms / 209.3ms |

Reading the numbers:

- Small batches are where the fast path pays: single-op puts run ~1.3-1.4x
  faster over gRPC (2.1k vs 1.5k/s) and batch=8 ~1.2x, because the
  HTTP+JSON request overhead is gone from every request.
- At batch=128 the two transports are within noise of each other
  (~20-22k keys/s): the bottleneck is no longer the client stack but the
  durable-consensus path (raft append/replication, group commit, apply
  wait). Going from 16 to 32 clients at batch=128 adds nothing — the
  pipeline is saturated.
- New failure mode surfaced by the faster client: gRPC batch=128 initially
  outran the raft pipeline's inflight window and proposals were rejected
  with a retryable `backpressure: peer overloaded` Unavailable. The gRPC
  bench backs off 5ms and retries those (HTTP mode unchanged); the
  rejections are the flow control working as designed, and they are why
  the batch=128 latency tails are wide.

### Comparison against TiKV (go-ycsb numbers from the section above)

| Stack | Throughput |
|---|---:|
| TiKV, go-ycsb single-op (gRPC, transport batching) | ~4,389 ops/s |
| catga gRPC, single-op x 16 clients | 2,154/s |
| catga HTTP, batch=8 x 16 clients (previous best in this file) | 8,266 keys/s |
| catga gRPC, batch=8 x 16 clients | 12,944 keys/s |
| catga gRPC/HTTP, batch=128 x 16 clients | ~21-22k keys/s |
| TiKV, go-ycsb 128-key BatchPut | ~100k keys/s |

Honest deltas:

- Single-op: TiKV's batched-transport gRPC client still does ~2x catga's
  single-op gRPC; catga's path carries the strict apply-wait before ack and
  the per-item propose round through the pipeline, while go-ycsb amortizes
  inside client-go's transport batcher.
- Batched: catga's gRPC batch path reaches ~21-22k keys/s, ~4.6x below
  TiKV's ~100k keys/s. With HTTP+JSON removed from the equation, the
  remaining gap is the consensus/durability machinery (proposal batching
  depth, replication pipeline, group commit), not the client transport —
  the lever list from "Update 3" now points entirely at the raft/apply
  side. Record sizes still differ (~20-byte values here vs ~1 KB YCSB
  records), which favors TiKV's byte throughput.
- The gRPC service also unblocks fairer future comparisons: a go-ycsb
  style driver could talk to this service directly.


---

## Update 4: pipeline tuning and the local ceiling

Tuned proposal pipeline (batch_size 256, max_inflight 8192) in the KV
example:

| config | gRPC batch=128 c=16 |
|---|---:|
| before (64/1024) | 21,655 keys/s |
| tuned (256/8192) | 23,176 keys/s |

Only +7%: the ceiling is NOT the admission window. Isolation bench on
a SINGLE node (no replication): 28,830 keys/s — replication costs
only ~20%. The persist worker ALREADY coalesces queued tasks into one
fsync batch (group commit), so the ~4.4ms-per-ready-cycle budget is
the async hop chain itself: client -> pipeline flush -> owner drain
-> raft propose -> persist worker -> fsync -> ack -> commit -> apply
worker -> waiter resolution (~6-8 task boundaries). Cutting further
requires structural change (a TiKV-raftstore-style tight loop fusing
propose/persist/apply), not tuning. Current plateau: ~23k keys/s
3-node, ~29k single-node — 3x the session start, ~4.3x behind TiKV's
batch path (was 12x at session start).
