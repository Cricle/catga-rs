# Performance

Catga publishes reproducible release-mode measurements as workflow and release
artifacts. Values are observations from the recorded runner, not
hardware-independent guarantees.

The latest complete Docker run is available from the
[performance workflow](https://github.com/Cricle/catga-rs/actions/runs/30461404688).
It ran the functional E2E preflight and every manual benchmark on commit
[`25b6e01`](https://github.com/Cricle/catga-rs/commit/25b6e018d97ae1c9afd7d63e3acc516cf49e472d).
Every benchmark emits payload size, operation scope, nearest-rank p50/p95/p99,
Linux process RSS, and Docker container statistics. Storage rows measure the
same 256-byte FlowStore create, read, and optimistic-update lifecycle.

## Release Snapshot

| Source | Benchmark | Throughput (ops/s) | p50 | p95 | p99 |
| --- | --- | ---: | ---: | ---: | ---: |
| Memory | Tokio mpsc round-trip lower bound | 3,228,855 | 250ns | 280ns | 431ns |
| Memory | Catga publish / receive / ack | 979,889 | 952ns | 1.02us | 1.49us |
| Memory | Mediator request | 5,357,365 | 150ns | 151ns | 211ns |
| Memory | Three-step local Flow | 4,020,487 | 200ns | 201ns | 221ns |
| Memory | Retain 4,096 outbox records | 1,208,523 | 401ns | 3.48us | 3.81us |
| In-process | CQRS + Flow + transport workflow | 642,741 | 1.45us | 1.54us | 2.27us |
| In-process | Bounded mediator batch scheduler | 2,989 | 299.4us | 384.4us | 384.4us |
| In-process | Local Flow execution | 2,481,042 | 331ns | 341ns | 370ns |
| In-process | Local DSL Flow execution | 691,578 | 1.08us | 1.85us | 2.05us |
| NATS JetStream | Durable publish / receive / ack | 2,278 | 427.5us | 488.9us | 751.3us |
| SQLite | FlowStore lifecycle (c=1) | 2,835 | 332.5us | 369.8us | 451.2us |
| MySQL | FlowStore lifecycle (c=1) | 372 | 2.56ms | 3.76ms | 4.89ms |
| PostgreSQL | FlowStore lifecycle (c=1) | 759 | 1.26ms | 1.53ms | 3.05ms |
| SQL Server | FlowStore lifecycle (c=1) | 299 | 3.23ms | 4.16ms | 5.97ms |
| Redis | FlowStore lifecycle (c=1) | 2,108 | 456.0us | 542.7us | 704.4us |
| Docker E2E | Axum HTTP quote | 16,373 | 58.8us | 82.1us | 89.1us |
| Docker E2E | NATS JetStream round-trip | 2,301 | 432.2us | 469.4us | 492.5us |

The Tokio row is a lower bound: it excludes Catga acknowledgement,
lifecycle-drain tracking, bounded telemetry, and typed errors. The outbox row
retains 1MiB of payload plus record and index metadata.

## Mediator And Flow

Workstation micro-benchmarks measured pure in-process dispatch without a tracing
subscriber. The dynamic mediator uses a contiguous Vec-slot registry; the typed
macro uses concrete handler fields and monomorphized dispatch.

| Path | Mode | Throughput | Avg latency |
| --- | --- | ---: | ---: |
| Dynamic request | Concurrent (16 tasks) | 7.92 M msg/s | 126 ns |
| Dynamic request | Sequential | 3.53 M msg/s | 283 ns |
| Dynamic event | Sequential, three handlers | 2.63 M events/s | 379 ns |
| Typed request | Concurrent (16 tasks) | 55.73 M msg/s | 17 ns |
| Typed request | Sequential | 20.34 M msg/s | 49 ns |
| Typed event | Sequential, one handler | 16.18 M events/s | 61 ns |

| Flow benchmark | Throughput | Notes |
| --- | ---: | --- |
| Local Flow (3 steps) | 2,481,042 flows/s | Compensating sequence, in-memory CI baseline |
| Local DSL Flow (3 steps) | 691,578 flows/s | Typed DSL with state threading, CI baseline |
| CQRS + Flow + transport workflow | 642,741 workflows/s | End-to-end critical path, CI baseline |
| NATS JetStream publish/receive/ack | 2,278 msg/s | Durable, 256B payload, Docker CI baseline |

## Database Durability

The FlowStore lifecycle is dominated by durable per-commit fsync, not Catga
client overhead. Each lifecycle creates, reads, and conditionally updates flow
state; it is not a raw SQL statement rate. MySQL, PostgreSQL, and SQL Server
flush their logs for every durable commit. Virtualized disks make that sync
roughly 1-10ms, capping serial throughput. Redis persists asynchronously, and
SQLite often defers WAL sync to checkpoint.

Concurrency lets the database group durable commits without a hidden Catga pool:

| Backend | c=1 lifecycle/s | c=16 lifecycle/s | Scaling |
| --- | ---: | ---: | ---: |
| MySQL | 372 | 1,885 | 5.1x |
| PostgreSQL | 759 | 2,221 | 2.9x |
| SQL Server | 299 | 1,263 | 4.2x |
| Redis | 2,108 | 14,493 | 6.9x |

Disabling durability is diagnostic only, not a recommendation:

| Backend | Durable default (c=1) | Durability disabled (c=1) | Isolated fsync cost |
| --- | ---: | ---: | ---: |
| MySQL | 85 ops/s | 424 ops/s (`innodb_flush_log_at_trx_commit=2`, `sync_binlog=0`) | about 5x |
| PostgreSQL | 219 ops/s | 577 ops/s (`synchronous_commit=off`) | about 2.6x |

Keep writes durable by increasing application concurrency, batching related
state changes into one transaction, using database group-commit settings, and
choosing power-loss-safe low-latency storage. Repeat the release measurement on
production-like hardware before setting capacity targets.

## Reproduce

```bash
scripts/performance.sh --profile full
cargo test --release -p catga-tests --test mediator_pure_throughput --test typed_mediator_bench --test flow_performance --test critical_path_performance -- --ignored --nocapture
```

For the local Podman durability experiment:

```powershell
./scripts/performance-local.ps1
./scripts/performance-local.ps1 -Backends postgres,mysql -RelaxedDurability
```
