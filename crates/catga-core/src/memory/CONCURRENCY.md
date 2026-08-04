# Concurrency Design Decisions

## DashMap vs RwLock<HashMap>

### When DashMap is appropriate

- **High write contention** — DashMap shards by key, reducing lock contention vs a single RwLock
- **Multiple independent keys** — Each shard is locked independently, so operations on different keys don't block
- **Simple operations (get/insert)** — DashMap excels at point operations

### When RwLock<HashMap> is preferable

- **High read, low write** — RwLock allows multiple concurrent readers
- **Complex atomic operations** — Transactional operations spanning multiple keys need a single lock
- **Memory efficiency** — DashMap has per-shard overhead; RwLock+HashMap is more compact

## Current Usage in memory/ modules

| File | Usage Pattern | Data Structure | Assessment |
|------|---------------|----------------|------------|
| `event_store.rs` | Stream routing | `DashMap<Box<str>, Arc<MemoryEventStream>>` | ✅ Appropriate — streams are independent |
| `snapshot.rs` | Snapshot routing | `DashMap<Box<str>, Arc<MemorySnapshotSlot>>` | ✅ Appropriate |
| `transport.rs` | Destination routing | `Arc<DashMap<Destination, Arc<MemoryDestination>>` | ✅ Appropriate |
| `inbox.rs` | Claims + completed | Two `DashMap<u64, ...>` | ✅ Appropriate — high concurrency |
| `outbox.rs` | Messages + published | Two `DashMap<u64, ...>` | ✅ Appropriate |
| `idempotency.rs` | Records + completed | Two `DashMap<Box<str>, ...>` | ✅ Appropriate |
| `subscription.rs` | Nested subscriptions | Nested `DashMap` | ⚠️ Consider: outer map has low contention |
| `projection.rs` | Nested projections | Nested `DashMap` | ⚠️ Consider: outer map has low contention |
| `dead_letter.rs` | Letter storage | `DashMap<u64, DeadLetter>` | ✅ Appropriate |
| `lease.rs` | Lease management | `DashMap<Box<str>, Lease>` | ✅ Appropriate |
| `flow.rs` | Flow tracking | `DashMap<Box<str>, Arc<FlowSlot>>` | ✅ Appropriate |
| `suspended_flow.rs` | Suspended flows | Two `DashMap` | ✅ Appropriate |
| `read_model.rs` | Changes + models | Two `DashMap<Box<str>, ...>` | ✅ Appropriate |
| `state_machine.rs` | Snapshot tracking | `DashMap<Box<str>, Arc<SnapshotSlot<S>>>` | ✅ Appropriate |
| `dsl_progress.rs` | Step progress | `DashMap<(Box<str>, u32), DslStepProgress>` | ✅ Appropriate |

## Test utilities

DashMap is used in `testing/` modules for capturing test data. This is appropriate for test code where simplicity and thread-safety matter more than memory efficiency.

## Summary

All DashMap usages in production `memory/` modules are appropriate for their access patterns:

- **Sharded by key** — Each stream/subscription/flow has independent access patterns
- **High concurrency expected** — In production, these stores handle concurrent requests
- **Simple operations** — Primarily get/insert/delete, not complex transactions

No changes recommended unless profiling shows contention issues.
