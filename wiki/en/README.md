# Catga - Rust Event-Driven Distributed Runtime

![Catga Logo](assets/catga-logo.svg)

**Catga** is a pure Rust implementation of an event-driven distributed runtime with complete CQRS and Event Sourcing patterns.

## Core Features

| Feature | Description |
|---------|-------------|
| **CQRS** | Complete Command Query Responsibility Separation implementation |
| **Event Sourcing** | Event sourcing and aggregate root management |
| **Distributed** | Multi-protocol support: NATS, Redis, RocketMQ |
| **Workflow** | Persistent state machine + compensating transactions |
| **High Performance** | Zero GC, no JIT overhead, extreme memory optimization |
| **Type Safety** | Compile-time type checking, end-to-end type inference |

## Performance Comparison

Catga's performance advantages over cqrs-es:

```
Benchmark (1000 events, single aggregate)
─────────────────────────────────────────
Catga     : 0.8ms   (zero heap allocation hot path)
cqrs-es   : 12ms    (generic specialization overhead)
Speedup   : 15x faster

Memory per aggregate (1MB events)
─────────────────────────────────────────
Catga     : ~2KB    (Arena allocation)
cqrs-es   : ~50KB   (dynamic sharding)
Memory    : 25x less
```

Detailed performance analysis: [Performance](./advanced/performance.md)

## Quick Start

```toml
[dependencies]
catga-core = "0.2"
async-trait = "0.1"
```

```rust
use catga_core::auto::AutoApp;
use catga_core::{CatgaResult, Handler, Message, Request};

struct Add(i64);
impl Message for Add {}
impl Request for Add {
    type Response = i64;
}

struct AddHandler;
#[async_trait::async_trait]
impl Handler<Add> for AddHandler {
    async fn handle(&self, msg: Add) -> CatgaResult<i64> {
        Ok(msg.0 * 2)
    }
}

# async fn run() -> CatgaResult<()> {
let app = AutoApp::builder()
    .handler(AddHandler)?
    .build()?;
# Ok(())
# }
```

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                      Application                            │
├─────────────────────────────────────────────────────────────┤
│  AutoApp                                                    │
│  ├── Mediator (request routing)                            │
│  ├── Registry (handler registration)                       │
│  └── Behaviors (cross-cutting concerns)                    │
├─────────────────────────────────────────────────────────────┤
│  Transport Layer (pluggable)                                │
│  ├── catga-core::memory (in-process)                       │
│  ├── catga-nats    (NATS JetStream)                        │
│  ├── catga-redis   (Redis Streams)                         │
│  └── catga-robustmq (RocketMQ)                             │
├─────────────────────────────────────────────────────────────┤
│  Persistence Layer                                          │
│  ├── EventStore (event storage)                            │
│  ├── SnapshotStore (snapshots)                             │
│  └── ReadModelStore (read models)                          │
└─────────────────────────────────────────────────────────────┘
```

## Documentation

### Getting Started
- [Installation](./getting-started/installation.md)
- [First Application](./getting-started/first-app.md)
- [Core Concepts](./getting-started/concepts.md)
- [Testing Guide](./getting-started/testing.md)

### Core Modules
- [Message & Handler](./core/message-handler.md)
- [Mediator & Registry](./core/mediator-registry.md)
- [CQRS Pattern](./core/cqrs.md)
- [Event Sourcing](./core/event-sourcing.md)

### Distributed
- [NATS Transport](./distributed/nats.md)
- [Redis Streams](./distributed/redis.md)
- [RocketMQ](./distributed/robustmq.md)
- [Cluster Mode](./distributed/cluster.md)
- [sorock Multi-Raft Backend](./distributed/sorock.md)

### Workflow
- [Flow Overview](./flow/overview.md)
- [State Machine](./flow/state-machine.md)
- [Compensating Transactions](./flow/compensation.md)

### Advanced Topics
- [Performance Optimization](./advanced/performance.md)
- [Type System](./advanced/type-system.md)
- [Lifecycle Management](./advanced/lifecycle.md)
- [CI Tiers and Release](./advanced/ci-release.md)

### Comparison
- [Catga vs cqrs-es Deep Comparison](./comparison/cqrs-es.md)

### Skill (Application Development Guide)
- [Application Development Guide (SKILL)](./skill/SKILL.md)
- [Mediator: Messages, Handlers, and Dispatch](./skill/mediator.md)
- [Pipeline: Request Strategies and Built-in Behaviors](./skill/pipeline.md)
- [Flow: Compensating Flow and Workflow](./skill/flow.md)
- [StateMachine: Event-Driven Persistent State Machine](./skill/state-machine.md)
- [Transport: Message Transport Contracts and Adapters](./skill/transport.md)
- [Message Reliability Patterns: Outbox / Inbox / Idempotency / Dead Letter / Subscription / Consumer Loops](./skill/reliability.md)
- [Event Sourcing, Projection, and Read Model](./skill/event-sourcing.md)
- [Stores: Persistent Storage](./skill/stores.md)
- [Distributed Components: Cluster / Raft / Distributed ID / Lease / Task Scheduling](./skill/distributed.md)
- [HTTP Integration (catga-axum)](./skill/http.md)
- [Codec, Compression, and Message Signing](./skill/codec.md)
- [Error Handling, Idempotency, and Production Checklist](./skill/production.md)
- [Catga Auto and Runtime Correctness Implementation Plan](./skill/auto-plan.md)
- [catga-auto design](./skill/auto-design.md)
- [Catga Auto Examples Implementation Plan](./skill/auto-examples-plan.md)

## Community & Support

- GitHub: https://github.com/catga-rs/catga-rs
- Documentation: https://catga.rs
