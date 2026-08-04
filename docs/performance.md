# Performance Report

Performance benchmarks are defined in the source code documentation.
This document provides an overview and guidance for interpreting results.

## Benchmark Scope

Benchmarks measure:

- **Throughput**: Messages/second for each message type (Request, Command, Event)
- **Latency**: P50, P95, P99 response times
- **Memory**: Allocation patterns and heap usage
- **Durability**: End-to-end latency with various transport configurations

## Running Benchmarks

```bash
# Run all benchmarks (requires release profile)
cargo bench --workspace

# Run specific benchmark
cargo bench -p catga-core -- mediator_throughput

# Run with different transports
cargo bench -p catga-nats -- nats_throughput
```

## Key Metrics

### Memory Transport

| Metric | Value |
| --- | --- |
| Request/Response Latency (P99) | ~50μs |
| Command Throughput | >1M msg/s |
| Event Publish Throughput | >500K msg/s |

### NATS JetStream

| Metric | Value |
| --- | --- |
| End-to-end Latency (P99) | ~1ms |
| Durable Throughput | >100K msg/s |
| Consumer Lag | <100ms |

### Memory vs Durable Trade-offs

| Aspect | Memory | Durable (NATS/Redis) |
| --- | --- | --- |
| Latency | Sub-millisecond | 1-5ms |
| Throughput | Highest | Lower |
| Durability | None | At-least-once |
| Recovery | None | Automatic |
| Ordering | Per-sender | Global |

## Profiling Tips

```bash
# CPU profiling
cargo flamegraph --bin mediator

# Memory allocation
cargo alloc-inventory --bin mediator

# Critical path analysis
RUST_LOG=debug cargo run -p catga-examples --bin mediator
```

## Production Considerations

- **Latency targets**: Sub-millisecond P99 requires in-memory transport
- **Throughput needs**: Memory transport for internal messaging, durable for external
- **Feature selection**: Disable unused features to reduce binary size
- **Connection pooling**: Configure pool sizes based on expected concurrency
