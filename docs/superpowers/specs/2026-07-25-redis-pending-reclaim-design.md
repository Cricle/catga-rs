# Redis Pending Reclaim Design

## Goal

Recover a Redis Streams delivery whose previous consumer has stopped making
progress, without retaining a pending backlog in the Rust process or changing
the acknowledgement ownership contract.

## Public API

`RedisTransport::connect` remains the simple, compatible constructor and uses
`RedisPendingReclaimOptions::default()`.  A new
`RedisTransport::connect_with_reclaim_options` constructor accepts a validated
`RedisPendingReclaimOptions` value.  The options own a minimum idle duration
and a maximum number of Redis scan commands per receive attempt.

The default minimum idle duration is 30 seconds.  The default scan limit is
small and fixed.  Construction rejects a zero duration, a duration that cannot
be represented as Redis milliseconds, and a zero or oversized scan limit with
`ErrorCode::Validation` before opening a Redis connection.

## Receive Algorithm

When the per-stream recovery gate is held, the transport first reads one
pending entry assigned to its own consumer. If none is available, it reads one
`XPENDING` record and conditionally issues one `XCLAIM` only when the record is
idle and belongs to a different consumer. Each scan therefore examines and
claims at most one entry. A small cursor per stream records the next pending
range, so repeated receives eventually cover
the group pending list instead of repeatedly examining only its prefix.

The receive attempt executes at most the configured number of reclaim scans.
It immediately returns the first claimed entry, preserving its broker delivery
attempt count.  If nothing can be reclaimed, `XREADGROUP >` waits for a short,
fixed interval before retrying the bounded reclaim pass.  This eventually walks
a large pending list even when no new message arrives.  The recovery gate
prevents two local receivers from reclaiming the same stream concurrently.

## Memory and Concurrency

The implementation retains only one pending metadata record, the single
`StreamId` returned by one `XCLAIM` command, and one boxed cursor per stream
already represented in the transport's in-flight map. It does not enumerate a
`XPENDING` result set,
collect a batch, spawn retry tasks, or hold a DashMap guard across an await.
The acknowledgement token carries the delivery consumer name. A short Redis
Lua script atomically verifies that name against the current pending owner
before issuing `XACK`, so a stale delivery cannot acknowledge a later claim.
Failed acknowledgements leave the broker entry recoverable.

## Error Handling and Tests

Redis command failures remain `CatgaError` values through the existing
`map_error` conversion.  Invalid reclaim options fail locally with validation
errors.  Tests cover constructor validation without Redis and, when
`CATGA_REDIS_URL` is available, two consumer identities: the first leaves an
entry pending, the second reclaims it only after the idle threshold and can
acknowledge it.  The integration test is explicitly ignored by default because
the repository does not provision a Redis service.
