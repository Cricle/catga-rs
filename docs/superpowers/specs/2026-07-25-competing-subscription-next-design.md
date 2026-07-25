# Competing Subscription Next-Event Design

## Goal

Provide the C# `CompetingConsumer.TryProcessNextAsync` behavior in the Rust
subscription API so competing consumers can release their lease after at most
one handled event rather than holding it for an entire backlog scan.

## API And Behavior

`CompetingSubscriptionRunner::try_process_next` returns
`CatgaResult<Option<bool>>`:

* `Ok(None)` means another consumer owns the lease.
* `Ok(Some(true))` means this consumer handled exactly one matching event.
* `Ok(Some(false))` means it acquired and released the lease but found no
  matching pending event.

The method visits matching stream identifiers in stable lexical order. It
reads bounded pages from the existing `SubscriptionRunner`, advances a
per-stream checkpoint across filtered events, and stops immediately after the
first successfully handled matching event. A handler or checkpoint failure
returns its structured error and still releases the lease.

## Concurrency And Memory

The implementation allocates no event backlog and retains only one bounded
event-store page. It uses the existing store-level lease instead of a process
local mutex, so Memory, Redis, and JetStream retain their current ownership
semantics. Releasing after one event gives a scheduler a natural fairness
point without adding a hidden worker or sleep loop.

## Compatibility

`try_run_once` remains available for callers that intentionally drain a full
subscription pass. The new method is the source-compatible single-event path;
its per-stream checkpoints are stronger than the source's global position and
therefore cannot skip a lower-version event in another stream.

## Verification

The regression tests cover one-event processing, filtered-event checkpoint
advancement, lease release, and a second caller processing the next event.
