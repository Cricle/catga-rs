# Subscription Runtime Loop Design

## Goal

Map the C# `SubscriptionRunner.RunContinuouslyAsync` capability to an explicit
Rust future that applications own, configure, and cancel without a hidden
runtime task.

## API

`SubscriptionLoopOptions::new(poll_interval)` validates a nonzero interval.
Its default is 100 milliseconds, matching the source behavior while allowing
applications to choose a lower-overhead schedule for durable stores.

`SubscriptionRunner::run_until_cancelled(subscription_name, options,
shutdown)` executes `run_once` immediately, then waits for either the next
interval or cancellation. It returns a saturating `SubscriptionRun` aggregate
when cancelled. Store, handler, and checkpoint errors remain structured
`CatgaError` values and end the caller-owned future rather than being silently
retried.

## Resource And Cancellation Semantics

One iteration keeps the existing bounded event-store page and has no retained
backlog. The method creates no task, channel, or timer outside its own future.
Cancellation is checked before a pass and during the wait between passes; an
already-started pass completes so a successfully handled event can persist its
checkpoint before the caller observes a clean shutdown.

## Verification

The integration test confirms that the first pass runs immediately despite a
long interval, cancellation ends the loop promptly, and the returned aggregate
reports the completed pass. It also checks validation rejects a zero interval.
