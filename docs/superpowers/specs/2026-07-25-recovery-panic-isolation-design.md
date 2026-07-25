# Recovery Panic Isolation Design

## Goal

Keep automatic recovery available when one third-party `RecoverableComponent`
panics, matching the source service's per-component failure isolation without
introducing a framework-owned task or unbounded state.

## Design

`RecoveryManager::recover_unhealthy` will contain the only unwind boundary in
the lifecycle subsystem. Each unhealthy component's `recover` future is polled
inside `AssertUnwindSafe(...).catch_unwind()`. A normal `Ok(())` increments the
successful count. A returned `CatgaError` or an unwind increments the failed
count, then the sweep continues with the next immutable component snapshot.

The existing atomic recovery guard remains responsible for clearing the
exclusive-sweep bit during normal returns and unwinding. `run_auto_recovery`
already retries a sweep with failures, so panic-derived failures automatically
follow its existing bounded retry and cancellation policy.

## Error Handling And Performance

The normal successful path adds no allocation and keeps the manager's
copy-on-write, lock-free component snapshot. Panic isolation is deliberately
limited to user-provided recovery code; Catga's own invariants continue to use
ordinary Rust failure semantics. No panic payload is allocated, retained, or
exposed through the public API.

## Verification

The lifecycle regression will prove that a panicking component produces a
completed sweep with one failed recovery, clears the exclusive flag, and does
not prevent a later healthy candidate from recovering. Existing automatic
recovery coverage continues to prove retries and cancellation.
