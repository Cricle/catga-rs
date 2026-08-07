# Lifecycle Management

## TransportLifecycle

Explicitly manages transport layer lifecycle:

```rust
use catga_core::{TransportLifecycle, TransportLifecycleOptions};

let lifecycle = TransportLifecycle::new(transport)
    .with_options(TransportLifecycleOptions {
        drain_timeout: Duration::from_secs(30),
        ..Default::default()
    });

// Start
lifecycle.start().await?;

// Stop accepting new messages
lifecycle.stop_accepting().await?;

// Drain in progress
lifecycle.drain().await?;

// Complete shutdown
lifecycle.shutdown().await?;
```

## AcceptanceGate

Lock-free stop-accepting component:

```rust
use catga_core::AcceptanceGate;

// Check if accepting new requests
if gate.is_accepting() {
    // Handle request
}

// Stop accepting
gate.close();
```

## OperationTracker

RAII drain slots:

```rust
use catga_core::{OperationTracker, OperationGuard};

let tracker = OperationTracker::new(10);  // 10 slots

// Acquire a slot
let guard = tracker.acquire().await?;
// guard automatically releases the slot
drop(guard);

// Wait for all slots to be released
tracker.drain().await?;
```

## RecoveryManager

Automatic recovery management:

```rust
use catga_core::{RecoveryManager, AutoRecoveryOptions};

let manager = RecoveryManager::new(transport)
    .with_options(AutoRecoveryOptions {
        retry_interval: Duration::from_secs(5),
        max_retries: 10,
        backoff: RetryJitter::exponential(Duration::from_secs(1)),
    });

// Start recovery loop
manager.start().await?;

// Manually trigger recovery
manager.recover().await?;
```

## ShutdownCoordinator

Coordinates multi-component shutdown:

```rust
use catga_core::ShutdownCoordinator;

let coordinator = ShutdownCoordinator::new();

// Register components
coordinator.register("transport", transport_lifecycle);
coordinator.register("consumer", consumer_lifecycle);

// Trigger shutdown
coordinator.shutdown().await?;

// Wait for completion
coordinator.await_termination().await?;
```
