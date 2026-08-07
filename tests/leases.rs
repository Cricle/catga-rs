//! Distributed-lease contract tests.

use std::time::Duration;

use catga_core::LeaseStore;
use catga_core::memory::MemoryLeases;

#[tokio::test]
async fn leases_exclusively_acquire_renew_expire_and_release_by_owner() {
    let leases = MemoryLeases::default();

    assert!(
        leases
            .try_acquire("outbox", "node-a", Duration::from_secs(1))
            .await
            .unwrap()
    );
    assert!(
        !leases
            .try_acquire("outbox", "node-b", Duration::from_secs(1))
            .await
            .unwrap()
    );
    assert!(
        leases
            .renew("outbox", "node-a", Duration::from_secs(1))
            .await
            .unwrap()
    );
    assert!(!leases.release("outbox", "node-b").await.unwrap());
    assert!(leases.release("outbox", "node-a").await.unwrap());
    assert!(
        leases
            .try_acquire("outbox", "node-b", Duration::ZERO)
            .await
            .unwrap()
    );
    assert!(
        leases
            .try_acquire("outbox", "node-a", Duration::from_secs(1))
            .await
            .unwrap()
    );
}
