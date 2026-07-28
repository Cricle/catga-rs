//! Real-service state-machine persistence integration tests.

use catga_codec_memorypack::MemoryPackable;
use catga_flow::{StateMachineSnapshot, StateMachineStore};
use catga_nats::NatsStateMachines;
use catga_redis::RedisStateMachines;

#[derive(Clone, Debug, Eq, PartialEq, MemoryPackable)]
struct PersistedOrder {
    quantity: u32,
    paid: bool,
}

#[tokio::test]
#[ignore = "requires CATGA_REDIS_URL"]
async fn redis_state_machines_preserve_snapshots_and_versions() {
    let server = std::env::var("CATGA_REDIS_URL")
        .expect("CATGA_REDIS_URL must be set for ignored Redis tests");
    let store = RedisStateMachines::<PersistedOrder>::connect(
        &server,
        format!("catga:state-machines:{}", std::process::id()),
    )
    .await
    .unwrap();
    assert_state_machine_store_contract(&store).await;
}

#[tokio::test]
#[ignore = "requires CATGA_NATS_URL"]
async fn nats_state_machines_preserve_snapshots_and_versions() {
    let server =
        std::env::var("CATGA_NATS_URL").expect("CATGA_NATS_URL must be set for ignored NATS tests");
    let store = NatsStateMachines::<PersistedOrder>::connect(
        &server,
        format!("CATGA_STATE_MACHINES_{}", std::process::id()),
    )
    .await
    .unwrap();
    assert_state_machine_store_contract(&store).await;
}

async fn assert_state_machine_store_contract<Store>(store: &Store)
where
    Store: StateMachineStore<PersistedOrder>,
{
    let initial = StateMachineSnapshot::new(
        "order-7",
        PersistedOrder {
            quantity: 3,
            paid: false,
        },
    );
    assert!(store.create(initial.clone()).await.unwrap());
    assert!(!store.create(initial.clone()).await.unwrap());

    let next = initial
        .next_version(PersistedOrder {
            quantity: 3,
            paid: true,
        })
        .unwrap();
    assert!(store.update(initial.version(), next.clone()).await.unwrap());
    assert!(!store.update(initial.version(), next.clone()).await.unwrap());

    let restored = store.get("order-7").await.unwrap().unwrap();
    assert_eq!(restored, next);

    let racing = StateMachineSnapshot::new(
        "order-race",
        PersistedOrder {
            quantity: 1,
            paid: false,
        },
    );
    assert!(store.create(racing.clone()).await.unwrap());
    let first = racing
        .next_version(PersistedOrder {
            quantity: 2,
            paid: true,
        })
        .unwrap();
    let second = racing
        .next_version(PersistedOrder {
            quantity: 3,
            paid: true,
        })
        .unwrap();
    let (first, second) = tokio::join!(
        store.update(racing.version(), first),
        store.update(racing.version(), second),
    );
    assert_eq!(
        usize::from(first.unwrap()) + usize::from(second.unwrap()),
        1
    );
}
