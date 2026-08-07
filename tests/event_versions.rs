//! Event schema-upgrade contract tests.

use std::sync::Arc;

use catga_core::memory::MemoryEventStore;
use catga_core::{
    Envelope, EventStore, EventUpgrader, EventVersionRegistry, MessageMetadata, UpgradingEventStore,
};

struct V1ToV2;

impl EventUpgrader for V1ToV2 {
    fn source_type(&self) -> &str {
        "order.created.v1"
    }

    fn target_type(&self) -> &str {
        "order.created.v2"
    }

    fn source_version(&self) -> u32 {
        1
    }

    fn target_version(&self) -> u32 {
        2
    }

    fn upgrade(&self, source: Envelope) -> catga_core::CatgaResult<Envelope> {
        Ok(Envelope::versioned(
            source.id(),
            self.target_type(),
            [source.payload(), &[2]].concat(),
            source.metadata(),
            self.target_version(),
        ))
    }
}

struct V2ToV3;

impl EventUpgrader for V2ToV3 {
    fn source_type(&self) -> &str {
        "order.created.v2"
    }

    fn target_type(&self) -> &str {
        "order.created.v3"
    }

    fn source_version(&self) -> u32 {
        2
    }

    fn target_version(&self) -> u32 {
        3
    }

    fn upgrade(&self, source: Envelope) -> catga_core::CatgaResult<Envelope> {
        Ok(Envelope::versioned(
            source.id(),
            self.target_type(),
            [source.payload(), &[3]].concat(),
            source.metadata(),
            self.target_version(),
        ))
    }
}

struct InvalidV1ToV2;

impl EventUpgrader for InvalidV1ToV2 {
    fn source_type(&self) -> &str {
        "order.created.v1"
    }

    fn target_type(&self) -> &str {
        "order.created.v2"
    }

    fn source_version(&self) -> u32 {
        1
    }

    fn target_version(&self) -> u32 {
        2
    }

    fn upgrade(&self, source: Envelope) -> catga_core::CatgaResult<Envelope> {
        Ok(Envelope::versioned(
            source.id(),
            self.source_type(),
            source.payload().to_vec(),
            source.metadata(),
            self.source_version(),
        ))
    }
}

#[test]
fn event_version_registry_upgrades_envelopes_through_immutable_rule_snapshots() {
    let registry = EventVersionRegistry::default();
    registry.register(Arc::new(V1ToV2)).unwrap();
    registry.register(Arc::new(V2ToV3)).unwrap();

    let upgraded = registry
        .upgrade_to_latest(Envelope::versioned(
            7,
            "order.created.v1",
            vec![1],
            MessageMetadata::new(7, None),
            1,
        ))
        .unwrap();
    assert_eq!(upgraded.message_type(), "order.created.v3");
    assert_eq!(upgraded.schema_version(), 3);
    assert_eq!(upgraded.payload(), [1, 2, 3]);
    assert_eq!(registry.current_version("order.created.v3"), 3);
    assert!(registry.has_upgraders("order.created.v1"));
}

#[test]
fn event_version_registry_rejects_duplicate_source_steps() {
    let registry = EventVersionRegistry::default();
    registry.register(Arc::new(V1ToV2)).unwrap();

    assert_eq!(
        registry.register(Arc::new(V1ToV2)).unwrap_err().code(),
        catga_core::ErrorCode::Conflict
    );
}

#[test]
fn event_version_registry_rejects_an_upgrader_with_an_invalid_declared_output() {
    let registry = EventVersionRegistry::default();
    registry.register(Arc::new(InvalidV1ToV2)).unwrap();

    assert_eq!(
        registry
            .upgrade_to_latest(Envelope::versioned(
                7,
                "order.created.v1",
                vec![1],
                MessageMetadata::new(7, None),
                1,
            ))
            .unwrap_err()
            .code(),
        catga_core::ErrorCode::Validation
    );
}

#[tokio::test]
async fn upgrading_event_store_transforms_read_views_without_mutating_history() {
    let registry = EventVersionRegistry::default();
    registry.register(Arc::new(V1ToV2)).unwrap();
    let history = MemoryEventStore::default();
    history
        .append(
            "orders-7",
            vec![Envelope::versioned(
                7,
                "order.created.v1",
                vec![1],
                MessageMetadata::new(7, None),
                1,
            )],
            None,
        )
        .await
        .unwrap();

    let upgraded = UpgradingEventStore::new(&history, &registry);
    let view = upgraded.read_page("orders-7", 0, 1).await.unwrap();
    let view = view.stream();
    assert_eq!(
        view.events()[0].envelope().message_type(),
        "order.created.v2"
    );
    assert_eq!(view.events()[0].envelope().schema_version(), 2);
    assert_eq!(view.events()[0].envelope().payload(), [1, 2]);

    let original = history.read_page("orders-7", 0, 1).await.unwrap();
    let original = original.stream();
    assert_eq!(
        original.events()[0].envelope().message_type(),
        "order.created.v1"
    );
    assert_eq!(original.events()[0].envelope().schema_version(), 1);
}
