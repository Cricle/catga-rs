//! Versioned transport tests.

use std::sync::Arc;

use catga_core::{
    CatgaResult, Envelope, EventUpgrader, EventVersionRegistry, MessageMetadata, MessageTransport,
    VersionedMessageTransport,
};
use catga_memory::MemoryTransport;

struct RenameOrderV1;

impl EventUpgrader for RenameOrderV1 {
    fn source_type(&self) -> &str {
        "orders.created.v1"
    }

    fn target_type(&self) -> &str {
        "orders.created.v2"
    }

    fn source_version(&self) -> u32 {
        1
    }

    fn target_version(&self) -> u32 {
        2
    }

    fn upgrade(&self, source: Envelope) -> CatgaResult<Envelope> {
        Ok(Envelope::versioned(
            source.id(),
            self.target_type(),
            source.payload().to_vec(),
            source.metadata(),
            self.target_version(),
        ))
    }
}

#[tokio::test]
async fn receive_upgrades_the_envelope_and_preserves_delivery_acknowledgement() {
    let registry = Arc::new(EventVersionRegistry::default());
    registry
        .register(Arc::new(RenameOrderV1))
        .expect("upgrader registration succeeds");
    let inner = Arc::new(MemoryTransport::new(1).expect("bounded transport is valid"));
    let transport = VersionedMessageTransport::new(Arc::clone(&inner), registry);

    transport
        .publish(Envelope::new(
            42,
            "orders.created.v1",
            vec![1, 2, 3],
            MessageMetadata::new(42, Some(7)),
        ))
        .await
        .expect("message is published");
    let delivery = transport.receive().await.expect("message is received");

    assert_eq!(delivery.envelope().id(), 42);
    assert_eq!(delivery.envelope().message_type(), "orders.created.v2");
    assert_eq!(delivery.envelope().schema_version(), 2);
    assert_eq!(delivery.envelope().payload(), &[1, 2, 3]);
    delivery
        .acknowledge()
        .await
        .expect("upgraded delivery retains its acknowledgement");
}
