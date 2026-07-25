use std::any::TypeId;

use catga_core::{ErrorCode, Message, MessageTypeRegistry};

struct OrderCreated;

impl Message for OrderCreated {}

struct OtherMessage;

impl Message for OtherMessage {}

#[test]
fn message_type_registry_resolves_canonical_short_and_compatibility_names() {
    let registry = MessageTypeRegistry::default();

    registry.register::<OrderCreated>().unwrap();
    registry
        .add_alias::<OrderCreated>("orders.created.v1")
        .unwrap();

    assert_eq!(
        registry.resolve(MessageTypeRegistry::canonical_name::<OrderCreated>()),
        Some(TypeId::of::<OrderCreated>())
    );
    assert_eq!(
        registry.resolve("OrderCreated"),
        Some(TypeId::of::<OrderCreated>())
    );
    assert_eq!(
        registry.resolve("orders.created.v1"),
        Some(TypeId::of::<OrderCreated>())
    );
    assert_eq!(
        registry
            .add_alias::<OtherMessage>("orders.created.v1")
            .unwrap_err()
            .code(),
        ErrorCode::Conflict
    );
}
