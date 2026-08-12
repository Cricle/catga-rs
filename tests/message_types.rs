//! Message type registry and naming contract tests.

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

/// Public message types exercising macro visibility propagation across module boundaries.
///
/// The generated `{Name}TypeId` companions must inherit the input type's visibility,
/// otherwise the public trait impls leak a private type (E0446).
pub mod public_messages {
    /// Request declared `pub`; its generated `PublicGetBalanceTypeId` must also be `pub`.
    #[catga_core::catga_request(response = u64)]
    pub struct PublicGetBalance;

    /// Command declared `pub`; its generated `PublicLogAuditTypeId` must also be `pub`.
    #[derive(catga_core::catga_command)]
    pub struct PublicLogAudit;

    /// Event declared `pub`; its generated `PublicOrderShippedTypeId` must also be `pub`.
    #[derive(Clone, catga_core::catga_event)]
    pub struct PublicOrderShipped;
}

#[test]
fn macros_propagate_visibility_to_generated_type_id_companions() {
    use catga_core::{Command, Event, MessageTypeId, Request};
    use public_messages::{
        PublicGetBalance, PublicGetBalanceTypeId, PublicLogAudit, PublicLogAuditTypeId,
        PublicOrderShipped, PublicOrderShippedTypeId,
    };

    assert_eq!(
        TypeId::of::<<PublicGetBalance as Request>::TypeId>(),
        TypeId::of::<PublicGetBalanceTypeId>()
    );
    assert_eq!(PublicGetBalanceTypeId::NAME, "PublicGetBalance");

    assert_eq!(
        TypeId::of::<<PublicLogAudit as Command>::TypeId>(),
        TypeId::of::<PublicLogAuditTypeId>()
    );
    assert_eq!(PublicLogAuditTypeId::NAME, "PublicLogAudit");

    assert_eq!(
        TypeId::of::<<PublicOrderShipped as Event>::TypeId>(),
        TypeId::of::<PublicOrderShippedTypeId>()
    );
    assert_eq!(PublicOrderShippedTypeId::NAME, "PublicOrderShipped");
}
