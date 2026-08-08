//! Unit tests for unit_message_type.

use catga_core::{ErrorCode, Message, MessageTypeRegistry};

struct TestMessage;
impl Message for TestMessage {}

struct AnotherMessage;
impl Message for AnotherMessage {}

#[test]
fn message_type_registry_default() {
    let registry = MessageTypeRegistry::default();
    assert!(!registry.is_registered::<TestMessage>());
}

#[test]
fn message_type_registry_register_adds_type() {
    let registry = MessageTypeRegistry::default();
    registry
        .register::<TestMessage>()
        .expect("valid registration");
    assert!(registry.is_registered::<TestMessage>());
}

#[test]
fn message_type_registry_resolve_returns_type_id() {
    let registry = MessageTypeRegistry::default();
    registry
        .register::<TestMessage>()
        .expect("valid registration");

    let type_name = MessageTypeRegistry::canonical_name::<TestMessage>();
    let resolved = registry.resolve(type_name);
    assert!(resolved.is_some());
}

#[test]
fn message_type_registry_resolve_returns_none_for_unregistered() {
    let registry = MessageTypeRegistry::default();
    let resolved = registry.resolve("unregistered.Type");
    assert!(resolved.is_none());
}

#[test]
fn message_type_registry_add_alias_creates_alias() {
    let registry = MessageTypeRegistry::default();
    registry
        .register::<TestMessage>()
        .expect("valid registration");
    registry
        .add_alias::<TestMessage>("custom.alias.v1")
        .expect("valid alias");

    let resolved = registry.resolve("custom.alias.v1");
    assert!(resolved.is_some());
}

#[test]
fn message_type_registry_add_alias_rejects_empty_alias() {
    let registry = MessageTypeRegistry::default();
    registry
        .register::<TestMessage>()
        .expect("valid registration");

    let result = registry.add_alias::<TestMessage>("");
    assert!(result.is_err());
    assert_eq!(
        result.expect_err("validation error expected").code(),
        ErrorCode::Validation
    );
}

#[test]
fn message_type_registry_add_alias_rejects_whitespace_alias() {
    let registry = MessageTypeRegistry::default();
    registry
        .register::<TestMessage>()
        .expect("valid registration");

    let result = registry.add_alias::<TestMessage>("   ");
    assert!(result.is_err());
    assert_eq!(
        result.expect_err("validation error expected").code(),
        ErrorCode::Validation
    );
}

#[test]
fn message_type_registry_add_alias_rejects_conflict() {
    let registry = MessageTypeRegistry::default();
    registry
        .register::<TestMessage>()
        .expect("valid registration");
    registry
        .register::<AnotherMessage>()
        .expect("valid registration");

    registry
        .add_alias::<TestMessage>("shared.alias")
        .expect("valid alias");

    let result = registry.add_alias::<AnotherMessage>("shared.alias");
    assert!(result.is_err());
    assert_eq!(
        result.expect_err("conflict expected").code(),
        ErrorCode::Conflict
    );
}

#[test]
fn message_type_registry_add_alias_allows_same_alias_for_same_type() {
    let registry = MessageTypeRegistry::default();
    registry
        .register::<TestMessage>()
        .expect("valid registration");

    registry
        .add_alias::<TestMessage>("same.alias")
        .expect("first registration");
    registry
        .add_alias::<TestMessage>("same.alias")
        .expect("duplicate registration");
}

#[test]
fn message_type_registry_resolve_with_short_name() {
    let registry = MessageTypeRegistry::default();
    registry
        .register::<TestMessage>()
        .expect("valid registration");

    let short_name = "TestMessage";
    let resolved = registry.resolve(short_name);
    assert!(resolved.is_some());
}

#[test]
fn message_type_registry_multiple_registrations() {
    let registry = MessageTypeRegistry::default();

    registry
        .register::<TestMessage>()
        .expect("valid registration");
    registry
        .register::<AnotherMessage>()
        .expect("valid registration");

    registry
        .add_alias::<TestMessage>("test.alias")
        .expect("valid alias");
    registry
        .add_alias::<AnotherMessage>("another.alias")
        .expect("valid alias");

    assert!(registry.resolve("test.alias").is_some());
    assert!(registry.resolve("another.alias").is_some());
}

#[test]
fn message_type_registry_concurrent_registration_is_thread_safe() {
    use std::sync::Arc;

    let registry = Arc::new(MessageTypeRegistry::default());
    registry
        .register::<TestMessage>()
        .expect("valid registration");

    for i in 0..100 {
        let reg = Arc::clone(&registry);
        let result = reg.add_alias::<TestMessage>(format!("alias.{}", i));
        assert!(result.is_ok(), "alias {} should be added", i);
    }

    for i in 0..100 {
        let resolved = registry.resolve(&format!("alias.{}", i));
        assert!(resolved.is_some(), "alias {} should be resolvable", i);
    }
}

#[test]
fn message_type_registry_canonical_name_extracts_full_path() {
    let name = MessageTypeRegistry::canonical_name::<TestMessage>();
    assert!(name.contains("TestMessage"));
}
