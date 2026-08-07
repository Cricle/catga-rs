//! Lock-free lookup of canonical and compatibility message type names.

use std::{any::TypeId, collections::HashMap};

use arc_swap::ArcSwap;

use crate::{CatgaError, CatgaResult, ErrorCode, Message};

#[derive(Clone, Default)]
struct TypeRules {
    types_by_name: HashMap<Box<str>, TypeId>,
    canonical_names: HashMap<TypeId, Box<str>>,
}

/// Resolves persisted message type names without runtime reflection or read-side locking.
pub struct MessageTypeRegistry {
    rules: ArcSwap<TypeRules>,
}

impl Default for MessageTypeRegistry {
    fn default() -> Self {
        Self {
            rules: ArcSwap::from_pointee(TypeRules::default()),
        }
    }
}

impl MessageTypeRegistry {
    /// Returns the stable canonical Rust type name for a message.
    pub fn canonical_name<M: Message>() -> &'static str {
        std::any::type_name::<M>()
    }

    /// Registers a message's canonical Rust type name and unqualified compatibility name.
    ///
    /// Register every persisted message type during startup before a consumer
    /// begins decoding envelopes. The short final path segment is registered
    /// as a compatibility name in addition to the canonical Rust type name.
    ///
    /// ```
    /// use catga_core::{Message, MessageTypeRegistry};
    ///
    /// #[derive(Message)]
    /// struct InvoiceIssued;
    ///
    /// let registry = MessageTypeRegistry::default();
    /// registry.register::<InvoiceIssued>()?;
    /// registry.add_alias::<InvoiceIssued>("billing.invoice-issued.v1")?;
    ///
    /// assert!(registry.is_registered::<InvoiceIssued>());
    /// assert!(registry.resolve("billing.invoice-issued.v1").is_some());
    /// # Ok::<(), catga_core::CatgaError>(())
    /// ```
    pub fn register<M: Message>(&self) -> CatgaResult<()> {
        let canonical = Self::canonical_name::<M>();
        let short = canonical.rsplit("::").next().unwrap_or(canonical);
        self.register_names(TypeId::of::<M>(), canonical, [canonical, short])
    }

    /// Registers an additional persisted compatibility alias for a message type.
    pub fn add_alias<M: Message>(&self, alias: impl Into<Box<str>>) -> CatgaResult<()> {
        self.register::<M>()?;
        let alias = alias.into();
        if alias.trim().is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "message type aliases must not be empty",
            ));
        }
        let type_id = TypeId::of::<M>();
        loop {
            let current = self.rules.load_full();
            if let Some(registered) = current.types_by_name.get(alias.as_ref()) {
                if *registered == type_id {
                    return Ok(());
                }
                return Err(alias_conflict());
            }
            let mut next = (*current).clone();
            next.types_by_name.insert(alias.clone(), type_id);
            let previous = self
                .rules
                .compare_and_swap(&current, std::sync::Arc::new(next));
            if std::sync::Arc::ptr_eq(&*previous, &current) {
                return Ok(());
            }
        }
    }

    /// Resolves a canonical or compatibility name to its registered Rust type identity.
    pub fn resolve(&self, type_name: &str) -> Option<TypeId> {
        self.rules.load().types_by_name.get(type_name).copied()
    }

    /// Returns whether the type has been explicitly registered.
    pub fn is_registered<M: Message>(&self) -> bool {
        self.rules
            .load()
            .canonical_names
            .contains_key(&TypeId::of::<M>())
    }

    fn register_names<const N: usize>(
        &self,
        type_id: TypeId,
        canonical: &str,
        names: [&str; N],
    ) -> CatgaResult<()> {
        loop {
            let current = self.rules.load_full();
            for name in names {
                if let Some(registered) = current.types_by_name.get(name)
                    && *registered != type_id
                {
                    return Err(alias_conflict());
                }
            }
            if let Some(registered) = current.canonical_names.get(&type_id) {
                if registered.as_ref() != canonical {
                    return Err(CatgaError::new(
                        ErrorCode::Conflict,
                        "a message type has a different canonical name",
                    ));
                }
                if names
                    .iter()
                    .all(|name| current.types_by_name.contains_key(*name))
                {
                    return Ok(());
                }
            }
            let mut next = (*current).clone();
            next.canonical_names
                .entry(type_id)
                .or_insert_with(|| canonical.into());
            for name in names {
                next.types_by_name.entry(name.into()).or_insert(type_id);
            }
            let previous = self
                .rules
                .compare_and_swap(&current, std::sync::Arc::new(next));
            if std::sync::Arc::ptr_eq(&*previous, &current) {
                return Ok(());
            }
        }
    }
}

fn alias_conflict() -> CatgaError {
    CatgaError::new(
        ErrorCode::Conflict,
        "a message type name is already registered for another message",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

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

        // Register alias for TestMessage
        registry
            .add_alias::<TestMessage>("shared.alias")
            .expect("valid alias");

        // Try to register the same alias for AnotherMessage
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
        // Adding the same alias again should succeed (idempotent)
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

        // Should resolve by short name (last segment of full path)
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

        // Add many aliases concurrently (simulated)
        for i in 0..100 {
            let reg = Arc::clone(&registry);
            let result = reg.add_alias::<TestMessage>(format!("alias.{}", i));
            assert!(result.is_ok(), "alias {} should be added", i);
        }

        // All aliases should be resolvable
        for i in 0..100 {
            let resolved = registry.resolve(&format!("alias.{}", i));
            assert!(resolved.is_some(), "alias {} should be resolvable", i);
        }
    }

    #[test]
    fn message_type_registry_canonical_name_extracts_full_path() {
        let name = MessageTypeRegistry::canonical_name::<TestMessage>();
        // The canonical name should contain the module path
        assert!(name.contains("TestMessage"));
    }
}
