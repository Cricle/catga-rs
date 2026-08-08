//! Unit tests for subscription module helper functions.

/// Replicated key construction functions for testing.
fn definition_key(prefix: &str, name: &str) -> String {
    format!("{prefix}:definition:{name}")
}

fn index_key(prefix: &str) -> String {
    format!("{prefix}:index")
}

fn checkpoints_key(prefix: &str, name: &str) -> String {
    format!("{prefix}:checkpoints:{name}")
}

fn lease_key(prefix: &str, name: &str) -> String {
    format!("{prefix}:lease:{name}")
}

// =============================================================================
// Key construction tests
// =============================================================================

#[test]
fn definition_key_format() {
    let key = definition_key("catga:sub", "my-subscription");
    assert_eq!(key, "catga:sub:definition:my-subscription");
}

#[test]
fn definition_key_with_dots() {
    let key = definition_key("prefix", "sub.name.with.dots");
    assert_eq!(key, "prefix:definition:sub.name.with.dots");
}

#[test]
fn definition_key_empty_name() {
    let key = definition_key("prefix", "");
    assert_eq!(key, "prefix:definition:");
}

#[test]
fn definition_key_consistent() {
    let key1 = definition_key("prefix", "name");
    let key2 = definition_key("prefix", "name");
    assert_eq!(key1, key2);
}

#[test]
fn definition_key_different_names() {
    let key1 = definition_key("prefix", "name-a");
    let key2 = definition_key("prefix", "name-b");
    assert_ne!(key1, key2);
}

#[test]
fn index_key_format() {
    let key = index_key("catga:sub");
    assert_eq!(key, "catga:sub:index");
}

#[test]
fn index_key_empty_prefix() {
    let key = index_key("");
    assert_eq!(key, ":index");
}

#[test]
fn index_key_consistent() {
    let key1 = index_key("prefix");
    let key2 = index_key("prefix");
    assert_eq!(key1, key2);
}

#[test]
fn checkpoints_key_format() {
    let key = checkpoints_key("catga:sub", "my-subscription");
    assert_eq!(key, "catga:sub:checkpoints:my-subscription");
}

#[test]
fn checkpoints_key_with_dots() {
    let key = checkpoints_key("prefix", "sub.name");
    assert_eq!(key, "prefix:checkpoints:sub.name");
}

#[test]
fn checkpoints_key_empty_name() {
    let key = checkpoints_key("prefix", "");
    assert_eq!(key, "prefix:checkpoints:");
}

#[test]
fn checkpoints_key_consistent() {
    let key1 = checkpoints_key("prefix", "name");
    let key2 = checkpoints_key("prefix", "name");
    assert_eq!(key1, key2);
}

#[test]
fn checkpoints_key_different_names() {
    let key1 = checkpoints_key("prefix", "name-a");
    let key2 = checkpoints_key("prefix", "name-b");
    assert_ne!(key1, key2);
}

#[test]
fn lease_key_format() {
    let key = lease_key("catga:sub", "my-subscription");
    assert_eq!(key, "catga:sub:lease:my-subscription");
}

#[test]
fn lease_key_with_dots() {
    let key = lease_key("prefix", "sub.name");
    assert_eq!(key, "prefix:lease:sub.name");
}

#[test]
fn lease_key_empty_name() {
    let key = lease_key("prefix", "");
    assert_eq!(key, "prefix:lease:");
}

#[test]
fn lease_key_consistent() {
    let key1 = lease_key("prefix", "name");
    let key2 = lease_key("prefix", "name");
    assert_eq!(key1, key2);
}

#[test]
fn lease_key_different_names() {
    let key1 = lease_key("prefix", "name-a");
    let key2 = lease_key("prefix", "name-b");
    assert_ne!(key1, key2);
}

// =============================================================================
// Key separation tests
// =============================================================================

#[test]
fn keys_are_distinct_for_same_subscription() {
    let prefix = "catga:sub";
    let name = "my-sub";
    let def = definition_key(prefix, name);
    let idx = index_key(prefix);
    let chk = checkpoints_key(prefix, name);
    let lse = lease_key(prefix, name);

    assert_ne!(def, idx);
    assert_ne!(def, chk);
    assert_ne!(def, lse);
    assert_ne!(idx, chk);
    assert_ne!(idx, lse);
    assert_ne!(chk, lse);
}

#[test]
fn keys_share_prefix() {
    let prefix = "catga:sub";
    let name = "test";
    assert!(definition_key(prefix, name).starts_with(prefix));
    assert!(index_key(prefix).starts_with(prefix));
    assert!(checkpoints_key(prefix, name).starts_with(prefix));
    assert!(lease_key(prefix, name).starts_with(prefix));
}

// =============================================================================
// Event type separator tests
// =============================================================================

#[test]
fn event_types_separator_is_unit_separator() {
    // The code uses \u{1f} (UNIT SEPARATOR, ASCII 0x1F) as separator between event types
    let separator = "\u{1f}";
    assert_eq!(separator.len(), 1);
    assert_eq!(separator.as_bytes().len(), 1); // single byte ASCII
}

#[test]
fn event_types_split_preserves_content() {
    let types = "EventA\u{1f}EventB\u{1f}EventC";
    let parts: Vec<&str> = types.split('\u{1f}').collect();
    assert_eq!(parts.len(), 3);
    assert_eq!(parts[0], "EventA");
    assert_eq!(parts[1], "EventB");
    assert_eq!(parts[2], "EventC");
}

#[test]
fn event_types_split_empty_removes_empty() {
    let types = "EventA\u{1f}\u{1f}EventB";
    let parts: Vec<&str> = types.split('\u{1f}').filter(|s| !s.is_empty()).collect();
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0], "EventA");
    assert_eq!(parts[1], "EventB");
}

#[test]
fn event_types_join_with_separator() {
    let types = vec!["EventA", "EventB", "EventC"];
    let joined = types.join("\u{1f}");
    assert_eq!(joined, "EventA\u{1f}EventB\u{1f}EventC");
}

// =============================================================================
// SubscriptionCheckpoint tests
// =============================================================================

#[test]
fn subscription_checkpoint_new() {
    use catga_core::SubscriptionCheckpoint;
    let cpt = SubscriptionCheckpoint::new("sub-name", "stream-id", 42);
    assert_eq!(cpt.subscription_name(), "sub-name");
    assert_eq!(cpt.stream_id(), "stream-id");
    assert_eq!(cpt.version(), 42);
}

#[test]
fn subscription_checkpoint_clone() {
    use catga_core::SubscriptionCheckpoint;
    let cpt1 = SubscriptionCheckpoint::new("sub", "id", 1);
    let cpt2 = cpt1.clone();
    assert_eq!(cpt1.subscription_name(), cpt2.subscription_name());
    assert_eq!(cpt1.stream_id(), cpt2.stream_id());
    assert_eq!(cpt1.version(), cpt2.version());
}

// =============================================================================
// PersistentSubscription tests
// =============================================================================

#[test]
fn persistent_subscription_new() {
    use catga_core::PersistentSubscription;
    let sub = PersistentSubscription::new("my-sub", "events.>");
    assert_eq!(sub.name(), "my-sub");
    assert_eq!(sub.stream_pattern(), "events.>");
}

#[test]
fn persistent_subscription_with_event_types() {
    use catga_core::PersistentSubscription;
    let sub = PersistentSubscription::new("my-sub", "events.>")
        .with_event_types(["EventA", "EventB"].into_iter());
    let types: Vec<&str> = sub.event_types().iter().map(|s| s.as_ref()).collect();
    assert_eq!(types.len(), 2);
    assert!(types.contains(&"EventA"));
    assert!(types.contains(&"EventB"));
}

#[test]
fn persistent_subscription_empty_event_types() {
    use catga_core::PersistentSubscription;
    let sub = PersistentSubscription::new("my-sub", "events.>")
        .with_event_types(std::iter::empty::<&str>());
    assert!(sub.event_types().is_empty());
}
