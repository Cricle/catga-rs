use catga_core::InboxClaim;

#[test]
fn inbox_claim_new_valid_message_id() {
    let claim = InboxClaim::new(42, 1);
    assert!(claim.is_some());
    let claim = claim.unwrap();
    assert_eq!(claim.message_id(), 42);
    assert_eq!(claim.generation(), 1);
}

#[test]
fn inbox_claim_new_zero_message_id() {
    let claim = InboxClaim::new(0, 1);
    assert!(claim.is_some());
}

#[test]
fn inbox_claim_new_zero_generation() {
    let result = InboxClaim::new(1, 0);
    assert!(result.is_none());
}

#[test]
fn inbox_claim_clone() {
    let claim = InboxClaim::new(42, 1).unwrap();
    let _cloned = claim.clone();
    assert_eq!(claim.message_id(), 42);
    assert_eq!(claim.generation(), 1);
}

#[test]
fn inbox_claim_debug() {
    let claim = InboxClaim::new(42, 1).unwrap();
    let debug_str = format!("{:?}", claim);
    assert!(debug_str.contains("42"));
    assert!(debug_str.contains("1"));
}

#[test]
fn inbox_claim_equality() {
    let claim1 = InboxClaim::new(1, 1).unwrap();
    let claim2 = InboxClaim::new(1, 1).unwrap();
    let claim3 = InboxClaim::new(1, 2).unwrap();
    let claim4 = InboxClaim::new(2, 1).unwrap();
    assert_eq!(claim1, claim2);
    assert_ne!(claim1, claim3);
    assert_ne!(claim1, claim4);
}

#[test]
fn inbox_claim_with_large_values() {
    let claim = InboxClaim::new(u64::MAX, u64::MAX - 1);
    assert!(claim.is_some());
    let claim = claim.unwrap();
    assert_eq!(claim.message_id(), u64::MAX);
    assert_eq!(claim.generation(), u64::MAX - 1);
}

#[test]
fn inbox_claim_with_zero_values() {
    let claim = InboxClaim::new(0, 0);
    assert!(claim.is_none());
}

#[test]
fn inbox_claim_with_one_generation() {
    let claim = InboxClaim::new(100, 1);
    assert!(claim.is_some());
}

#[test]
fn inbox_claim_serialization_roundtrip() {
    use catga_core::InboxClaim;
    let claim = InboxClaim::new(42, 5).unwrap();
    let debug_str = format!("{:?}", claim);
    assert!(debug_str.contains("42"));
    assert!(debug_str.contains("5"));
}
