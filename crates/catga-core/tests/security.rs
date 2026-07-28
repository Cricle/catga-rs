//! Application claim validation and serialization contract tests.

use catga_core::{CatgaError, ErrorCode, MAX_SECURITY_CLAIMS, SecurityClaims, SecurityIdentity};

#[test]
fn application_claims_are_queryable_without_granting_roles() -> catga_core::CatgaResult<()> {
    let identity = SecurityIdentity::try_with_claims(
        "subject-42",
        ["reader"],
        [("tenant.id", "acme"), ("role", "administrator")],
    )?;
    assert_eq!(identity.claim("tenant.id"), Some("acme"));
    assert_eq!(identity.claim("missing"), None);
    assert!(!identity.has_role("administrator"));
    Ok(())
}

#[test]
fn application_claims_reject_invalid_or_unbounded_input() {
    let invalid_key = SecurityIdentity::try_with_claims(
        "subject-42",
        std::iter::empty::<&str>(),
        [("tenant id", "acme")],
    );
    assert_eq!(
        invalid_key.err().map(|error| error.code()),
        Some(ErrorCode::Validation)
    );

    let too_many = SecurityIdentity::try_with_claims(
        "subject-42",
        std::iter::empty::<&str>(),
        (0..=MAX_SECURITY_CLAIMS).map(|index| (format!("claim.{index}"), "value")),
    );
    assert_eq!(
        too_many.err().map(|error| error.code()),
        Some(ErrorCode::Validation)
    );
}

#[test]
fn application_claims_serialize_and_deserialize_with_validation() -> catga_core::CatgaResult<()> {
    let identity = SecurityIdentity::try_with_claims(
        "subject-42",
        std::iter::empty::<&str>(),
        [("tenant.id", "acme")],
    )?;
    let encoded = serde_json::to_string(identity.claims())
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
    let decoded: SecurityClaims = serde_json::from_str(&encoded)
        .map_err(|error| CatgaError::new(ErrorCode::Internal, error.to_string()))?;
    assert_eq!(decoded.get("tenant.id"), Some("acme"));
    Ok(())
}

#[test]
fn deserialized_claims_enforce_the_count_limit() {
    let encoded = format!(
        "[{}]",
        (0..=MAX_SECURITY_CLAIMS)
            .map(|index| format!(r#"{{"key":"claim.{index}","value":"value"}}"#))
            .collect::<Vec<_>>()
            .join(","),
    );
    assert!(serde_json::from_str::<SecurityClaims>(&encoded).is_err());
}
