//! Task-scoped identities and immutable authorization requirements.

use std::sync::Arc;

use std::fmt;

use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{Error as _, SeqAccess, Visitor},
};

use crate::{CatgaError, CatgaResult, ErrorCode, Request};

/// Maximum application claims retained by one [`SecurityIdentity`].
pub const MAX_SECURITY_CLAIMS: usize = 32;
/// Maximum UTF-8 byte length of one application claim key.
pub const MAX_SECURITY_CLAIM_KEY_BYTES: usize = 64;
/// Maximum UTF-8 byte length of one application claim value.
pub const MAX_SECURITY_CLAIM_VALUE_BYTES: usize = 1_024;

tokio::task_local! {
    static SECURITY_IDENTITY: SecurityIdentity;
}

/// An immutable authenticated identity for the current asynchronous request chain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecurityIdentity {
    subject: Arc<str>,
    roles: Arc<[Arc<str>]>,
    claims: SecurityClaims,
}

/// One application-defined identity claim.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SecurityClaim {
    key: Box<str>,
    value: Box<str>,
}

impl<'de> Deserialize<'de> for SecurityClaim {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct ClaimWire {
            key: Box<str>,
            value: Box<str>,
        }

        let ClaimWire { key, value } = ClaimWire::deserialize(deserializer)?;
        Self::try_new(key, value).map_err(|error| D::Error::custom(error.message()))
    }
}

impl SecurityClaim {
    /// Creates one validated application claim.
    pub fn try_new(key: impl Into<Box<str>>, value: impl Into<Box<str>>) -> CatgaResult<Self> {
        let key = key.into();
        let value = value.into();
        validate_claim_key(&key)?;
        if value.len() > MAX_SECURITY_CLAIM_VALUE_BYTES {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "security claim value exceeds the configured memory budget",
            ));
        }
        Ok(Self { key, value })
    }

    /// Returns the stable application-defined claim key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the application-defined claim value.
    pub fn value(&self) -> &str {
        &self.value
    }
}

/// Immutable, bounded application claims associated with a security identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecurityClaims(Arc<[SecurityClaim]>);

impl SecurityClaims {
    fn empty() -> Self {
        Self(Arc::from([]))
    }

    fn try_from_iter<I, K, V>(claims: I) -> CatgaResult<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<Box<str>>,
        V: Into<Box<str>>,
    {
        let mut entries = Vec::with_capacity(MAX_SECURITY_CLAIMS);
        for (key, value) in claims {
            if entries.len() == MAX_SECURITY_CLAIMS {
                return Err(CatgaError::new(
                    ErrorCode::Validation,
                    "security claim count exceeds the configured memory budget",
                ));
            }
            entries.push(SecurityClaim::try_new(key, value)?);
        }
        Self::try_from_entries(entries)
    }

    fn try_from_entries(mut entries: Vec<SecurityClaim>) -> CatgaResult<Self> {
        entries.sort_unstable_by(|left, right| left.key.cmp(&right.key));
        if entries.windows(2).any(|pair| pair[0].key == pair[1].key) {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "security claim keys must be unique",
            ));
        }
        Ok(Self(entries.into()))
    }

    /// Returns the claim value for `key`, using exact key matching.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0
            .binary_search_by(|claim| claim.key().cmp(key))
            .ok()
            .map(|index| self.0[index].value())
    }

    /// Returns every claim in deterministic key order.
    pub fn as_slice(&self) -> &[SecurityClaim] {
        &self.0
    }
}

impl Serialize for SecurityClaims {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.as_ref().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SecurityClaims {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ClaimsVisitor;

        impl<'de> Visitor<'de> for ClaimsVisitor {
            type Value = SecurityClaims;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("at most 32 validated security claims")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut claims = Vec::with_capacity(MAX_SECURITY_CLAIMS);
                while let Some(claim) = sequence.next_element::<SecurityClaim>()? {
                    if claims.len() == MAX_SECURITY_CLAIMS {
                        return Err(A::Error::custom(
                            "security claim count exceeds the configured memory budget",
                        ));
                    }
                    claims.push(claim);
                }
                SecurityClaims::try_from_entries(claims)
                    .map_err(|error| A::Error::custom(error.message()))
            }
        }

        deserializer.deserialize_seq(ClaimsVisitor)
    }
}

impl SecurityIdentity {
    /// Creates an authenticated identity with a stable subject and zero or more roles.
    pub fn new<I, R>(subject: impl Into<Arc<str>>, roles: I) -> Self
    where
        I: IntoIterator<Item = R>,
        R: Into<Arc<str>>,
    {
        Self {
            subject: subject.into(),
            roles: roles.into_iter().map(Into::into).collect(),
            claims: SecurityClaims::empty(),
        }
    }

    /// Creates an authenticated identity with bounded application claims.
    ///
    /// Claims remain data only: role and policy authorization continue to use
    /// the separately authenticated role set and registered policies.
    pub fn try_with_claims<I, R, C, K, V>(
        subject: impl Into<Arc<str>>,
        roles: I,
        claims: C,
    ) -> CatgaResult<Self>
    where
        I: IntoIterator<Item = R>,
        R: Into<Arc<str>>,
        C: IntoIterator<Item = (K, V)>,
        K: Into<Box<str>>,
        V: Into<Box<str>>,
    {
        Ok(Self {
            subject: subject.into(),
            roles: roles.into_iter().map(Into::into).collect(),
            claims: SecurityClaims::try_from_iter(claims)?,
        })
    }

    /// Returns the host-provided authenticated subject.
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// Returns the immutable role set assigned to this identity.
    pub fn roles(&self) -> &[Arc<str>] {
        &self.roles
    }

    /// Returns the immutable, bounded application claim collection.
    pub const fn claims(&self) -> &SecurityClaims {
        &self.claims
    }

    /// Returns one application claim value by its exact key.
    pub fn claim(&self, key: &str) -> Option<&str> {
        self.claims.get(key)
    }

    /// Returns whether the identity has `role`, using the C# registry's ASCII-insensitive match.
    pub fn has_role(&self, role: &str) -> bool {
        self.roles
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(role))
    }
}

fn validate_claim_key(key: &str) -> CatgaResult<()> {
    let valid = !key.is_empty()
        && key.len() <= MAX_SECURITY_CLAIM_KEY_BYTES
        && key.as_bytes().iter().enumerate().all(|(index, byte)| {
            byte.is_ascii_alphanumeric() || (matches!(*byte, b'.' | b'_' | b'-') && index != 0)
        });
    if valid {
        Ok(())
    } else {
        Err(CatgaError::new(
            ErrorCode::Validation,
            "security claim key must be a bounded ASCII identifier",
        ))
    }
}

/// Returns the identity scoped to the current asynchronous task chain.
pub fn current_security_identity() -> Option<SecurityIdentity> {
    SECURITY_IDENTITY.try_with(Clone::clone).ok()
}

/// Runs `future` with `identity` scoped to its asynchronous task chain.
pub async fn scope_security_identity<T>(
    identity: SecurityIdentity,
    future: impl Future<Output = T>,
) -> T {
    SECURITY_IDENTITY.scope(identity, future).await
}

/// Static authorization requirements declared by a request type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuthorizationRequirements {
    roles: &'static [&'static str],
    policy: Option<&'static str>,
}

impl AuthorizationRequirements {
    /// Requires an authenticated identity without restricting roles or policies.
    pub const fn authenticated() -> Self {
        Self {
            roles: &[],
            policy: None,
        }
    }

    /// Requires an authenticated identity with any one of `roles`.
    pub const fn with_roles(roles: &'static [&'static str]) -> Self {
        Self {
            roles,
            policy: None,
        }
    }

    /// Requires an authenticated identity and a named policy when that policy is registered.
    pub const fn with_policy(policy: &'static str) -> Self {
        Self {
            roles: &[],
            policy: Some(policy),
        }
    }

    /// Requires an authenticated identity, any one role, and a named registered policy.
    pub const fn with_roles_and_policy(
        roles: &'static [&'static str],
        policy: &'static str,
    ) -> Self {
        Self {
            roles,
            policy: Some(policy),
        }
    }

    /// Returns the required roles, where any matching role grants access.
    pub const fn roles(self) -> &'static [&'static str] {
        self.roles
    }

    /// Returns the optional named policy.
    pub const fn policy(self) -> Option<&'static str> {
        self.policy
    }
}

/// Declares the authorization requirement for a typed request.
///
/// Request types without this trait stay public because they cannot be paired
/// with [`crate::AuthorizationBehavior`].
pub trait AuthorizedRequest: Request {
    /// Returns the static authorization contract for this request type.
    fn authorization() -> AuthorizationRequirements;
}

#[cfg(test)]
mod tests {
    use crate::{CatgaError, ErrorCode, MAX_SECURITY_CLAIMS, SecurityClaims, SecurityIdentity};

    #[test]
    fn application_claims_are_queryable_without_granting_roles() -> crate::CatgaResult<()> {
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
    fn application_claims_serialize_and_deserialize_with_validation() -> crate::CatgaResult<()> {
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
                .map(|index| format!(r#"{{\"key\":\"claim.{index}\",\"value\":\"value\"}}"#))
                .collect::<Vec<_>>()
                .join(","),
        );

        assert!(serde_json::from_str::<SecurityClaims>(&encoded).is_err());
    }
}
