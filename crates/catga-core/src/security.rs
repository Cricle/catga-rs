//! Task-scoped identities and immutable authorization requirements.

use std::sync::Arc;

use crate::Request;

tokio::task_local! {
    static SECURITY_IDENTITY: SecurityIdentity;
}

/// An immutable authenticated identity for the current asynchronous request chain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecurityIdentity {
    subject: Arc<str>,
    roles: Arc<[Arc<str>]>,
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
        }
    }

    /// Returns the host-provided authenticated subject.
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// Returns the immutable role set assigned to this identity.
    pub fn roles(&self) -> &[Arc<str>] {
        &self.roles
    }

    /// Returns whether the identity has `role`, using the C# registry's ASCII-insensitive match.
    pub fn has_role(&self, role: &str) -> bool {
        self.roles
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(role))
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
