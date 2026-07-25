use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;

use crate::{
    AuthorizationRequirements, AuthorizedRequest, Behavior, CatgaError, CatgaResult, ErrorCode,
    Next, SecurityIdentity,
};

/// Evaluates a named authorization policy for one typed request.
#[async_trait]
pub trait AuthorizationPolicy<M>: Send + Sync
where
    M: AuthorizedRequest,
{
    /// Returns the policy name matched without ASCII case sensitivity.
    fn name(&self) -> &str;

    /// Returns whether `identity` may perform `request`.
    async fn authorize(&self, identity: &SecurityIdentity, request: &M) -> CatgaResult<bool>;
}

/// An immutable startup-built collection of policies for one request type.
pub struct AuthorizationPolicies<M>
where
    M: AuthorizedRequest,
{
    policies: Box<[Arc<dyn AuthorizationPolicy<M>>]>,
}

impl<M> AuthorizationPolicies<M>
where
    M: AuthorizedRequest,
{
    /// Creates a policy collection from same-typed startup registrations.
    pub fn new<P>(policies: impl IntoIterator<Item = Arc<P>>) -> Self
    where
        P: AuthorizationPolicy<M> + 'static,
    {
        Self {
            policies: policies
                .into_iter()
                .map(|policy| policy as Arc<dyn AuthorizationPolicy<M>>)
                .collect(),
        }
    }

    /// Creates a policy collection from shared registrations of different concrete types.
    pub fn from_shared(
        policies: impl IntoIterator<Item = Arc<dyn AuthorizationPolicy<M>>>,
    ) -> Self {
        Self {
            policies: policies.into_iter().collect(),
        }
    }

    fn find(&self, name: &str) -> Option<&Arc<dyn AuthorizationPolicy<M>>> {
        self.policies
            .iter()
            .find(|policy| policy.name().eq_ignore_ascii_case(name))
    }
}

impl<M> Default for AuthorizationPolicies<M>
where
    M: AuthorizedRequest,
{
    fn default() -> Self {
        Self {
            policies: Box::new([]),
        }
    }
}

/// Enforces the static authorization requirement declared by a request type.
pub struct AuthorizationBehavior<M>
where
    M: AuthorizedRequest,
{
    policies: AuthorizationPolicies<M>,
    marker: PhantomData<fn(M)>,
}

impl<M> AuthorizationBehavior<M>
where
    M: AuthorizedRequest,
{
    /// Creates a behavior that enforces authentication and roles without named policies.
    pub fn new() -> Self {
        Self::with_policies(AuthorizationPolicies::default())
    }

    /// Creates a behavior with immutable named policies assembled during startup.
    pub fn with_policies(policies: AuthorizationPolicies<M>) -> Self {
        Self {
            policies,
            marker: PhantomData,
        }
    }
}

impl<M> Default for AuthorizationBehavior<M>
where
    M: AuthorizedRequest,
{
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<M> Behavior<M> for AuthorizationBehavior<M>
where
    M: AuthorizedRequest,
{
    async fn handle(&self, message: M, next: Next<M>) -> CatgaResult<M::Response> {
        let requirements = M::authorization();
        let Some(identity) = crate::current_security_identity() else {
            return Err(CatgaError::new(
                ErrorCode::Unauthorized,
                "authentication required",
            ));
        };
        if !has_required_role(&identity, requirements) {
            return Err(CatgaError::new(
                ErrorCode::Forbidden,
                required_roles_message(requirements),
            ));
        }
        if let Some(policy_name) = requirements.policy()
            && let Some(policy) = self.policies.find(policy_name)
            && !policy.authorize(&identity, &message).await?
        {
            return Err(CatgaError::new(
                ErrorCode::Forbidden,
                format!("policy '{policy_name}' denied"),
            ));
        }
        next.run(message).await
    }
}

fn has_required_role(identity: &SecurityIdentity, requirements: AuthorizationRequirements) -> bool {
    requirements.roles().is_empty()
        || requirements
            .roles()
            .iter()
            .any(|role| identity.has_role(role))
}

fn required_roles_message(requirements: AuthorizationRequirements) -> String {
    let roles = requirements.roles();
    let mut message = String::from("required role: ");
    for (index, role) in roles.iter().enumerate() {
        if index != 0 {
            message.push_str(", ");
        }
        message.push_str(role);
    }
    message
}
