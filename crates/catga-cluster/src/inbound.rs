//! Transport-neutral Raft ingress authorization.

use std::{collections::HashMap, error::Error, fmt, sync::Arc};

use crate::RaftMessage;

/// A stable identity that a transport adapter obtained from an authenticated peer.
///
/// The identity is intentionally opaque to the cluster core. An HTTPS adapter can derive it
/// from an mTLS SAN or SPIFFE URI, while another transport can use a verified signing-key ID.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RaftPeerIdentity(Arc<str>);

impl RaftPeerIdentity {
    /// Validates and creates a non-empty authenticated peer identity.
    pub fn new(value: impl AsRef<str>) -> Result<Self, RaftInboundPolicyError> {
        let value = value.as_ref().trim();
        if value.is_empty() {
            return Err(RaftInboundPolicyError::EmptyIdentity);
        }
        Ok(Self(Arc::from(value)))
    }

    /// Returns the stable identity value supplied by the authenticated transport boundary.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for RaftPeerIdentity {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// Reason why a Raft ingress policy rejected a received protocol frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RaftInboundRejection {
    /// No authenticated peer identity reached the route.
    Unauthenticated,
    /// The authenticated identity, sender, or destination was not trusted.
    Forbidden,
}

/// Authorizes a decoded Raft frame before it can reach a [`crate::RaftNode`] inbox.
///
/// Implementations must treat `peer` as authenticated transport metadata, not a value supplied
/// by the Raft frame. This separation lets applications choose mTLS, signed frames, or another
/// authentication mechanism without coupling the cluster core to a web framework or TLS stack.
pub trait RaftInboundPolicy: Send + Sync {
    /// Returns whether `peer` may submit `message` to the local node.
    fn authorize(
        &self,
        peer: Option<&RaftPeerIdentity>,
        message: &RaftMessage,
    ) -> Result<(), RaftInboundRejection>;
}

impl<T> RaftInboundPolicy for Arc<T>
where
    T: RaftInboundPolicy + ?Sized,
{
    fn authorize(
        &self,
        peer: Option<&RaftPeerIdentity>,
        message: &RaftMessage,
    ) -> Result<(), RaftInboundRejection> {
        (**self).authorize(peer, message)
    }
}

impl<F> RaftInboundPolicy for F
where
    F: Fn(Option<&RaftPeerIdentity>, &RaftMessage) -> Result<(), RaftInboundRejection>
        + Send
        + Sync,
{
    fn authorize(
        &self,
        peer: Option<&RaftPeerIdentity>,
        message: &RaftMessage,
    ) -> Result<(), RaftInboundRejection> {
        self(peer, message)
    }
}

/// Validation failure while constructing a static member-bound ingress policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RaftInboundPolicyError {
    /// Raft node ID zero is reserved and cannot be used as a local or peer ID.
    ZeroNodeId,
    /// The same peer ID appeared more than once in the identity map.
    DuplicatePeerId,
    /// An authenticated peer identity was empty after trimming whitespace.
    EmptyIdentity,
}

impl fmt::Display for RaftInboundPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroNodeId => formatter.write_str("Raft node IDs must be non-zero"),
            Self::DuplicatePeerId => {
                formatter.write_str("a Raft peer identity was configured more than once")
            }
            Self::EmptyIdentity => formatter.write_str("a Raft peer identity must not be empty"),
        }
    }
}

impl Error for RaftInboundPolicyError {}

/// A fixed Raft member map that binds every remote node ID to one authenticated identity.
///
/// This is the production default for clusters with static membership. It rejects unauthenticated
/// frames, frames sent to another node, self-originated frames, unknown members, and identities
/// that do not match the claimed `from` node ID.
#[derive(Clone, Debug)]
pub struct StaticRaftInboundPolicy {
    local_id: u64,
    identities: HashMap<u64, RaftPeerIdentity>,
}

impl StaticRaftInboundPolicy {
    /// Creates a static policy for `local_id` and its authenticated remote peers.
    pub fn new<I, S>(local_id: u64, peers: I) -> Result<Self, RaftInboundPolicyError>
    where
        I: IntoIterator<Item = (u64, S)>,
        S: AsRef<str>,
    {
        if local_id == 0 {
            return Err(RaftInboundPolicyError::ZeroNodeId);
        }
        let mut identities = HashMap::new();
        for (id, identity) in peers {
            if id == 0 {
                return Err(RaftInboundPolicyError::ZeroNodeId);
            }
            let identity = RaftPeerIdentity::new(identity)?;
            if identities.insert(id, identity).is_some() {
                return Err(RaftInboundPolicyError::DuplicatePeerId);
            }
        }
        Ok(Self {
            local_id,
            identities,
        })
    }
}

impl RaftInboundPolicy for StaticRaftInboundPolicy {
    fn authorize(
        &self,
        peer: Option<&RaftPeerIdentity>,
        message: &RaftMessage,
    ) -> Result<(), RaftInboundRejection> {
        let Some(peer) = peer else {
            return Err(RaftInboundRejection::Unauthenticated);
        };
        if message.to != self.local_id || message.from == self.local_id || message.from == 0 {
            return Err(RaftInboundRejection::Forbidden);
        }
        match self.identities.get(&message.from) {
            Some(expected) if expected == peer => Ok(()),
            Some(_) | None => Err(RaftInboundRejection::Forbidden),
        }
    }
}
