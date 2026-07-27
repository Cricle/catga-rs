//! Allocation-free header lookup over a compact, startup-configured route table.

use crate::{CatgaError, CatgaResult, Destination, EnvelopeHeaders, ErrorCode};

/// Selects a named destination from a borrowed set of transport headers.
///
/// Rules are configured during startup and retain insertion order.  Resolution does not allocate,
/// lock, or clone: it returns a borrow of the configured [`Destination`].  This preserves the
/// upstream router's first-match behavior without placing an unbounded header map in every
/// [`crate::Envelope`].
#[derive(Clone, Debug, Default)]
pub struct MessageRouter {
    default_destination: Option<Destination>,
    routes: Vec<HeaderRoute>,
}

/// Routes a stable message type name to one durable transport destination.
///
/// Configure this table during startup, then share it immutably with typed publishers and
/// consumers. Resolution performs no allocation or locking and returns a borrow of the stored
/// destination. The expected number of application message types is small, making an ordered
/// vector more compact and predictable than a per-message hash lookup.
#[derive(Clone, Debug, Default)]
pub struct MessageDestinationRouter {
    routes: Vec<MessageTypeRoute>,
}

#[derive(Clone, Debug)]
struct MessageTypeRoute {
    message_type: Box<str>,
    destination: Destination,
}

impl MessageDestinationRouter {
    /// Creates an empty message-type destination router.
    #[must_use]
    pub const fn new() -> Self {
        Self { routes: Vec::new() }
    }

    /// Adds an exact route for one stable message type name.
    ///
    /// Empty names and duplicate registrations return [`ErrorCode::Validation`], preventing an
    /// ambiguous publish path at runtime.
    pub fn add_route(
        &mut self,
        message_type: impl Into<Box<str>>,
        destination: Destination,
    ) -> CatgaResult<()> {
        let message_type = message_type.into();
        if message_type.trim().is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "message type route must not be empty or whitespace-only",
            ));
        }
        if self
            .routes
            .iter()
            .any(|route| route.message_type.as_ref() == message_type.as_ref())
        {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "message type route is already configured",
            ));
        }
        self.routes.push(MessageTypeRoute {
            message_type,
            destination,
        });
        Ok(())
    }

    /// Resolves `message_type` to its configured durable destination.
    #[must_use]
    pub fn resolve(&self, message_type: &str) -> Option<&Destination> {
        self.routes
            .iter()
            .find(|route| route.message_type.as_ref() == message_type)
            .map(|route| &route.destination)
    }

    /// Returns the number of configured message type routes.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.routes.len()
    }

    /// Returns whether no message type routes are configured.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }
}

/// One owned header-match rule in a [`MessageRouter`].
#[derive(Clone, Debug)]
struct HeaderRoute {
    key: Box<str>,
    value: Box<str>,
    destination: Destination,
}

impl MessageRouter {
    /// Creates an empty router with an optional destination returned when no rule matches.
    pub const fn new(default_destination: Option<Destination>) -> Self {
        Self {
            default_destination,
            routes: Vec::new(),
        }
    }

    /// Adds one ordered header-equality route.
    ///
    /// Empty or whitespace-only keys and values return [`ErrorCode::Validation`].  A duplicate
    /// rule remains valid: the route added first wins during [`Self::resolve`], matching the
    /// upstream router's ordering semantics.
    pub fn add_route(
        &mut self,
        key: impl Into<Box<str>>,
        value: impl Into<Box<str>>,
        destination: Destination,
    ) -> CatgaResult<()> {
        let key = key.into();
        let value = value.into();
        if key.trim().is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "message route header key must not be empty or whitespace-only",
            ));
        }
        if value.trim().is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "message route header value must not be empty or whitespace-only",
            ));
        }
        self.routes.push(HeaderRoute {
            key,
            value,
            destination,
        });
        Ok(())
    }

    /// Resolves `headers` to the first matching route or the configured fallback destination.
    ///
    /// Header order is irrelevant.  The expected number of configured rules and headers is
    /// small, so this uses an allocation-free linear scan rather than a per-message hash table.
    pub fn resolve<'a>(&'a self, headers: &[(&str, &str)]) -> Option<&'a Destination> {
        self.routes
            .iter()
            .find(|route| {
                headers.iter().any(|(key, value)| {
                    *key == route.key.as_ref() && *value == route.value.as_ref()
                })
            })
            .map(|route| &route.destination)
            .or(self.default_destination.as_ref())
    }

    /// Resolves immutable envelope headers without allocating a temporary header map or slice.
    ///
    /// This is the transport-context counterpart to [`Self::resolve`]. Header
    /// matching preserves rule insertion order and returns the configured
    /// fallback when no configured header pair matches.
    pub fn resolve_envelope_headers<'a>(
        &'a self,
        headers: &EnvelopeHeaders,
    ) -> Option<&'a Destination> {
        self.routes
            .iter()
            .find(|route| {
                headers
                    .iter()
                    .any(|(key, value)| key == route.key.as_ref() && value == route.value.as_ref())
            })
            .map(|route| &route.destination)
            .or(self.default_destination.as_ref())
    }

    /// Returns the number of configured ordered rules without allocating.
    pub const fn len(&self) -> usize {
        self.routes.len()
    }

    /// Returns whether no ordered rules have been configured.
    pub const fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }
}
