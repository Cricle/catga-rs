//! Standard Axum extractor for the Catga mediator.
//!
//! [`MediatorState`] implements [`FromRequestParts`] so handlers can combine it with any
//! other Axum extractor—`Path`, `Query`, `State`, `Json`—without adopting a fixed route
//! template. The mediator is stored in the router's application state and extracted per
//! request with a single `Arc` clone.

use std::{convert::Infallible, ops::Deref, sync::Arc};

use axum::{extract::FromRequestParts, http::request::Parts};
use catga_core::Mediator;

/// A lightweight extractor that provides access to the application [`Mediator`].
///
/// Store an `Arc<Mediator>` (or a [`MediatorState`] directly) in your Axum state, then
/// extract it in any handler:
///
/// ```no_run
/// # use std::sync::Arc;
/// # use axum::{Router, extract::Path, routing::get, http::StatusCode};
/// # use catga_axum::MediatorState;
/// # use catga_core::{Mediator, Registry};
/// async fn get_order(
///     mediator: MediatorState,
///     Path(id): Path<u64>,
/// ) -> StatusCode {
///     // mediator.send(GetOrder { id }).await
///     StatusCode::OK
/// }
///
/// # fn build() {
/// # let registry = Registry::new();
/// let mediator = Arc::new(Mediator::new(registry));
/// let state = MediatorState::new(mediator);
/// let app: Router<MediatorState> = Router::new()
///     .route("/orders/{id}", get(get_order));
/// let app: Router<()> = app.with_state(state);
/// # }
/// ```
///
/// The extractor is compatible with nested state via [`axum::extract::FromRef`]: if your
/// application state contains an `Arc<Mediator>` field, implement `FromRef<YourState>` for
/// `MediatorState` (or derive it) and the extractor resolves automatically.
#[derive(Clone)]
pub struct MediatorState(Arc<Mediator>);

impl MediatorState {
    /// Wraps an application-owned mediator for use as Axum state.
    pub fn new(mediator: Arc<Mediator>) -> Self {
        Self(mediator)
    }

    /// Returns the inner mediator for explicit dispatch.
    pub fn mediator(&self) -> &Arc<Mediator> {
        &self.0
    }
}

impl Deref for MediatorState {
    type Target = Mediator;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Arc<Mediator>> for MediatorState {
    fn from(mediator: Arc<Mediator>) -> Self {
        Self(mediator)
    }
}

impl From<MediatorState> for Arc<Mediator> {
    fn from(state: MediatorState) -> Self {
        state.0
    }
}

impl<S> FromRequestParts<S> for MediatorState
where
    S: Send + Sync,
    Arc<Mediator>: FromRef<S>,
{
    type Rejection = Infallible;

    async fn from_request_parts(_parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        Ok(Self(Arc::<Mediator>::from_ref(state)))
    }
}

/// Allows [`MediatorState`] to be extracted from a router whose state is `Arc<Mediator>`.
///
/// This mirrors Axum's blanket `FromRef` for identity state so that
/// `Router::with_state(mediator)` works directly.
impl axum::extract::FromRef<MediatorState> for Arc<Mediator> {
    fn from_ref(state: &MediatorState) -> Self {
        Arc::clone(&state.0)
    }
}

use axum::extract::FromRef;
