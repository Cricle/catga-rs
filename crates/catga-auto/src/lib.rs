#![forbid(unsafe_code)]
#![deny(missing_docs)]
//! A small, typed application facade for Catga runtimes.
//!
//! `catga-auto` owns only startup composition: it builds the existing Catga registry, binds a
//! [`MediatorHandle`], and exposes an explicit shutdown token. It does not add reflection,
//! hidden tasks, or dynamic dispatch to request hot paths. Transport, Flow, and cluster features
//! remain explicit dependencies selected by the application.

use std::sync::Arc;

use catga_core::{
    CatgaResult, Command, CommandHandler, Event, EventHandler, Handler, Mediator, MediatorHandle,
    Registry, Request,
};
use tokio_util::sync::CancellationToken;

mod bus;

pub use bus::{
    Bus, BusBuilder, BusFaultPublisher, BusPublisher, BusRequestClient, FaultPublishingHandler,
    FilteredHandler, MessageForwarder, PublisherHandle,
};

/// Re-exports the state-machine Bus adapter when the `flow` feature is enabled.
#[cfg(feature = "flow")]
pub use bus::StateMachineHandler;

/// Re-exports Catga's bounded typed competing-consumer runner.
pub use catga_core::{CompetingConsumer, TypedDeliveryHandler};

/// A startup builder for one immutable Catga application graph.
pub struct AutoAppBuilder {
    registry: Registry,
    shutdown: CancellationToken,
    handle: MediatorHandle,
}

impl Default for AutoAppBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl AutoAppBuilder {
    /// Creates an empty application builder with a fresh shutdown token.
    pub fn new() -> Self {
        Self {
            registry: Registry::new(),
            shutdown: CancellationToken::new(),
            handle: MediatorHandle::new(),
        }
    }

    /// Uses an application-owned shutdown token.
    pub fn with_shutdown_token(mut self, shutdown: CancellationToken) -> Self {
        self.shutdown = shutdown;
        self
    }

    /// Registers one typed request handler and returns the consumed builder for fluent composition.
    ///
    /// Registration can fail when another handler for `M` was already registered.
    pub fn request<M, H>(mut self, handler: H) -> CatgaResult<Self>
    where
        M: Request,
        H: Handler<M> + 'static,
    {
        self.registry.register_request(handler)?;
        Ok(self)
    }

    /// Registers one typed command handler and returns the consumed builder for fluent composition.
    ///
    /// Registration can fail when another handler for `C` was already registered.
    pub fn command<C, H>(mut self, handler: H) -> CatgaResult<Self>
    where
        C: Command,
        H: CommandHandler<C> + 'static,
    {
        self.registry.register_command(handler)?;
        Ok(self)
    }

    /// Registers an additional typed event handler and returns the consumed builder for fluent composition.
    pub fn event<E, H>(mut self, handler: H) -> Self
    where
        E: Event,
        H: EventHandler<E> + 'static,
    {
        self.registry.register_event(handler);
        self
    }

    /// Registers one typed request handler.
    pub fn register_request<M, H>(&mut self, handler: H) -> CatgaResult<&mut Self>
    where
        M: Request,
        H: Handler<M> + 'static,
    {
        self.registry.register_request::<M, H>(handler)?;
        Ok(self)
    }

    /// Registers one typed command handler.
    pub fn register_command<C, H>(&mut self, handler: H) -> CatgaResult<&mut Self>
    where
        C: Command,
        H: CommandHandler<C> + 'static,
    {
        self.registry.register_command::<C, H>(handler)?;
        Ok(self)
    }

    /// Registers an additional typed event handler.
    pub fn register_event<E, H>(&mut self, handler: H) -> &mut Self
    where
        E: Event,
        H: EventHandler<E> + 'static,
    {
        self.registry.register_event::<E, H>(handler);
        self
    }

    /// Builds the immutable application graph.
    pub fn build(self) -> CatgaResult<AutoApp> {
        let mediator = Arc::new(Mediator::new(self.registry));
        self.handle.bind(Arc::clone(&mediator))?;
        Ok(AutoApp {
            mediator,
            handle: self.handle,
            shutdown: self.shutdown,
        })
    }
}

/// The immutable, explicitly owned Catga application facade.
pub struct AutoApp {
    mediator: Arc<Mediator>,
    handle: MediatorHandle,
    shutdown: CancellationToken,
}

impl AutoApp {
    /// Starts a new application builder.
    pub fn builder() -> AutoAppBuilder {
        AutoAppBuilder::new()
    }

    /// Returns the immutable typed mediator.
    pub fn mediator(&self) -> &Mediator {
        self.mediator.as_ref()
    }

    /// Clones the application-owned mediator for framework integration.
    ///
    /// This preserves the same immutable application graph and does not add a dispatch layer.
    pub fn mediator_arc(&self) -> Arc<Mediator> {
        Arc::clone(&self.mediator)
    }

    /// Returns the startup-bound mediator handle for sharing with application services.
    pub fn handle(&self) -> &MediatorHandle {
        &self.handle
    }

    /// Returns a clone of the explicit application shutdown token.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Signals application shutdown without spawning or aborting any task.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// Waits until the application-owned shutdown token is cancelled.
    pub async fn run_until_cancelled(&self) {
        self.shutdown.cancelled().await;
    }
}

/// Minimal web adapters enabled by the `axum` feature.
#[cfg(feature = "axum")]
pub mod web {
    use ::axum::{Router, http::StatusCode, routing::get};
    use catga_axum::MediatorState;

    use super::AutoApp;

    /// Creates Axum mediator state from an application-owned [`AutoApp`].
    ///
    /// The state shares the application's existing mediator and starts no task.
    pub fn mediator_state(app: &AutoApp) -> MediatorState {
        MediatorState::from(app.mediator_arc())
    }

    /// Adds a bounded, dependency-light health endpoint at `path`.
    pub fn health_route(path: &'static str) -> Router {
        Router::new().route(path, get(|| async { StatusCode::NO_CONTENT }))
    }
}

/// Re-exports the optional Axum adapter when the `axum` feature is enabled.
#[cfg(feature = "axum")]
pub use catga_axum as axum_adapter;

/// Re-exports the optional cluster runtime when the `cluster` feature is enabled.
#[cfg(feature = "cluster")]
pub use catga_cluster as cluster;

/// Re-exports the optional Flow runtime when the `flow` feature is enabled.
#[cfg(feature = "flow")]
pub use catga_flow as flow;

/// Re-exports the optional NATS adapter when the `nats` feature is enabled.
#[cfg(feature = "nats")]
pub use catga_nats as nats;

/// Re-exports the optional Redis adapter when the `redis` feature is enabled.
#[cfg(feature = "redis")]
pub use catga_redis as redis;

#[cfg(test)]
mod tests {
    use super::AutoAppBuilder;
    use async_trait::async_trait;
    use catga_core::{CatgaResult, Handler, Message, Request};

    #[derive(Clone)]
    struct Ping;

    impl Message for Ping {}

    impl Request for Ping {
        type Response = ();
    }

    struct PingHandler;

    #[async_trait]
    impl Handler<Ping> for PingHandler {
        async fn handle(&self, _: Ping) -> CatgaResult<()> {
            Ok(())
        }
    }

    #[test]
    fn build_consumes_the_builder_and_binds_its_handle() {
        let mut builder = AutoAppBuilder::new();
        builder
            .register_request::<Ping, _>(PingHandler)
            .expect("register handler");
        let handle = builder.handle.clone();

        let app = builder.build().expect("build application");
        assert!(handle.is_bound());
        assert!(app.handle().is_bound());
    }
}
