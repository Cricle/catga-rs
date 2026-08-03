#![forbid(unsafe_code)]
//! Compile-time handler discovery and application facade.
//!
//! This module provides a small, typed application facade for Catga runtimes.
//!
//! `auto` owns only startup composition: it builds the existing Catga registry, binds a
//! [`MediatorHandle`], and exposes an explicit shutdown token. It does not add reflection,
//! hidden tasks, or dynamic dispatch to request hot paths. Transport, Flow, and cluster features
//! remain explicit dependencies selected by the application.
//!
//! # Handler Registration
//!
//! Plain async functions automatically satisfy handler traits thanks to Fn-blanket impls in
//! `catga-core`. No `#[async_trait]` needed for simple handlers:
//!
//! ```
//! use catga_core::auto::AutoApp;
//! use catga_core::{CatgaResult, Message, Request};
//!
//! struct Ping;
//! impl Message for Ping {}
//! impl Request for Ping {
//!     type Response = ();
//!     type TypeId = catga_core::DefaultMessageTypeId;
//! }
//!
//! // Plain async fn - no #[async_trait] needed!
//! async fn ping_handler(_: Ping) -> CatgaResult<()> { Ok(()) }
//!
//! # async fn run() -> catga_core::CatgaResult<()> {
//! let app = AutoApp::builder()
//!     .handler(ping_handler)?  // Inferred message type!
//!     .build()?;
//! # Ok(())
//! # }
//! ```
//!
//! For module-level auto-discovery, use the `#[catga_auto]` attribute:
//!
//! ```ignore
//! #[catga_auto]
//! mod handlers {
//!     async fn ping_handler(_: Ping) -> CatgaResult<String> {
//!         Ok("pong".to_string())
//!     }
//! }
//!
//! let registry = handlers::__catga_auto_register(Registry::new())?;
//! ```

use std::sync::Arc;

use crate::{
    CatgaResult, Command, CommandHandler, Event, EventHandler, Handler, Mediator, MediatorHandle,
    Registry, Request,
};
use tokio_util::sync::CancellationToken;

pub mod bus;
pub mod global_dispatch;

pub use bus::{
    Bus, BusBuilder, BusFaultPublisher, BusPublisher, BusRequestClient, DeliveryMessageOf,
    FaultPublishingHandler, FilteredHandler, MessageForwarder, PublisherHandle,
};
pub use global_dispatch::{bind_mediator, is_bound, mediator_handle, publish, send, send_command};

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

    /// Registers one typed handler and returns the builder for fluent composition.
    ///
    /// The message type is inferred from the handler's signature. Use this for requests,
    /// commands, and events. For explicit control, use `.request()`, `.command()`, or `.event()`.
    ///
    /// ```
    /// use catga_core::auto::AutoApp;
    /// use catga_core::{CatgaResult, Message, Request};
    ///
    /// struct Ping;
    /// impl Message for Ping {}
    /// impl Request for Ping {
    ///     type Response = ();
    ///     type TypeId = catga_core::DefaultMessageTypeId;
    /// }
    ///
    /// async fn ping_handler(_: Ping) -> CatgaResult<()> {
    ///     Ok(())
    /// }
    ///
    /// # async fn run() -> catga_core::CatgaResult<()> {
    /// let app = AutoApp::builder()
    ///     .handler(ping_handler)?
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn handler<M, H>(mut self, handler: H) -> CatgaResult<Self>
    where
        M: Request,
        H: Handler<M> + Send + Sync + 'static,
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

    /// Uses a pre-built registry (e.g., from `#[catga_auto]` module's `__catga_auto_register`).
    pub fn with_registry(mut self, registry: Registry) -> Self {
        self.registry = registry;
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
        // Also bind the global dispatch mediator
        global_dispatch::bind_mediator(Arc::clone(&mediator))?;
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

#[cfg(test)]
mod tests {
    use super::AutoAppBuilder;
    use async_trait::async_trait;
    use crate::{CatgaResult, Handler, Message, Registry, Request};

    #[derive(Clone)]
    struct Ping;

    impl Message for Ping {}

    impl Request for Ping {
        type Response = String;
        type TypeId = crate::DefaultMessageTypeId;
    }

    struct PingHandler;

    #[async_trait]
    impl Handler<Ping> for PingHandler {
        async fn handle(&self, _: Ping) -> CatgaResult<String> {
            Ok("pong".to_string())
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

    #[tokio::test]
    async fn handler_method_infers_message_type() -> CatgaResult<()> {
        // Plain async fn - no #[async_trait] needed!
        async fn ping_handler(_: Ping) -> CatgaResult<String> {
            Ok("pong".to_string())
        }

        let app = AutoAppBuilder::new().handler(ping_handler)?.build()?;
        assert!(app.handle().is_bound());
        Ok(())
    }

    #[test]
    fn with_registry_accepts_prebuilt_registry() {
        let registry = Registry::new();
        let builder = AutoAppBuilder::new().with_registry(registry);
        let app = builder.build().expect("build with prebuilt registry");
        assert!(app.handle().is_bound());
    }
}
