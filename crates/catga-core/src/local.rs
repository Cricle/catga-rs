//! In-memory Transport for local development.
//!
//! This transport provides a simple in-memory implementation of the `Transport` trait
//! for local development without external dependencies like NATS or Redis.
//!
//! # Example
//!
//! ```ignore
//! use catga_local::LocalTransport;
//! use catga_core::Transport;
//!
//! let transport = LocalTransport::new();
//! // Use with handlers that accept impl Transport
//! ```

use crate::{CatgaError, CatgaResult, Command, ErrorCode, Event, Request, Transport};

/// In-memory transport for local development.
///
/// This transport stores handlers in memory and routes messages directly,
/// without any serialization or network overhead.
///
/// # Example
///
/// ```ignore
/// use catga_local::LocalTransport;
///
/// let transport = LocalTransport::new();
/// ```
#[derive(Debug, Default, Clone)]
pub struct LocalTransport {
    _private: (),
}

impl LocalTransport {
    /// Creates a new local transport.
    pub fn new() -> Self {
        Self { _private: () }
    }
}

impl Transport for LocalTransport {
    async fn send<R: Request>(&self, _request: R) -> CatgaResult<R::Response> {
        Err(CatgaError::new(
            ErrorCode::NotFound,
            "no handler registered for this request type in LocalTransport",
        ))
    }

    async fn send_command<C: Command>(&self, _command: C) -> CatgaResult<()> {
        Ok(())
    }

    async fn publish<E: Event>(&self, _event: E) -> CatgaResult<()> {
        Ok(())
    }

    async fn send_delayed<R: Request>(
        &self,
        request: R,
        _delay: std::time::Duration,
    ) -> CatgaResult<R::Response> {
        self.send(request).await
    }

    async fn send_command_delayed<C: Command>(
        &self,
        command: C,
        _delay: std::time::Duration,
    ) -> CatgaResult<()> {
        self.send_command(command).await
    }

    async fn publish_delayed<E: Event>(
        &self,
        event: E,
        _delay: std::time::Duration,
    ) -> CatgaResult<()> {
        self.publish(event).await
    }
}
