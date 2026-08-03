//! Test helpers for Catga applications.
//!
//! This module provides typed, in-process fixtures rather than a service container: use
//! [`HandlerSpy`] and [`EventHandlerSpy`] to retain assertions, and [`MessageCapture`] for
//! concurrent message capture testing.
//!
//! These utilities intentionally model Catga contracts, not a production deployment. They do not
//! start a network listener, persist state across process boundaries, or prove scheduling and
//! transport behavior of a production adapter; cover those boundaries with the adapter's own
//! integration tests.

pub mod assertions;
pub mod capture;
pub mod spies;

pub use spies::{EventHandlerSpy, HandlerSpy};
pub use capture::MessageCapture;
pub use assertions::*;
