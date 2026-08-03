#![forbid(unsafe_code)]
//! Lightweight test helpers for Catga applications.
//!
//! This crate provides typed, in-process fixtures for Flow and aggregate testing, along with
//! re-exports of core testing utilities from [`catga_core::testing`]. Use [`CatgaTestHarness`] to
//! register handlers before starting a mediator, [`FlowTestContext`] for isolated Flow persistence
//! dependencies, and [`AggregateScenario`] for event-sourced aggregate testing. Each helper owns its
//! own bounded in-memory state, so tests should construct a fresh helper instead of sharing one
//! across concurrently running cases.
//!
//! ```
//! use catga_testing::FlowTestContext;
//!
//! let context = FlowTestContext::new();
//! let first = context.suspended_flows();
//! let second = context.suspended_flows();
//! assert!(std::sync::Arc::ptr_eq(&first, &second));
//! ```
//!
//! These utilities intentionally model Catga contracts, not a production deployment. They do not
//! start a network listener, persist state across process boundaries, or prove scheduling and
//! transport behavior of a production adapter; cover those boundaries with the adapter's own
//! integration tests.

// Re-export all core testing utilities from catga_core::testing
pub use catga_core::testing::{
    assert_contains, assert_error_code, assert_failure, assert_success, assert_value,
    EventHandlerSpy, HandlerSpy, MessageCapture,
};

mod aggregate;
mod bus_harness;
mod flow;
mod harness;

pub use aggregate::{AggregateScenario, ReplayedAggregate};
pub use bus_harness::BusTestHarness;
pub use flow::FlowTestContext;
pub use harness::{CatgaTestHarness, RunningCatgaTestHarness};
