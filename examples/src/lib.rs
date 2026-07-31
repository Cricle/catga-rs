#![forbid(unsafe_code)]
//! Complete, runnable Catga application examples.
//!
//! [`order_service`] composes typed Axum endpoints, CQRS handlers, compensating flow steps,
//! event persistence, an acknowledged outbox delivery, and a cluster-leadership boundary in one
//! small application. It defaults to in-memory adapters so it can be run locally without Docker.

/// A modular checkout application showing Catga's core composition boundaries.
pub mod order_service;

/// Shared domain types for the runnable API and worker Todo example.
pub mod distributed_todo;
