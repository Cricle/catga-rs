//! Storage layer for catga-raft.
//!
//! This module provides the storage abstraction layer for Raft consensus,
//! including:
//! - [`catga_storage`] - CatgaStorage enum used by the builder/owner loop
//! - [`engine`] - EngineStorage: persistent raft::Storage via raft-engine

pub mod engine;
pub mod catga_storage;

pub use engine::EngineStorage;
pub use catga_storage::{CatgaStorage, SnapshotProvider};
