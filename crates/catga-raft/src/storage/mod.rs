//! Storage layer for catga-raft.
//!
//! This module provides the storage abstraction layer for Raft consensus,
//! including:
//! - [`catga_storage`] - CatgaStorage enum used by the builder/owner loop
//! - [`engine`] - EngineStorage: persistent raft::Storage via raft-engine

pub mod catga_storage;
pub mod engine;

pub use catga_storage::{CatgaStorage, SnapshotProvider};
pub use engine::EngineStorage;
