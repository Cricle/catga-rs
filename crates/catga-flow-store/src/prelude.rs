//! Catga flow store unified prelude.
//!
//! This module provides one import for common flow store types.
//! Features must be enabled to use specific backends.
//!
//! # Example (SQLite)
//!
//! ```
//! #[cfg(feature = "sqlite")] {
//! use catga_flow_store::prelude::*;
//! }
//! ```

#[cfg(feature = "sqlite")]
pub use crate::SqlFlowStore;

#[cfg(feature = "sqlite")]
pub use crate::SqlFlowScheduler;

#[cfg(feature = "postgres")]
pub use crate::PgFlowStore;

#[cfg(feature = "postgres")]
pub use crate::PgFlowScheduler;

#[cfg(feature = "mysql")]
pub use crate::MySqlFlowStore;

#[cfg(feature = "mysql")]
pub use crate::MySqlFlowScheduler;

#[cfg(feature = "redis")]
pub use crate::RedisFlowStore;

#[cfg(feature = "redis")]
pub use crate::RedisFlowScheduler;
