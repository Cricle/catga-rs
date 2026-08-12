//! Collision-free resource naming for tests sharing one broker.

use std::sync::atomic::{AtomicUsize, Ordering};

static TEST_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

/// Returns a process-unique resource name so concurrent test binaries never collide.
pub fn unique(prefix: &str) -> String {
    let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{}_{}", std::process::id(), sequence)
}
