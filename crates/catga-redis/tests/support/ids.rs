//! Shared unique-prefix fixture for Redis service-backed contract tests.

/// Returns a unique Redis key prefix for one test invocation.
pub fn unique_prefix(label: &str) -> String {
    format!("catga-test-{label}-{}", uuid::Uuid::new_v4())
}
