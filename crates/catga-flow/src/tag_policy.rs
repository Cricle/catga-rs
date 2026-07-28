use std::{collections::HashMap, time::Duration};

/// Immutable timeout, retry, and persistence rules selected by Flow step tags.
///
/// ```
/// use std::time::Duration;
/// use catga_flow::FlowTagPolicy;
///
/// let policy = FlowTagPolicy::new(Duration::from_secs(30), 2)
///     .with_timeout("payment", Duration::from_secs(5))
///     .with_retries("payment", 4)
///     .with_persist("payment");
///
/// assert_eq!(policy.timeout_for("payment"), Duration::from_secs(5));
/// assert_eq!(policy.retries_for("other"), 2);
/// assert!(policy.should_persist("payment"));
/// ```
#[derive(Clone, Debug)]
pub struct FlowTagPolicy {
    default_timeout: Duration,
    default_retries: usize,
    timeouts: HashMap<Box<str>, Duration>,
    retries: HashMap<Box<str>, usize>,
    persisted: std::collections::HashSet<Box<str>>,
}

impl FlowTagPolicy {
    /// Creates a policy whose defaults apply when a tag has no override.
    pub fn new(default_timeout: Duration, default_retries: usize) -> Self {
        Self {
            default_timeout,
            default_retries,
            timeouts: HashMap::new(),
            retries: HashMap::new(),
            persisted: std::collections::HashSet::new(),
        }
    }

    /// Returns a copy with a timeout override for `tag`.
    pub fn with_timeout(mut self, tag: impl Into<Box<str>>, timeout: Duration) -> Self {
        self.timeouts.insert(tag.into(), timeout);
        self
    }

    /// Returns a copy with a retry override for `tag`.
    pub fn with_retries(mut self, tag: impl Into<Box<str>>, retries: usize) -> Self {
        self.retries.insert(tag.into(), retries);
        self
    }

    /// Marks a tag as requiring durable persistence in execution models that support optional
    /// checkpoints.
    ///
    /// [`crate::FlowRuntime`] always persists every durable transition, so this marker has no
    /// additional runtime effect there. It remains available for explicit process-local DSL
    /// checkpoint composition rather than silently weakening durable recovery.
    pub fn with_persist(mut self, tag: impl Into<Box<str>>) -> Self {
        self.persisted.insert(tag.into());
        self
    }

    /// Returns the effective timeout for `tag`.
    pub fn timeout_for(&self, tag: &str) -> Duration {
        self.timeouts
            .get(tag)
            .copied()
            .unwrap_or(self.default_timeout)
    }

    /// Returns the effective retry count for `tag`.
    pub fn retries_for(&self, tag: &str) -> usize {
        self.retries
            .get(tag)
            .copied()
            .unwrap_or(self.default_retries)
    }

    /// Returns whether a step carrying `tag` must persist its state.
    pub fn should_persist(&self, tag: &str) -> bool {
        self.persisted.contains(tag)
    }
}
