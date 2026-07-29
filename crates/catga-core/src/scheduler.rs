//! Runtime-neutral contracts for application-owned scheduled tasks.
//!
//! This module deliberately models registration and cancellation only. Scheduler lifecycle is
//! owned by the application or by a separate lifecycle contract, so Core never starts a polling
//! loop, creates a background worker, or selects a cron implementation.

use std::sync::Arc;

use async_trait::async_trait;

use crate::{CatgaError, CatgaResult, ErrorCode};

/// Maximum UTF-8 byte length accepted for a cron expression.
///
/// The limit bounds configuration retained by scheduler implementations. Cron grammar validation
/// remains adapter-owned because Core intentionally has no dependency on a specific parser.
pub const MAX_CRON_SCHEDULE_BYTES: usize = 512;

/// Maximum UTF-8 byte length accepted for an adapter-issued scheduled-task identifier.
pub const MAX_SCHEDULED_TASK_ID_BYTES: usize = 256;

/// A runtime-neutral task schedule.
///
/// Catga currently standardizes cron expressions because the provided scheduler adapter is based
/// on `tokio-cron-scheduler`. The expression is retained unchanged; its grammar and timezone
/// semantics are defined by the selected [`TaskScheduler`] implementation.
///
/// ```
/// use catga_core::TaskSchedule;
///
/// let schedule = TaskSchedule::cron("0 */5 * * * *").expect("valid cron");
/// assert_eq!(schedule.as_cron(), "0 */5 * * * *");
/// assert!(TaskSchedule::cron("").is_err());
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskSchedule {
    cron: Box<str>,
}

impl TaskSchedule {
    /// Creates a bounded, nonempty cron schedule expression.
    ///
    /// This validates only invariants shared by every adapter. It does not parse cron syntax, so
    /// invalid syntax is reported by [`TaskScheduler::schedule`] using the selected adapter.
    pub fn cron(expression: impl Into<Box<str>>) -> CatgaResult<Self> {
        let cron = expression.into();
        if cron.trim().is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "cron schedule expression must not be empty",
            ));
        }
        if cron.len() > MAX_CRON_SCHEDULE_BYTES {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                format!("cron schedule expression exceeds {MAX_CRON_SCHEDULE_BYTES} UTF-8 bytes"),
            ));
        }
        Ok(Self { cron })
    }

    /// Returns the original cron expression for adapter-specific parsing and registration.
    pub fn as_cron(&self) -> &str {
        &self.cron
    }
}

/// An opaque identifier assigned by a [`TaskScheduler`] when a task is registered.
///
/// Identifiers are adapter-owned. A caller should retain the returned value only to pass it back
/// to [`TaskScheduler::cancel`], rather than deriving meaning from its textual representation.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ScheduledTaskId(Box<str>);

impl ScheduledTaskId {
    /// Creates a bounded, nonempty adapter-issued identifier.
    pub fn new(value: impl Into<Box<str>>) -> CatgaResult<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "scheduled task identifier must not be empty",
            ));
        }
        if value.len() > MAX_SCHEDULED_TASK_ID_BYTES {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                format!(
                    "scheduled task identifier exceeds {MAX_SCHEDULED_TASK_ID_BYTES} UTF-8 bytes"
                ),
            ));
        }
        Ok(Self(value))
    }

    /// Returns the adapter-owned identifier text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Work invoked once by a [`TaskScheduler`] after its selected schedule becomes due.
///
/// Implementations should return ordinary Catga errors rather than panicking. The scheduler owns
/// error observation and retry policy; tasks must not assume the runtime will retry a failure.
#[async_trait]
pub trait ScheduledTask: Send + Sync {
    /// Executes one scheduled invocation.
    async fn execute(&self) -> CatgaResult<()>;
}

/// Registers and cancels application-owned scheduled tasks.
///
/// This abstraction contains no lifecycle methods: a scheduler adapter may expose a separate
/// explicit `start`/`shutdown` API, preserving Catga's rule that applications own background
/// work. Implementations parse [`TaskSchedule`], invoke each [`ScheduledTask`] once per due
/// occurrence, and map unsupported schedule forms to [`ErrorCode::Unsupported`].
#[async_trait]
pub trait TaskScheduler: Send + Sync {
    /// Registers `task` for `schedule` and returns the adapter-issued cancellation identity.
    async fn schedule(
        &self,
        schedule: TaskSchedule,
        task: Arc<dyn ScheduledTask>,
    ) -> CatgaResult<ScheduledTaskId>;

    /// Cancels a task previously returned by [`Self::schedule`].
    ///
    /// Implementations return [`ErrorCode::NotFound`] when the identifier is no longer known.
    async fn cancel(&self, task_id: &ScheduledTaskId) -> CatgaResult<()>;
}
