use std::{sync::Arc, time::SystemTime};

use catga_core::CatgaError;
use serde::{Deserialize, Serialize};

/// The lifecycle phase of a durable flow.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum FlowStatus {
    /// The forward action is eligible to run.
    Running,
    /// Completed actions are being undone.
    Compensating,
    /// The flow is persisted until an external trigger resumes it.
    Suspended,
    /// The flow completed successfully.
    Done,
    /// The flow completed with an error.
    Failed,
    /// The flow was stopped before it reached another terminal outcome.
    Cancelled,
}

impl FlowStatus {
    /// Returns whether this phase is terminal.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Done | Self::Failed | Self::Cancelled)
    }
}

/// Immutable, versioned state for one durable flow execution.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FlowState {
    id: Box<str>,
    flow_type: Box<str>,
    status: FlowStatus,
    step: u32,
    version: i64,
    owner: Option<Box<str>>,
    heartbeat: SystemTime,
    data: Arc<[u8]>,
    error: Option<CatgaError>,
}

impl FlowState {
    /// Creates a running flow owned by `owner` at version zero.
    pub fn new(
        id: impl Into<Box<str>>,
        flow_type: impl Into<Box<str>>,
        data: impl Into<Arc<[u8]>>,
        owner: impl Into<Box<str>>,
    ) -> Self {
        Self {
            id: id.into(),
            flow_type: flow_type.into(),
            status: FlowStatus::Running,
            step: 0,
            version: 0,
            owner: Some(owner.into()),
            heartbeat: SystemTime::now(),
            data: data.into(),
            error: None,
        }
    }

    /// Returns the stable flow identity.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the stable flow type used to select handlers.
    pub fn flow_type(&self) -> &str {
        &self.flow_type
    }

    /// Returns the current lifecycle phase.
    pub const fn status(&self) -> FlowStatus {
        self.status
    }

    /// Returns the completed step count.
    pub const fn step(&self) -> u32 {
        self.step
    }

    /// Returns the optimistic-concurrency version.
    pub const fn version(&self) -> i64 {
        self.version
    }

    /// Returns the process currently responsible for the flow, when claimed.
    pub fn owner(&self) -> Option<&str> {
        self.owner.as_deref()
    }

    /// Returns when the current owner last recorded liveness.
    pub const fn heartbeat(&self) -> SystemTime {
        self.heartbeat
    }

    /// Returns immutable flow input without copying it.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Returns shared ownership of immutable flow input.
    pub fn shared_data(&self) -> Arc<[u8]> {
        Arc::clone(&self.data)
    }

    /// Returns the terminal failure, when the flow has failed.
    pub fn error(&self) -> Option<&CatgaError> {
        self.error.as_ref()
    }

    /// Returns a new state owned by `owner`, with a fresh heartbeat.
    pub fn claimed_by(self, owner: impl Into<Box<str>>) -> Self {
        Self {
            owner: Some(owner.into()),
            heartbeat: SystemTime::now(),
            ..self
        }
    }

    /// Returns a new state with an explicit owner heartbeat.
    pub fn heartbeated_at(self, heartbeat: SystemTime) -> Self {
        Self { heartbeat, ..self }
    }

    /// Returns a new state at the supplied completed-step count.
    pub fn at_step(self, step: u32) -> Self {
        Self { step, ..self }
    }

    /// Marks the flow as compensating.
    pub fn compensating(self) -> Self {
        Self {
            status: FlowStatus::Compensating,
            ..self
        }
    }

    /// Retains a failure while completed actions are being undone.
    pub fn with_error(self, error: CatgaError) -> Self {
        Self {
            error: Some(error),
            ..self
        }
    }

    /// Marks the flow as persisted and waiting for a later resume.
    pub fn suspended(self) -> Self {
        Self {
            status: FlowStatus::Suspended,
            owner: None,
            ..self
        }
    }

    /// Marks the flow as actively executing under its current owner.
    pub fn running(self) -> Self {
        Self {
            status: FlowStatus::Running,
            ..self
        }
    }

    /// Marks the flow as successfully completed at `step`.
    pub fn done(self, step: u32) -> Self {
        Self {
            status: FlowStatus::Done,
            step,
            owner: None,
            error: None,
            ..self
        }
    }

    /// Marks the flow as failed, retaining the operation error.
    pub fn failed(self, error: CatgaError) -> Self {
        Self {
            status: FlowStatus::Failed,
            owner: None,
            error: Some(error),
            ..self
        }
    }

    /// Marks the flow as cancelled and clears active ownership.
    pub fn cancelled(self) -> Self {
        Self {
            status: FlowStatus::Cancelled,
            owner: None,
            error: None,
            ..self
        }
    }

    /// Returns a new state for the next optimistic-concurrency version.
    pub fn next_version(self) -> Self {
        Self {
            version: self.version.saturating_add(1),
            ..self
        }
    }
}
