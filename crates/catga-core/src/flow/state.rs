use std::{sync::Arc, time::SystemTime};

use crate::codec::memorypack::{
    MemoryPackDecodeLimits, MemoryPackDeserialize, MemoryPackError, MemoryPackReader,
    MemoryPackSerialize, MemoryPackWriter, MemoryPackable,
};
use crate::{CatgaError, CatgaResult, ErrorCode};
use serde::{Deserialize, Serialize};

use super::memorypack::{
    ErrorWire, TimeWire, decode_error, decode_time, encode_error, encode_time,
};
use super::serde_helpers::{deserialize_arc_slice, serialize_arc_slice};

/// Maximum accepted durable flow input payload size in bytes.
pub const MAX_FLOW_DATA_BYTES: usize = 1024 * 1024;

const MAX_FLOW_CODEC_METADATA_BYTES: usize = 1024 * 1024;

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
///
/// State transitions return a new value, so callers can validate and persist the exact revision
/// selected by their optimistic-concurrency store.
///
/// ```no_run
/// use catga_core::flow::{FlowState, FlowStatus};
///
/// let state = FlowState::new("flow-42", "checkout", Vec::<u8>::new(), "worker-a")
///     .next_version()?
///     .done(1);
///
/// assert_eq!(state.status(), FlowStatus::Done);
/// assert_eq!(state.version(), 1);
/// # Ok::<(), catga_core::CatgaError>(())
/// ```
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FlowState {
    id: Box<str>,
    flow_type: Box<str>,
    status: FlowStatus,
    step: u32,
    version: i64,
    owner: Option<Box<str>>,
    heartbeat: SystemTime,
    #[serde(serialize_with = "serialize_arc_slice", deserialize_with = "deserialize_arc_slice")]
    data: Arc<[u8]>,
    error: Option<CatgaError>,
}

#[derive(MemoryPackable)]
struct FlowStateWire {
    id: String,
    flow_type: String,
    status: u8,
    step: u32,
    version: i64,
    owner: Option<String>,
    heartbeat: TimeWire,
    data: Vec<u8>,
    error: Option<ErrorWire>,
}

impl MemoryPackSerialize for FlowState {
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        self.validate()
            .map_err(|error| MemoryPackError::SerializationError(error.message().to_owned()))?;
        FlowStateWire {
            id: self.id.to_string(),
            flow_type: self.flow_type.to_string(),
            status: encode_flow_status(self.status),
            step: self.step,
            version: self.version,
            owner: self.owner.as_deref().map(str::to_owned),
            heartbeat: encode_time(self.heartbeat),
            data: self.data.to_vec(),
            error: self.error.as_ref().map(encode_error),
        }
        .serialize(writer)
    }
}

impl MemoryPackDeserialize for FlowState {
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        let wire = FlowStateWire::deserialize(reader)?;
        let state = Self {
            id: wire.id.into_boxed_str(),
            flow_type: wire.flow_type.into_boxed_str(),
            status: decode_flow_status(wire.status)?,
            step: wire.step,
            version: wire.version,
            owner: wire.owner.map(String::into_boxed_str),
            heartbeat: decode_time(wire.heartbeat)?,
            data: Arc::from(wire.data),
            error: wire.error.map(decode_error).transpose()?,
        };
        state.validate().map_err(|error| {
            MemoryPackError::DeserializationError(format!(
                "invalid flow state: {}",
                error.message()
            ))
        })?;
        Ok(state)
    }
}

fn encode_flow_status(value: FlowStatus) -> u8 {
    match value {
        FlowStatus::Running => 0,
        FlowStatus::Compensating => 1,
        FlowStatus::Suspended => 2,
        FlowStatus::Done => 3,
        FlowStatus::Failed => 4,
        FlowStatus::Cancelled => 5,
    }
}

fn decode_flow_status(value: u8) -> Result<FlowStatus, MemoryPackError> {
    match value {
        0 => Ok(FlowStatus::Running),
        1 => Ok(FlowStatus::Compensating),
        2 => Ok(FlowStatus::Suspended),
        3 => Ok(FlowStatus::Done),
        4 => Ok(FlowStatus::Failed),
        5 => Ok(FlowStatus::Cancelled),
        value => Err(MemoryPackError::DeserializationError(format!(
            "invalid flow status: {value}"
        ))),
    }
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

    /// Validates the durable flow state before it crosses a persistence boundary.
    pub fn validate(&self) -> CatgaResult<()> {
        if self.data.len() > MAX_FLOW_DATA_BYTES {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                format!("flow input payload exceeds {MAX_FLOW_DATA_BYTES} bytes"),
            ));
        }
        Ok(())
    }

    /// Returns the bounded MemoryPack decode policy for durable flow records.
    pub fn memorypack_decode_limits() -> CatgaResult<MemoryPackDecodeLimits> {
        let maximum = MAX_FLOW_DATA_BYTES.saturating_add(MAX_FLOW_CODEC_METADATA_BYTES);
        MemoryPackDecodeLimits::new(maximum, maximum, 256 * 1024, MAX_FLOW_DATA_BYTES, 32).map_err(
            |error| {
                CatgaError::new(
                    ErrorCode::Internal,
                    format!("cannot configure flow state decode limits: {error}"),
                )
            },
        )
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
    ///
    /// Version saturation is rejected rather than allowing another durable transition to reuse
    /// `i64::MAX` and weaken compare-and-swap fencing.
    pub fn next_version(self) -> CatgaResult<Self> {
        let version = self.version.checked_add(1).ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Conflict,
                "flow state version cannot advance beyond i64::MAX",
            )
        })?;
        Ok(Self { version, ..self })
    }

    /// Returns whether `next` is the exact representable successor of `expected`.
    pub const fn is_next_version(expected: i64, next: i64) -> bool {
        matches!(expected.checked_add(1), Some(candidate) if candidate == next)
    }
}
