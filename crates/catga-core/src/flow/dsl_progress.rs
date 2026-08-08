//! Explicit durable progress for recoverable closure-based DSL flows.

use std::{sync::Arc, time::SystemTime};

use crate::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    MemoryPackWriter, MemoryPackable,
};
use crate::{CatgaError, CatgaResult, ErrorCode};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::memorypack::{TimeWire, decode_time, encode_time};
use super::serde_helpers::{deserialize_arc_slice, serialize_arc_slice};

/// Distinguishes application state from Catga-owned checkpoint metadata.
///
/// Missing values deserialize as [`DslProgressKind::ApplicationState`] so persisted records from
/// before internal checkpoint frames remain application-owned payloads.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum DslProgressKind {
    /// Application-defined bytes produced by [`DslStateCodec`].
    #[default]
    ApplicationState,
    /// Catga-owned, versioned internal execution cursor bytes.
    CheckpointFrame,
    /// Catga-owned terminal success state for a completed checkpointed DSL run.
    Terminal,
}

/// Encodes and restores application state at an explicit durable DSL checkpoint.
pub trait DslStateCodec<S>: Send + Sync {
    /// Serializes the state reached after a durable DSL checkpoint.
    ///
    /// [`crate::DslFlow::run_checkpointed`] is the only consumer that wraps these bytes in its
    /// internal cursor frame for recoverable nested conditional branches.
    fn encode(&self, state: &S) -> CatgaResult<Vec<u8>>;

    /// Restores state supplied by a previously saved checkpoint.
    fn decode(&self, bytes: &[u8]) -> CatgaResult<S>;
}

/// Immutable, versioned progress payload for one named DSL flow step.
///
/// The payload is deliberately opaque to progress stores. `DslFlow::run_checkpointed` reserves a
/// versioned internal frame for in-progress nested conditional branches and otherwise stores the
/// application bytes supplied by [`DslStateCodec`]. Existing raw top-level payloads remain
/// readable for recovery, avoiding unsound attempts to serialize Rust closures.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DslStepProgress {
    flow_id: Box<str>,
    step_index: u32,
    version: i64,
    #[serde(default)]
    kind: DslProgressKind,
    #[serde(
        serialize_with = "serialize_arc_slice",
        deserialize_with = "deserialize_arc_slice"
    )]
    payload: Arc<[u8]>,
    updated_at: SystemTime,
}

#[derive(MemoryPackable)]
struct DslStepProgressWire {
    flow_id: String,
    step_index: u32,
    version: i64,
    kind: u8,
    payload: Vec<u8>,
    updated_at: TimeWire,
}

impl MemoryPackSerialize for DslStepProgress {
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        DslStepProgressWire {
            flow_id: self.flow_id.to_string(),
            step_index: self.step_index,
            version: self.version,
            kind: encode_progress_kind(self.kind),
            payload: self.payload.to_vec(),
            updated_at: encode_time(self.updated_at),
        }
        .serialize(writer)
    }
}

impl MemoryPackDeserialize for DslStepProgress {
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        let wire = DslStepProgressWire::deserialize(reader)?;
        Ok(Self {
            flow_id: wire.flow_id.into_boxed_str(),
            step_index: wire.step_index,
            version: wire.version,
            kind: decode_progress_kind(wire.kind)?,
            payload: Arc::from(wire.payload),
            updated_at: decode_time(wire.updated_at)?,
        })
    }
}

fn encode_progress_kind(value: DslProgressKind) -> u8 {
    match value {
        DslProgressKind::ApplicationState => 0,
        DslProgressKind::CheckpointFrame => 1,
        DslProgressKind::Terminal => 2,
    }
}

fn decode_progress_kind(value: u8) -> Result<DslProgressKind, MemoryPackError> {
    match value {
        0 => Ok(DslProgressKind::ApplicationState),
        1 => Ok(DslProgressKind::CheckpointFrame),
        2 => Ok(DslProgressKind::Terminal),
        value => Err(MemoryPackError::DeserializationError(format!(
            "invalid DSL progress kind: {value}"
        ))),
    }
}

impl DslStepProgress {
    /// Creates initial progress for `step_index` of one durable flow.
    pub fn new(
        flow_id: impl Into<Box<str>>,
        step_index: u32,
        payload: impl Into<Arc<[u8]>>,
    ) -> Self {
        Self {
            flow_id: flow_id.into(),
            step_index,
            version: 0,
            kind: DslProgressKind::ApplicationState,
            payload: payload.into(),
            updated_at: SystemTime::now(),
        }
    }

    /// Returns the durable flow identity.
    pub fn flow_id(&self) -> &str {
        &self.flow_id
    }

    /// Returns the zero-based DSL step index that owns this progress.
    pub const fn step_index(&self) -> u32 {
        self.step_index
    }

    /// Returns the optimistic-concurrency version.
    pub const fn version(&self) -> i64 {
        self.version
    }

    /// Returns whether the payload is application state or an internal checkpoint frame.
    pub const fn kind(&self) -> DslProgressKind {
        self.kind
    }

    /// Returns the application-defined progress payload without copying it.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Returns shared ownership of the payload for zero-copy asynchronous handoff.
    pub fn shared_payload(&self) -> Arc<[u8]> {
        Arc::clone(&self.payload)
    }

    /// Returns when this progress revision was created.
    pub const fn updated_at(&self) -> SystemTime {
        self.updated_at
    }

    /// Replaces the payload and advances the version for a compare-and-set update.
    ///
    /// Returns [`ErrorCode::Conflict`] when the version cannot advance beyond `i64::MAX`.
    pub fn next_version(self, payload: impl Into<Arc<[u8]>>) -> CatgaResult<Self> {
        let version = self.version.checked_add(1).ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Conflict,
                "DSL step progress version cannot advance beyond i64::MAX",
            )
        })?;
        Ok(Self {
            version,
            payload: payload.into(),
            updated_at: SystemTime::now(),
            ..self
        })
    }

    /// Replaces a completed internal cursor with application state at the next version.
    ///
    /// A checkpoint frame is only valid while a nested operation is in progress. Completion must
    /// clear that marker so a future recovery decodes the payload with the caller's
    /// [`DslStateCodec`] instead of treating ordinary state bytes as a Catga-owned frame.
    pub(crate) fn completed_application_state(
        self,
        payload: impl Into<Arc<[u8]>>,
    ) -> CatgaResult<Self> {
        let version = self.version.checked_add(1).ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Conflict,
                "DSL step progress version cannot advance beyond i64::MAX",
            )
        })?;
        Ok(Self {
            version,
            kind: DslProgressKind::ApplicationState,
            payload: payload.into(),
            updated_at: SystemTime::now(),
            ..self
        })
    }

    /// Replaces the payload with a Catga-owned internal checkpoint frame.
    pub(crate) fn checkpoint_frame(self, payload: impl Into<Arc<[u8]>>) -> Self {
        Self {
            kind: DslProgressKind::CheckpointFrame,
            payload: payload.into(),
            ..self
        }
    }

    /// Replaces the payload with a Catga-owned terminal outcome frame.
    pub(crate) fn terminal(self, payload: impl Into<Arc<[u8]>>) -> Self {
        Self {
            kind: DslProgressKind::Terminal,
            payload: payload.into(),
            ..self
        }
    }

    /// Returns whether `next` is the exact representable successor of `expected`.
    ///
    /// Progress stores use this to reject same-version writes at `i64::MAX` and every other
    /// non-successor update before persisting it.
    pub const fn is_next_version(expected: i64, next: i64) -> bool {
        matches!(expected.checked_add(1), Some(candidate) if candidate == next)
    }
}

/// Persists explicitly encoded progress for recoverable DSL flow steps.
#[async_trait]
pub trait DslStepProgressStore: Send + Sync {
    /// Creates initial step progress only when no record has the same flow and step identity.
    async fn create(&self, progress: DslStepProgress) -> CatgaResult<bool>;

    /// Replaces progress only when `expected_version` is current and `next` is its exact
    /// representable successor.
    async fn update(&self, expected_version: i64, next: DslStepProgress) -> CatgaResult<bool>;

    /// Loads progress for one flow step.
    async fn get(&self, flow_id: &str, step_index: u32) -> CatgaResult<Option<DslStepProgress>>;

    /// Deletes progress for one flow step and reports whether a record existed.
    async fn delete(&self, flow_id: &str, step_index: u32) -> CatgaResult<bool>;
}

