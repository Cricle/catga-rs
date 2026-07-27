//! Internal checkpoint-frame encoding for the closure-based DSL.

use catga_codec_memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    MemoryPackSerializer, MemoryPackWriter, MemoryPackable,
};
use catga_core::{CatgaError, CatgaResult, ErrorCode};

pub(super) const MAX_CHECKPOINT_PATH_DEPTH: usize = 32;
const CHECKPOINT_FRAME_MAGIC: &[u8; 4] = b"CDF1";
const CHECKPOINT_FRAME_VERSION: u8 = 1;
const CHECKPOINT_FRAME_HEADER_BYTES: usize = 10;
const CHECKPOINT_LEVEL_BYTES: usize = 8;
const MAX_CHECKPOINT_STATE_BYTES: usize = 1024 * 1024;
const MAX_CHECKPOINT_FRAME_BYTES: usize = CHECKPOINT_FRAME_HEADER_BYTES
    + CHECKPOINT_LEVEL_BYTES * MAX_CHECKPOINT_PATH_DEPTH
    + MAX_CHECKPOINT_STATE_BYTES;

#[derive(Clone, Copy)]
pub(super) struct CheckpointLevel {
    pub(super) branch: u32,
    pub(super) next_step: u32,
}

pub(super) struct CheckpointFrame {
    pub(super) levels: Vec<CheckpointLevel>,
    pub(super) state: Vec<u8>,
    pub(super) work: CheckpointWork,
}

#[derive(Clone)]
pub(super) enum CheckpointWork {
    Branch,
    // Retained so older serialized frames remain decodable and reject at the legacy API boundary.
    ForEach {
        next_index: u32,
        total: u32,
    },
    ReplayableForEach {
        next_index: u32,
        items: Vec<Vec<u8>>,
    },
    Parallel {
        states: Vec<Option<Vec<u8>>>,
    },
    WhenAny {
        winner: u32,
        state: Vec<u8>,
    },
    ParallelBranches {
        branches: Vec<Option<ParallelBranchProgress>>,
    },
}

#[derive(Clone)]
pub(super) enum ParallelBranchProgress {
    Completed {
        state: Vec<u8>,
    },
    InProgress {
        step_index: u32,
        checkpoint_frame: bool,
        payload: Vec<u8>,
    },
}

#[derive(Default, MemoryPackable)]
struct ForEachWire {
    next_index: u32,
    total: u32,
}

#[derive(Default, MemoryPackable)]
struct ReplayableForEachWire {
    next_index: u32,
    items: Vec<Vec<u8>>,
}

#[derive(Default, MemoryPackable)]
struct ParallelWire {
    states: Vec<Option<Vec<u8>>>,
}

#[derive(Default, MemoryPackable)]
struct WhenAnyWire {
    winner: u32,
    state: Vec<u8>,
}

#[derive(Default, MemoryPackable)]
struct ParallelBranchesWire {
    branches: Vec<Option<ParallelBranchProgress>>,
}

#[derive(Default, MemoryPackable)]
struct ParallelCompletedWire {
    state: Vec<u8>,
}

#[derive(Default, MemoryPackable)]
struct ParallelInProgressWire {
    step_index: u32,
    checkpoint_frame: bool,
    payload: Vec<u8>,
}

impl MemoryPackSerialize for CheckpointWork {
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        match self {
            Self::Branch => writer.write_u8(0),
            Self::ForEach { next_index, total } => {
                writer.write_u8(1)?;
                ForEachWire {
                    next_index: *next_index,
                    total: *total,
                }
                .serialize(writer)
            }
            Self::ReplayableForEach { next_index, items } => {
                writer.write_u8(2)?;
                ReplayableForEachWire {
                    next_index: *next_index,
                    items: items.clone(),
                }
                .serialize(writer)
            }
            Self::Parallel { states } => {
                writer.write_u8(3)?;
                ParallelWire {
                    states: states.clone(),
                }
                .serialize(writer)
            }
            Self::WhenAny { winner, state } => {
                writer.write_u8(4)?;
                WhenAnyWire {
                    winner: *winner,
                    state: state.clone(),
                }
                .serialize(writer)
            }
            Self::ParallelBranches { branches } => {
                writer.write_u8(5)?;
                ParallelBranchesWire {
                    branches: branches.clone(),
                }
                .serialize(writer)
            }
        }
    }
}

impl MemoryPackDeserialize for CheckpointWork {
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        match reader.read_u8()? {
            0 => Ok(Self::Branch),
            1 => {
                let wire = ForEachWire::deserialize(reader)?;
                Ok(Self::ForEach {
                    next_index: wire.next_index,
                    total: wire.total,
                })
            }
            2 => {
                let wire = ReplayableForEachWire::deserialize(reader)?;
                Ok(Self::ReplayableForEach {
                    next_index: wire.next_index,
                    items: wire.items,
                })
            }
            3 => Ok(Self::Parallel {
                states: ParallelWire::deserialize(reader)?.states,
            }),
            4 => {
                let wire = WhenAnyWire::deserialize(reader)?;
                Ok(Self::WhenAny {
                    winner: wire.winner,
                    state: wire.state,
                })
            }
            5 => Ok(Self::ParallelBranches {
                branches: ParallelBranchesWire::deserialize(reader)?.branches,
            }),
            value => Err(MemoryPackError::DeserializationError(format!(
                "invalid DSL checkpoint work tag: {value}"
            ))),
        }
    }
}

impl MemoryPackSerialize for ParallelBranchProgress {
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        match self {
            Self::Completed { state } => {
                writer.write_u8(0)?;
                ParallelCompletedWire {
                    state: state.clone(),
                }
                .serialize(writer)
            }
            Self::InProgress {
                step_index,
                checkpoint_frame,
                payload,
            } => {
                writer.write_u8(1)?;
                ParallelInProgressWire {
                    step_index: *step_index,
                    checkpoint_frame: *checkpoint_frame,
                    payload: payload.clone(),
                }
                .serialize(writer)
            }
        }
    }
}

impl MemoryPackDeserialize for ParallelBranchProgress {
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        match reader.read_u8()? {
            0 => Ok(Self::Completed {
                state: ParallelCompletedWire::deserialize(reader)?.state,
            }),
            1 => {
                let wire = ParallelInProgressWire::deserialize(reader)?;
                Ok(Self::InProgress {
                    step_index: wire.step_index,
                    checkpoint_frame: wire.checkpoint_frame,
                    payload: wire.payload,
                })
            }
            value => Err(MemoryPackError::DeserializationError(format!(
                "invalid DSL parallel branch tag: {value}"
            ))),
        }
    }
}

impl Default for ParallelBranchProgress {
    fn default() -> Self {
        Self::Completed { state: Vec::new() }
    }
}

impl CheckpointFrame {
    pub(super) fn encode(
        levels: &[CheckpointLevel],
        state: Vec<u8>,
        work: CheckpointWork,
    ) -> CatgaResult<Vec<u8>> {
        if levels.len() > MAX_CHECKPOINT_PATH_DEPTH {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "DSL checkpoint path exceeds the maximum depth",
            ));
        }
        if state.len() > MAX_CHECKPOINT_STATE_BYTES {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "DSL checkpoint state exceeds the maximum size",
            ));
        }
        let level_bytes = levels
            .len()
            .checked_mul(CHECKPOINT_LEVEL_BYTES)
            .ok_or_else(|| {
                CatgaError::new(ErrorCode::Validation, "DSL checkpoint path is too large")
            })?;
        let work = MemoryPackSerializer::serialize(&work).map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "DSL checkpoint work cursor cannot be encoded",
            )
        })?;
        let work_len = u32::try_from(work.len()).map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "DSL checkpoint work cursor is too large",
            )
        })?;
        let capacity = CHECKPOINT_FRAME_HEADER_BYTES
            .checked_add(level_bytes)
            .and_then(|value| value.checked_add(state.len()))
            .and_then(|value| value.checked_add(4))
            .and_then(|value| value.checked_add(work.len()))
            .ok_or_else(|| {
                CatgaError::new(ErrorCode::Validation, "DSL checkpoint frame is too large")
            })?;
        if capacity > MAX_CHECKPOINT_FRAME_BYTES {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "DSL checkpoint frame exceeds the maximum size",
            ));
        }
        let level_count = u8::try_from(levels.len()).map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "DSL checkpoint path exceeds the maximum depth",
            )
        })?;
        let state_len = u32::try_from(state.len()).map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "DSL checkpoint state exceeds the maximum size",
            )
        })?;
        let mut payload = Vec::with_capacity(capacity);
        payload.extend_from_slice(CHECKPOINT_FRAME_MAGIC);
        payload.push(CHECKPOINT_FRAME_VERSION);
        payload.push(level_count);
        for level in levels {
            payload.extend_from_slice(&level.branch.to_be_bytes());
            payload.extend_from_slice(&level.next_step.to_be_bytes());
        }
        payload.extend_from_slice(&state_len.to_be_bytes());
        payload.extend_from_slice(&state);
        payload.extend_from_slice(&work_len.to_be_bytes());
        payload.extend_from_slice(&work);
        Ok(payload)
    }

    pub(super) fn decode(payload: &[u8]) -> CatgaResult<Option<Self>> {
        if !payload.starts_with(CHECKPOINT_FRAME_MAGIC) {
            return Ok(None);
        }
        if payload.len() > MAX_CHECKPOINT_FRAME_BYTES
            || payload.len() < CHECKPOINT_FRAME_HEADER_BYTES
        {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "DSL checkpoint frame has an invalid size",
            ));
        }
        if payload[4] != CHECKPOINT_FRAME_VERSION {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "DSL checkpoint frame has an unsupported version",
            ));
        }
        let level_count = usize::from(payload[5]);
        if level_count > MAX_CHECKPOINT_PATH_DEPTH {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "DSL checkpoint frame exceeds the maximum path depth",
            ));
        }
        let level_bytes = level_count
            .checked_mul(CHECKPOINT_LEVEL_BYTES)
            .ok_or_else(|| {
                CatgaError::new(ErrorCode::Validation, "DSL checkpoint path is too large")
            })?;
        let state_length_offset = 6_usize.checked_add(level_bytes).ok_or_else(|| {
            CatgaError::new(ErrorCode::Validation, "DSL checkpoint frame is too large")
        })?;
        let state_offset = state_length_offset.checked_add(4).ok_or_else(|| {
            CatgaError::new(ErrorCode::Validation, "DSL checkpoint frame is too large")
        })?;
        if state_offset > payload.len() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "DSL checkpoint frame is truncated",
            ));
        }
        let state_len = usize::try_from(u32::from_be_bytes(
            payload[state_length_offset..state_offset]
                .try_into()
                .map_err(|_| {
                    CatgaError::new(
                        ErrorCode::Validation,
                        "DSL checkpoint state length is invalid",
                    )
                })?,
        ))
        .map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "DSL checkpoint state length is too large",
            )
        })?;
        if state_len > MAX_CHECKPOINT_STATE_BYTES {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "DSL checkpoint state exceeds the maximum size",
            ));
        }
        let expected_len = state_offset.checked_add(state_len).ok_or_else(|| {
            CatgaError::new(ErrorCode::Validation, "DSL checkpoint frame is too large")
        })?;
        let work = if expected_len == payload.len() {
            CheckpointWork::Branch
        } else {
            let work_length_end = expected_len.checked_add(4).ok_or_else(|| {
                CatgaError::new(
                    ErrorCode::Validation,
                    "DSL checkpoint work cursor is too large",
                )
            })?;
            if work_length_end > payload.len() {
                return Err(CatgaError::new(
                    ErrorCode::Validation,
                    "DSL checkpoint work cursor is truncated",
                ));
            }
            let work_len = usize::try_from(u32::from_be_bytes(
                payload[expected_len..work_length_end]
                    .try_into()
                    .map_err(|_| {
                        CatgaError::new(
                            ErrorCode::Validation,
                            "DSL checkpoint work length is invalid",
                        )
                    })?,
            ))
            .map_err(|_| {
                CatgaError::new(
                    ErrorCode::Validation,
                    "DSL checkpoint work cursor is too large",
                )
            })?;
            let work_end = work_length_end.checked_add(work_len).ok_or_else(|| {
                CatgaError::new(
                    ErrorCode::Validation,
                    "DSL checkpoint work cursor is too large",
                )
            })?;
            if work_end != payload.len() {
                return Err(CatgaError::new(
                    ErrorCode::Validation,
                    "DSL checkpoint work cursor has an invalid length",
                ));
            }
            MemoryPackSerializer::deserialize_bounded(
                &payload[work_length_end..work_end],
                Default::default(),
            )
            .map_err(|_| {
                CatgaError::new(
                    ErrorCode::Validation,
                    "DSL checkpoint work cursor is invalid",
                )
            })?
        };
        let mut levels = Vec::with_capacity(level_count);
        for index in 0..level_count {
            let offset = 6_usize
                .checked_add(index.checked_mul(CHECKPOINT_LEVEL_BYTES).ok_or_else(|| {
                    CatgaError::new(ErrorCode::Validation, "DSL checkpoint path is too large")
                })?)
                .ok_or_else(|| {
                    CatgaError::new(ErrorCode::Validation, "DSL checkpoint path is too large")
                })?;
            let branch_end = offset.checked_add(4).ok_or_else(|| {
                CatgaError::new(ErrorCode::Validation, "DSL checkpoint path is too large")
            })?;
            let step_end = branch_end.checked_add(4).ok_or_else(|| {
                CatgaError::new(ErrorCode::Validation, "DSL checkpoint path is too large")
            })?;
            let branch =
                u32::from_be_bytes(payload[offset..branch_end].try_into().map_err(|_| {
                    CatgaError::new(ErrorCode::Validation, "DSL checkpoint branch is invalid")
                })?);
            let next_step =
                u32::from_be_bytes(payload[branch_end..step_end].try_into().map_err(|_| {
                    CatgaError::new(ErrorCode::Validation, "DSL checkpoint step is invalid")
                })?);
            levels.push(CheckpointLevel { branch, next_step });
        }
        Ok(Some(Self {
            levels,
            state: payload[state_offset..expected_len].to_vec(),
            work,
        }))
    }
}
