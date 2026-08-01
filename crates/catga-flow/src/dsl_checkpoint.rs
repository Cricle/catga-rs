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

#[cfg(test)]
mod tests {
    use super::*;

    fn memorypack_error<T>(result: Result<T, MemoryPackError>) -> MemoryPackError {
        match result {
            Ok(_) => panic!("invalid checkpoint unexpectedly decoded"),
            Err(error) => error,
        }
    }

    fn decode_work(work: CheckpointWork) -> CheckpointWork {
        let bytes = MemoryPackSerializer::serialize(&work).expect("encode checkpoint work");
        MemoryPackSerializer::deserialize(&bytes).expect("decode checkpoint work")
    }

    #[test]
    fn checkpoint_work_round_trips_every_persisted_cursor_shape() {
        assert!(matches!(
            decode_work(CheckpointWork::Branch),
            CheckpointWork::Branch
        ));

        assert!(matches!(
            decode_work(CheckpointWork::ForEach {
                next_index: 2,
                total: 5,
            }),
            CheckpointWork::ForEach {
                next_index: 2,
                total: 5,
            }
        ));

        assert!(matches!(
            decode_work(CheckpointWork::ReplayableForEach {
                next_index: 1,
                items: vec![b"first".to_vec(), b"second".to_vec()],
            }),
            CheckpointWork::ReplayableForEach { next_index: 1, items }
                if items == [b"first".to_vec(), b"second".to_vec()]
        ));

        assert!(matches!(
            decode_work(CheckpointWork::Parallel {
                states: vec![Some(b"done".to_vec()), None],
            }),
            CheckpointWork::Parallel { states }
                if states == [Some(b"done".to_vec()), None]
        ));

        assert!(matches!(
            decode_work(CheckpointWork::WhenAny {
                winner: 3,
                state: b"winner".to_vec(),
            }),
            CheckpointWork::WhenAny { winner: 3, state } if state == b"winner"
        ));

        assert!(matches!(
            decode_work(CheckpointWork::ParallelBranches {
                branches: vec![
                    Some(ParallelBranchProgress::Completed {
                        state: b"complete".to_vec(),
                    }),
                    Some(ParallelBranchProgress::InProgress {
                        step_index: 4,
                        checkpoint_frame: true,
                        payload: b"resume".to_vec(),
                    }),
                    None,
                ],
            }),
            CheckpointWork::ParallelBranches { branches }
                if matches!(
                    &branches[..],
                    [
                        Some(ParallelBranchProgress::Completed { state }),
                        Some(ParallelBranchProgress::InProgress {
                            step_index: 4,
                            checkpoint_frame: true,
                            payload,
                        }),
                        None,
                    ] if state == b"complete" && payload == b"resume"
                )
        ));
    }

    #[test]
    fn checkpoint_frame_round_trips_nested_path_state_and_cursor() {
        let levels = [
            CheckpointLevel {
                branch: 1,
                next_step: 2,
            },
            CheckpointLevel {
                branch: 3,
                next_step: 4,
            },
        ];
        let encoded = CheckpointFrame::encode(
            &levels,
            b"application state".to_vec(),
            CheckpointWork::ParallelBranches {
                branches: vec![Some(ParallelBranchProgress::InProgress {
                    step_index: 7,
                    checkpoint_frame: false,
                    payload: b"nested state".to_vec(),
                })],
            },
        )
        .expect("encode checkpoint frame");

        let decoded = CheckpointFrame::decode(&encoded)
            .expect("decode checkpoint frame")
            .expect("checkpoint magic must produce a frame");
        assert_eq!(decoded.levels.len(), 2);
        assert_eq!(decoded.levels[0].branch, 1);
        assert_eq!(decoded.levels[0].next_step, 2);
        assert_eq!(decoded.levels[1].branch, 3);
        assert_eq!(decoded.levels[1].next_step, 4);
        assert_eq!(decoded.state, b"application state");
        assert!(matches!(
            decoded.work,
            CheckpointWork::ParallelBranches { branches }
                if matches!(
                    &branches[..],
                    [Some(ParallelBranchProgress::InProgress {
                        step_index: 7,
                        checkpoint_frame: false,
                        payload,
                    })] if payload == b"nested state"
                )
        ));
    }

    #[test]
    fn checkpoint_frame_accepts_legacy_branch_frames_without_a_work_cursor() {
        let mut legacy = Vec::from(CHECKPOINT_FRAME_MAGIC.as_slice());
        legacy.extend([CHECKPOINT_FRAME_VERSION, 0]);
        legacy.extend(0_u32.to_be_bytes());

        let decoded = CheckpointFrame::decode(&legacy)
            .expect("decode legacy frame")
            .expect("checkpoint magic must produce a frame");
        assert!(decoded.levels.is_empty());
        assert!(decoded.state.is_empty());
        assert!(matches!(decoded.work, CheckpointWork::Branch));
    }

    #[test]
    fn checkpoint_frame_rejects_size_version_depth_and_cursor_corruption() {
        assert!(
            CheckpointFrame::decode(b"application state")
                .expect("non-frame payload is application state")
                .is_none()
        );

        for payload in [
            b"CDF1".as_slice(),
            b"CDF1\x02\0\0\0\0\0".as_slice(),
            b"CDF1\x01!\0\0\0\0".as_slice(),
            b"CDF1\x01\0\0\0\0\x01".as_slice(),
        ] {
            let error = match CheckpointFrame::decode(payload) {
                Err(error) => error,
                Ok(_) => panic!("corrupt checkpoint unexpectedly decoded"),
            };
            assert_eq!(error.code(), ErrorCode::Validation);
        }

        let valid = CheckpointFrame::encode(&[], b"state".to_vec(), CheckpointWork::Branch)
            .expect("encode frame");
        let mut invalid_work_length = valid.clone();
        let work_length = invalid_work_length.len() - 5;
        invalid_work_length[work_length..work_length + 4].copy_from_slice(&99_u32.to_be_bytes());
        assert_eq!(
            match CheckpointFrame::decode(&invalid_work_length) {
                Err(error) => error.code(),
                Ok(_) => panic!("declared work beyond frame unexpectedly decoded"),
            },
            ErrorCode::Validation
        );

        let mut invalid_work_tag = valid;
        let last = invalid_work_tag.len() - 1;
        invalid_work_tag[last] = 99;
        assert_eq!(
            match CheckpointFrame::decode(&invalid_work_tag) {
                Err(error) => error.code(),
                Ok(_) => panic!("unknown work tag unexpectedly decoded"),
            },
            ErrorCode::Validation
        );
    }

    #[test]
    fn checkpoint_frame_enforces_depth_and_state_size_limits() {
        let levels = vec![
            CheckpointLevel {
                branch: 0,
                next_step: 0,
            };
            MAX_CHECKPOINT_PATH_DEPTH + 1
        ];
        assert_eq!(
            CheckpointFrame::encode(&levels, Vec::new(), CheckpointWork::Branch)
                .expect_err("deep checkpoint path rejected")
                .code(),
            ErrorCode::Validation
        );
        assert_eq!(
            CheckpointFrame::encode(
                &[],
                vec![0; MAX_CHECKPOINT_STATE_BYTES + 1],
                CheckpointWork::Branch,
            )
            .expect_err("large checkpoint state rejected")
            .code(),
            ErrorCode::Validation
        );
    }

    #[test]
    fn checkpoint_cursor_tags_reject_unknown_values() {
        let error = memorypack_error(MemoryPackSerializer::deserialize::<CheckpointWork>(&[
            u8::MAX,
        ]));
        assert!(matches!(error, MemoryPackError::DeserializationError(_)));

        let error = memorypack_error(MemoryPackSerializer::deserialize::<ParallelBranchProgress>(
            &[u8::MAX],
        ));
        assert!(matches!(error, MemoryPackError::DeserializationError(_)));
    }

    #[test]
    fn checkpoint_frame_rejects_oversized_state_and_trailing_cursor_bytes() {
        let mut oversized_state = Vec::from(CHECKPOINT_FRAME_MAGIC.as_slice());
        oversized_state.extend([CHECKPOINT_FRAME_VERSION, 0]);
        oversized_state.extend(((MAX_CHECKPOINT_STATE_BYTES as u32) + 1).to_be_bytes());
        assert_eq!(
            match CheckpointFrame::decode(&oversized_state) {
                Err(error) => error.code(),
                Ok(_) => panic!("declared oversized state unexpectedly decoded"),
            },
            ErrorCode::Validation
        );

        let mut trailing_cursor =
            CheckpointFrame::encode(&[], Vec::new(), CheckpointWork::Branch).expect("frame");
        trailing_cursor.push(0);
        assert_eq!(
            match CheckpointFrame::decode(&trailing_cursor) {
                Err(error) => error.code(),
                Ok(_) => panic!("trailing cursor unexpectedly decoded"),
            },
            ErrorCode::Validation
        );
    }
}
