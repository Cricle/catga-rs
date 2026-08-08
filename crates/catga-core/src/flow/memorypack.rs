//! Shared MemoryPack wire helpers for durable flow records.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    MemoryPackWriter, MemoryPackable,
};
use crate::{CatgaError, ErrorCode};

pub(crate) const TIME_WIRE_BYTES: usize = 13;

#[derive(Default, MemoryPackable)]
pub(crate) struct TimeWire {
    before_epoch: bool,
    seconds: u64,
    nanoseconds: u32,
}

#[derive(Default, MemoryPackable)]
pub(crate) struct DurationWire {
    seconds: u64,
    nanoseconds: u32,
}

#[derive(Default, MemoryPackable)]
pub(crate) struct ErrorWire {
    code: u8,
    message: String,
    details: Option<String>,
    retryable: bool,
}

pub(crate) fn encode_time(value: SystemTime) -> TimeWire {
    let (before_epoch, duration) = match value.duration_since(UNIX_EPOCH) {
        Ok(duration) => (false, duration),
        Err(error) => (true, error.duration()),
    };
    TimeWire {
        before_epoch,
        seconds: duration.as_secs(),
        nanoseconds: duration.subsec_nanos(),
    }
}

pub(crate) fn decode_time(value: TimeWire) -> Result<SystemTime, MemoryPackError> {
    if value.nanoseconds >= 1_000_000_000 {
        return Err(MemoryPackError::DeserializationError(
            "flow timestamp nanoseconds are out of range".into(),
        ));
    }
    let duration = Duration::new(value.seconds, value.nanoseconds);
    if value.before_epoch {
        UNIX_EPOCH.checked_sub(duration).ok_or_else(|| {
            MemoryPackError::DeserializationError("flow timestamp is out of range".into())
        })
    } else {
        UNIX_EPOCH.checked_add(duration).ok_or_else(|| {
            MemoryPackError::DeserializationError("flow timestamp is out of range".into())
        })
    }
}

pub(crate) fn encode_time_wire(value: SystemTime, output: &mut Vec<u8>) {
    let wire = encode_time(value);
    output.push(u8::from(wire.before_epoch));
    output.extend_from_slice(&wire.seconds.to_be_bytes());
    output.extend_from_slice(&wire.nanoseconds.to_be_bytes());
}

pub(crate) fn decode_time_wire(value: &[u8]) -> Result<SystemTime, MemoryPackError> {
    if value.len() != TIME_WIRE_BYTES {
        return Err(MemoryPackError::DeserializationError(
            "flow timestamp wire size is invalid".into(),
        ));
    }
    let before_epoch = match value[0] {
        0 => false,
        1 => true,
        _ => {
            return Err(MemoryPackError::DeserializationError(
                "flow timestamp epoch flag is invalid".into(),
            ));
        }
    };
    let seconds = u64::from_be_bytes(value[1..9].try_into().map_err(|_| {
        MemoryPackError::DeserializationError("flow timestamp seconds are malformed".into())
    })?);
    let nanoseconds = u32::from_be_bytes(value[9..TIME_WIRE_BYTES].try_into().map_err(|_| {
        MemoryPackError::DeserializationError("flow timestamp nanoseconds are malformed".into())
    })?);
    decode_time(TimeWire {
        before_epoch,
        seconds,
        nanoseconds,
    })
}

pub(crate) fn encode_duration(value: Duration) -> DurationWire {
    DurationWire {
        seconds: value.as_secs(),
        nanoseconds: value.subsec_nanos(),
    }
}

pub(crate) fn decode_duration(value: DurationWire) -> Duration {
    Duration::new(value.seconds, value.nanoseconds)
}

pub(crate) fn encode_error(value: &CatgaError) -> ErrorWire {
    ErrorWire {
        code: encode_error_code(value.code()),
        message: value.message().to_owned(),
        details: value.details().map(str::to_owned),
        retryable: value.is_retryable(),
    }
}

pub(crate) fn decode_error(value: ErrorWire) -> Result<CatgaError, MemoryPackError> {
    let code = decode_error_code(value.code)?;
    if value.retryable != code.is_retryable() {
        return Err(MemoryPackError::DeserializationError(
            "flow error retryability does not match its error code".into(),
        ));
    }
    let error = CatgaError::new(code, value.message);
    Ok(match value.details {
        Some(details) => error.with_details(details),
        None => error,
    })
}

pub(crate) fn encode_error_code(value: ErrorCode) -> u8 {
    match value {
        ErrorCode::Validation => 0,
        ErrorCode::NotFound => 1,
        ErrorCode::Conflict => 2,
        ErrorCode::Unauthorized => 3,
        ErrorCode::Forbidden => 4,
        ErrorCode::Cancelled => 5,
        ErrorCode::Timeout => 6,
        ErrorCode::Unsupported => 7,
        ErrorCode::Transient => 8,
        ErrorCode::Unavailable => 9,
        ErrorCode::Internal => 10,
        ErrorCode::HandlerFailed => 11,
        ErrorCode::HandlerNotFound => 12,
        ErrorCode::PipelineFailed => 13,
        ErrorCode::PersistenceFailed => 14,
        ErrorCode::LockFailed => 15,
        ErrorCode::TransportFailed => 16,
        ErrorCode::SerializationFailed => 17,
        ErrorCode::FlowFailed => 18,
        ErrorCode::FlowCancelled => 19,
        ErrorCode::FlowTimeout => 20,
        ErrorCode::FlowCompensating => 21,
    }
}

pub(crate) fn decode_error_code(value: u8) -> Result<ErrorCode, MemoryPackError> {
    match value {
        0 => Ok(ErrorCode::Validation),
        1 => Ok(ErrorCode::NotFound),
        2 => Ok(ErrorCode::Conflict),
        3 => Ok(ErrorCode::Unauthorized),
        4 => Ok(ErrorCode::Forbidden),
        5 => Ok(ErrorCode::Cancelled),
        6 => Ok(ErrorCode::Timeout),
        7 => Ok(ErrorCode::Unsupported),
        8 => Ok(ErrorCode::Transient),
        9 => Ok(ErrorCode::Unavailable),
        10 => Ok(ErrorCode::Internal),
        11 => Ok(ErrorCode::HandlerFailed),
        12 => Ok(ErrorCode::HandlerNotFound),
        13 => Ok(ErrorCode::PipelineFailed),
        14 => Ok(ErrorCode::PersistenceFailed),
        15 => Ok(ErrorCode::LockFailed),
        16 => Ok(ErrorCode::TransportFailed),
        17 => Ok(ErrorCode::SerializationFailed),
        18 => Ok(ErrorCode::FlowFailed),
        19 => Ok(ErrorCode::FlowCancelled),
        20 => Ok(ErrorCode::FlowTimeout),
        21 => Ok(ErrorCode::FlowCompensating),
        value => Err(MemoryPackError::DeserializationError(format!(
            "invalid flow error code: {value}"
        ))),
    }
}

