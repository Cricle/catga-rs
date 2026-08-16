//! Shared MemoryPack wire helpers for durable flow records.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    MemoryPackWriter, MemoryPackable,
};

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
