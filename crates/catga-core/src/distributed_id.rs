//! Lock-free Snowflake distributed ID generation.

use std::{
    hint::spin_loop,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{CatgaError, CatgaResult, ErrorCode};

const INITIALIZED: u64 = 1 << 63;
const DEFAULT_EPOCH_MILLIS: u64 = 1_704_067_200_000;

/// Configurable allocation of the 63 data bits in a Snowflake ID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnowflakeLayout {
    timestamp_bits: u8,
    worker_id_bits: u8,
    sequence_bits: u8,
    epoch_millis: u64,
}

impl SnowflakeLayout {
    /// Creates and validates a custom 63-bit Snowflake layout.
    pub fn new(
        timestamp_bits: u8,
        worker_id_bits: u8,
        sequence_bits: u8,
        epoch_millis: u64,
    ) -> CatgaResult<Self> {
        let layout = Self {
            timestamp_bits,
            worker_id_bits,
            sequence_bits,
            epoch_millis,
        };
        layout.validate()?;
        Ok(layout)
    }

    /// Returns the number of bits assigned to elapsed milliseconds.
    pub const fn timestamp_bits(&self) -> u8 {
        self.timestamp_bits
    }

    /// Returns the number of bits assigned to the worker identifier.
    pub const fn worker_id_bits(&self) -> u8 {
        self.worker_id_bits
    }

    /// Returns the number of bits assigned to the per-millisecond sequence.
    pub const fn sequence_bits(&self) -> u8 {
        self.sequence_bits
    }

    /// Returns the Unix-millisecond custom epoch.
    pub const fn epoch_millis(&self) -> u64 {
        self.epoch_millis
    }

    /// Returns the highest valid worker identifier.
    pub const fn max_worker_id(&self) -> u32 {
        ((1_u64 << self.worker_id_bits) - 1) as u32
    }

    /// Returns the highest sequence number in one millisecond.
    pub const fn max_sequence(&self) -> u64 {
        (1_u64 << self.sequence_bits) - 1
    }

    const fn timestamp_shift(&self) -> u8 {
        self.worker_id_bits + self.sequence_bits
    }

    const fn worker_id_shift(&self) -> u8 {
        self.sequence_bits
    }

    fn validate(&self) -> CatgaResult<()> {
        if u16::from(self.timestamp_bits)
            + u16::from(self.worker_id_bits)
            + u16::from(self.sequence_bits)
            != 63
        {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "Snowflake timestamp, worker, and sequence bits must total 63",
            ));
        }
        if !(30..=50).contains(&self.timestamp_bits)
            || self.worker_id_bits > 20
            || self.sequence_bits > 20
        {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "Snowflake bit allocation is outside the supported ranges",
            ));
        }
        Ok(())
    }
}

impl Default for SnowflakeLayout {
    fn default() -> Self {
        Self {
            timestamp_bits: 44,
            worker_id_bits: 8,
            sequence_bits: 11,
            epoch_millis: DEFAULT_EPOCH_MILLIS,
        }
    }
}

/// Parsed Snowflake fields without allocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IdMetadata {
    timestamp_millis: u64,
    worker_id: u32,
    sequence: u64,
}

impl IdMetadata {
    /// Returns the Unix-millisecond generation timestamp.
    pub const fn timestamp_millis(&self) -> u64 {
        self.timestamp_millis
    }

    /// Returns the worker identifier.
    pub const fn worker_id(&self) -> u32 {
        self.worker_id
    }

    /// Returns the per-millisecond sequence number.
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }
}

/// Produces unique distributed IDs and decodes their metadata.
pub trait DistributedIdGenerator: Send + Sync {
    /// Generates the next positive 63-bit ID.
    fn next_id(&self) -> CatgaResult<u64>;

    /// Fills a caller-provided buffer with unique IDs.
    fn fill(&self, destination: &mut [u64]) -> CatgaResult<()>;

    /// Generates an ID and writes its decimal representation into `destination` without a
    /// formatting allocation.
    ///
    /// Returns `Ok(None)` when `destination` cannot hold the generated representation. The ID
    /// has already been reserved in that case, matching a fallible span-format operation: retry
    /// with a larger buffer produces a later, still-unique ID. The written bytes are ASCII digits
    /// and therefore valid UTF-8.
    fn try_write_next_id(&self, destination: &mut [u8]) -> CatgaResult<Option<usize>> {
        Ok(write_decimal_u64(self.next_id()?, destination))
    }

    /// Decodes the timestamp, worker, and sequence components of an ID.
    fn parse(&self, id: u64) -> IdMetadata;
}

fn write_decimal_u64(mut value: u64, destination: &mut [u8]) -> Option<usize> {
    let mut digits = 1;
    let mut remaining = value;
    while remaining >= 10 {
        remaining /= 10;
        digits += 1;
    }
    if destination.len() < digits {
        return None;
    }
    for index in (0..digits).rev() {
        destination[index] = b'0' + (value % 10) as u8;
        value /= 10;
    }
    Some(digits)
}

/// A one-atomic-word, CAS-based Snowflake generator.
pub struct SnowflakeIdGenerator {
    worker_id: u32,
    layout: SnowflakeLayout,
    state: AtomicU64,
}

struct IdReservation {
    timestamp_offset: u64,
    start_sequence: u64,
    count: usize,
}

impl SnowflakeIdGenerator {
    /// Creates a generator for one worker and validated bit layout.
    pub fn new(worker_id: u32, layout: SnowflakeLayout) -> CatgaResult<Self> {
        layout.validate()?;
        if worker_id > layout.max_worker_id() {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "Snowflake worker identifier exceeds the configured layout",
            ));
        }
        Ok(Self {
            worker_id,
            layout,
            state: AtomicU64::new(0),
        })
    }

    /// Returns this generator's bit layout.
    pub const fn layout(&self) -> SnowflakeLayout {
        self.layout
    }

    fn now_offset(&self) -> CatgaResult<u64> {
        let now: u64 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| CatgaError::new(ErrorCode::Internal, "system time predates Unix epoch"))?
            .as_millis()
            .try_into()
            .map_err(|_| {
                CatgaError::new(ErrorCode::Internal, "system milliseconds overflow u64")
            })?;
        now.checked_sub(self.layout.epoch_millis).ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Transient,
                "Snowflake clock precedes the configured epoch",
            )
        })
    }

    fn wait_for_next_millis(&self, last_offset: u64) -> CatgaResult<u64> {
        loop {
            let offset = self.now_offset()?;
            if offset > last_offset {
                return Ok(offset);
            }
            spin_loop();
        }
    }

    /// Atomically reserves up to `requested` sequence values at one logical millisecond.
    ///
    /// `None` means the active millisecond has no remaining sequence values, so callers must
    /// wait for a later timestamp before trying again. Keeping reservation private preserves the
    /// simple [`DistributedIdGenerator`] interface while allowing `fill` to amortize CAS work.
    fn reserve_at(
        &self,
        timestamp_offset: u64,
        requested: usize,
    ) -> CatgaResult<Option<IdReservation>> {
        if requested == 0 {
            return Ok(None);
        }
        if timestamp_offset >= (1_u64 << self.layout.timestamp_bits) {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "Snowflake timestamp exceeds the configured layout lifetime",
            ));
        }

        loop {
            let current = self.state.load(Ordering::Acquire);
            let (reserved_timestamp, start_sequence) = if current & INITIALIZED == 0 {
                (timestamp_offset, 0)
            } else {
                let packed = current & !INITIALIZED;
                let last_timestamp = packed >> self.layout.sequence_bits;
                let last_sequence = packed & self.layout.max_sequence();
                if timestamp_offset < last_timestamp {
                    return Err(CatgaError::new(
                        ErrorCode::Transient,
                        "Snowflake clock moved backwards",
                    ));
                }
                if timestamp_offset > last_timestamp {
                    (timestamp_offset, 0)
                } else if last_sequence < self.layout.max_sequence() {
                    (last_timestamp, last_sequence + 1)
                } else {
                    return Ok(None);
                }
            };
            let available = self.layout.max_sequence() - start_sequence + 1;
            let count = requested.min(usize::try_from(available).unwrap_or(usize::MAX));
            let end_sequence = start_sequence + u64::try_from(count).unwrap_or(available) - 1;
            let next =
                INITIALIZED | (reserved_timestamp << self.layout.sequence_bits) | end_sequence;
            if self
                .state
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(Some(IdReservation {
                    timestamp_offset: reserved_timestamp,
                    start_sequence,
                    count,
                }));
            }
            spin_loop();
        }
    }
}

impl DistributedIdGenerator for SnowflakeIdGenerator {
    fn next_id(&self) -> CatgaResult<u64> {
        let mut id = 0;
        self.fill(std::slice::from_mut(&mut id))?;
        Ok(id)
    }

    fn fill(&self, destination: &mut [u64]) -> CatgaResult<()> {
        let mut written = 0;
        let mut timestamp_offset = self.now_offset()?;
        while written < destination.len() {
            let remaining = destination.len() - written;
            let Some(reservation) = self.reserve_at(timestamp_offset, remaining)? else {
                timestamp_offset = self.wait_for_next_millis(timestamp_offset)?;
                continue;
            };
            let base = (reservation.timestamp_offset << self.layout.timestamp_shift())
                | (u64::from(self.worker_id) << self.layout.worker_id_shift());
            for (offset, id) in destination[written..written + reservation.count]
                .iter_mut()
                .enumerate()
            {
                *id = base | (reservation.start_sequence + offset as u64);
            }
            written += reservation.count;
            timestamp_offset = self.now_offset()?;
        }
        Ok(())
    }

    fn parse(&self, id: u64) -> IdMetadata {
        IdMetadata {
            timestamp_millis: (id >> self.layout.timestamp_shift()) + self.layout.epoch_millis,
            worker_id: ((id >> self.layout.worker_id_shift())
                & u64::from(self.layout.max_worker_id())) as u32,
            sequence: id & self.layout.max_sequence(),
        }
    }
}
