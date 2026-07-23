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

    /// Decodes the timestamp, worker, and sequence components of an ID.
    fn parse(&self, id: u64) -> IdMetadata;
}

/// A one-atomic-word, CAS-based Snowflake generator.
pub struct SnowflakeIdGenerator {
    worker_id: u32,
    layout: SnowflakeLayout,
    state: AtomicU64,
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

    fn compose(&self, timestamp_offset: u64, sequence: u64) -> u64 {
        (timestamp_offset << self.layout.timestamp_shift())
            | (u64::from(self.worker_id) << self.layout.worker_id_shift())
            | sequence
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
}

impl DistributedIdGenerator for SnowflakeIdGenerator {
    fn next_id(&self) -> CatgaResult<u64> {
        loop {
            let current = self.state.load(Ordering::Acquire);
            let now = self.now_offset()?;
            let (timestamp, sequence) = if current & INITIALIZED == 0 {
                (now, 0)
            } else {
                let packed = current & !INITIALIZED;
                let last_timestamp = packed >> self.layout.sequence_bits;
                let last_sequence = packed & self.layout.max_sequence();
                if now < last_timestamp {
                    return Err(CatgaError::new(
                        ErrorCode::Transient,
                        "Snowflake clock moved backwards",
                    ));
                }
                if now > last_timestamp {
                    (now, 0)
                } else if last_sequence < self.layout.max_sequence() {
                    (last_timestamp, last_sequence + 1)
                } else {
                    (self.wait_for_next_millis(last_timestamp)?, 0)
                }
            };
            if timestamp >= (1_u64 << self.layout.timestamp_bits) {
                return Err(CatgaError::new(
                    ErrorCode::Validation,
                    "Snowflake timestamp exceeds the configured layout lifetime",
                ));
            }
            let next = INITIALIZED | (timestamp << self.layout.sequence_bits) | sequence;
            if self
                .state
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(self.compose(timestamp, sequence));
            }
            spin_loop();
        }
    }

    fn fill(&self, destination: &mut [u64]) -> CatgaResult<()> {
        for id in destination {
            *id = self.next_id()?;
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
