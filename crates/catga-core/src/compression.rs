//! Bounded transport payload compression with a compact self-describing frame.

use std::io::{Read, Write};

use brotli::{CompressorWriter, Decompressor};
use flate2::{
    Compression,
    read::{DeflateDecoder, GzDecoder},
    write::{DeflateEncoder, GzEncoder},
};

use crate::{CatgaError, CatgaResult, ErrorCode};

const HEADER_LEN: usize = 5;

/// Maximum uncompressed payload accepted by [`decompress`].
pub const DEFAULT_MAX_DECOMPRESSED_BYTES: usize = 64 * 1024 * 1024;

/// The wire compression algorithm stored in a compressed payload header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum CompressionAlgorithm {
    /// No header or transformation is added.
    None = 0,
    /// RFC 1952 gzip framing.
    Gzip = 1,
    /// Brotli compression.
    Brotli = 2,
    /// RFC 1951 deflate compression.
    Deflate = 3,
}

impl TryFrom<u8> for CompressionAlgorithm {
    type Error = CatgaError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::Gzip),
            2 => Ok(Self::Brotli),
            3 => Ok(Self::Deflate),
            _ => Err(CatgaError::new(
                ErrorCode::Validation,
                "compressed payload has an unknown algorithm",
            )),
        }
    }
}

/// Compression-size measurements suitable for metrics collection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompressionStats {
    original_bytes: usize,
    compressed_bytes: usize,
}

impl CompressionStats {
    /// Creates statistics from an original and compressed payload size.
    pub const fn new(original_bytes: usize, compressed_bytes: usize) -> Self {
        Self {
            original_bytes,
            compressed_bytes,
        }
    }

    /// Returns the original payload length.
    pub const fn original_bytes(self) -> usize {
        self.original_bytes
    }

    /// Returns the encoded payload length.
    pub const fn compressed_bytes(self) -> usize {
        self.compressed_bytes
    }

    /// Returns bytes saved, which is negative when compression expands the payload.
    pub fn saved_bytes(self) -> isize {
        isize::try_from(self.original_bytes).unwrap_or(isize::MAX)
            - isize::try_from(self.compressed_bytes).unwrap_or(isize::MAX)
    }

    /// Returns the encoded/original size ratio, or one for an empty input.
    pub fn ratio(self) -> f64 {
        if self.original_bytes == 0 {
            1.0
        } else {
            self.compressed_bytes as f64 / self.original_bytes as f64
        }
    }
}

/// Compresses a payload into a freshly allocated vector.
pub fn compress(data: &[u8], algorithm: CompressionAlgorithm) -> CatgaResult<Vec<u8>> {
    let mut output = Vec::with_capacity(data.len().saturating_add(HEADER_LEN));
    compress_into(data, algorithm, &mut output)?;
    Ok(output)
}

/// Compresses a payload into a reusable vector, clearing its previous contents first.
pub fn compress_into(
    data: &[u8],
    algorithm: CompressionAlgorithm,
    output: &mut Vec<u8>,
) -> CatgaResult<()> {
    output.clear();
    if algorithm == CompressionAlgorithm::None || data.is_empty() {
        output.extend_from_slice(data);
        return Ok(());
    }
    let original_length = u32::try_from(data.len()).map_err(|_| {
        CatgaError::new(
            ErrorCode::Validation,
            "payload exceeds the compression frame length limit",
        )
    })?;
    output.push(algorithm as u8);
    output.extend_from_slice(&original_length.to_le_bytes());
    match algorithm {
        CompressionAlgorithm::None => unreachable!("none returns before framing"),
        CompressionAlgorithm::Gzip => {
            let mut writer = GzEncoder::new(output, Compression::fast());
            writer.write_all(data).map_err(write_error)?;
            writer.finish().map_err(write_error)?;
        }
        CompressionAlgorithm::Brotli => {
            let mut writer = CompressorWriter::new(output, 4_096, 4, 22);
            writer.write_all(data).map_err(write_error)?;
            writer.flush().map_err(write_error)?;
        }
        CompressionAlgorithm::Deflate => {
            let mut writer = DeflateEncoder::new(output, Compression::fast());
            writer.write_all(data).map_err(write_error)?;
            writer.finish().map_err(write_error)?;
        }
    }
    Ok(())
}

/// Decompresses a framed payload with [`DEFAULT_MAX_DECOMPRESSED_BYTES`] as its allocation cap.
pub fn decompress(data: &[u8]) -> CatgaResult<Vec<u8>> {
    decompress_limited(data, DEFAULT_MAX_DECOMPRESSED_BYTES)
}

/// Decompresses a payload while rejecting a frame whose declared or actual output exceeds `limit`.
pub fn decompress_limited(data: &[u8], limit: usize) -> CatgaResult<Vec<u8>> {
    if data.len() < HEADER_LEN {
        return copy_raw_limited(data, limit);
    }
    let Ok(algorithm) = CompressionAlgorithm::try_from(data[0]) else {
        return copy_raw_limited(data, limit);
    };
    if algorithm == CompressionAlgorithm::None {
        return copy_raw_limited(&data[HEADER_LEN..], limit);
    }
    let declared = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
    if declared > limit {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "compressed payload exceeds the decompression limit",
        ));
    }
    let mut output = Vec::new();
    output.try_reserve_exact(declared).map_err(|_| {
        CatgaError::new(
            ErrorCode::Internal,
            "unable to reserve decompression output buffer",
        )
    })?;
    let payload = &data[HEADER_LEN..];
    match algorithm {
        CompressionAlgorithm::None => unreachable!("none returns before decompression"),
        CompressionAlgorithm::Gzip => {
            read_framed(GzDecoder::new(payload), &mut output, declared, limit)?;
        }
        CompressionAlgorithm::Brotli => {
            read_framed(
                Decompressor::new(payload, 4_096),
                &mut output,
                declared,
                limit,
            )?;
        }
        CompressionAlgorithm::Deflate => {
            read_framed(DeflateDecoder::new(payload), &mut output, declared, limit)?;
        }
    }
    Ok(output)
}

/// Returns whether bytes begin with a supported compressed-payload frame.
pub fn is_compressed(data: &[u8]) -> bool {
    data.len() >= HEADER_LEN
        && CompressionAlgorithm::try_from(data[0])
            .is_ok_and(|algorithm| algorithm != CompressionAlgorithm::None)
}

fn copy_raw_limited(data: &[u8], limit: usize) -> CatgaResult<Vec<u8>> {
    if data.len() > limit {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "raw payload exceeds the decompression limit",
        ));
    }
    Ok(data.to_vec())
}

fn read_framed<R: Read>(
    reader: R,
    output: &mut Vec<u8>,
    declared: usize,
    limit: usize,
) -> CatgaResult<()> {
    reader
        .take(u64::try_from(limit.saturating_add(1)).unwrap_or(u64::MAX))
        .read_to_end(output)
        .map_err(read_error)?;
    if output.len() > limit || output.len() != declared {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "compressed payload has an invalid decoded length",
        ));
    }
    Ok(())
}

fn write_error(error: std::io::Error) -> CatgaError {
    CatgaError::new(ErrorCode::Internal, error.to_string())
}

fn read_error(error: std::io::Error) -> CatgaError {
    CatgaError::new(ErrorCode::Validation, error.to_string())
}
