//! Compression limit and malformed-frame integration tests.

use catga_core::{
    CatgaResult, CompressionAlgorithm, CompressionStats, ErrorCode, compress, compress_into,
    compress_to_slice, decompress, decompress_limited, is_compressed,
};

const COMPRESSED_ALGORITHMS: [CompressionAlgorithm; 3] = [
    CompressionAlgorithm::Gzip,
    CompressionAlgorithm::Brotli,
    CompressionAlgorithm::Deflate,
];

#[test]
fn framed_algorithms_round_trip_through_owned_reusable_and_fixed_outputs() -> CatgaResult<()> {
    let payload =
        b"compression contract payload; compression contract payload; compression contract payload";

    for algorithm in COMPRESSED_ALGORITHMS {
        let owned = compress(payload, algorithm)?;
        assert!(is_compressed(&owned));
        assert_eq!(decompress(&owned)?, payload);

        let mut reusable = vec![0xFF; 32];
        compress_into(payload, algorithm, &mut reusable)?;
        assert_eq!(decompress(&reusable)?, payload);

        let mut fixed = [0_u8; 1_024];
        let written = compress_to_slice(payload, algorithm, &mut fixed)?;
        assert!(is_compressed(&fixed[..written]));
        assert_eq!(decompress(&fixed[..written])?, payload);
    }
    Ok(())
}

#[test]
fn raw_and_explicit_none_payloads_preserve_wire_contracts() -> CatgaResult<()> {
    let raw = b"plain payload";
    let raw_encoded = compress(raw, CompressionAlgorithm::None)?;
    assert_eq!(raw_encoded, raw);
    assert!(!is_compressed(&raw_encoded));
    assert_eq!(decompress_limited(raw, raw.len())?, raw);

    let magic_prefixed = b"CTGA application bytes";
    let framed = compress(magic_prefixed, CompressionAlgorithm::None)?;
    assert!(!is_compressed(&framed));
    assert_eq!(decompress(&framed)?, magic_prefixed);

    let mut fixed = [0_u8; 64];
    let written = compress_to_slice(magic_prefixed, CompressionAlgorithm::None, &mut fixed)?;
    assert_eq!(decompress(&fixed[..written])?, magic_prefixed);
    Ok(())
}

#[test]
fn compression_rejects_malformed_frames_and_preserves_failed_output_boundaries() {
    assert!(matches!(
        CompressionAlgorithm::try_from(9),
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert!(matches!(
        decompress_limited(b"raw", 2),
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert!(matches!(
        decompress(b"CTGA"),
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert!(matches!(
        decompress(b"CTGA\x01\x09\x00\x00\x00\x00"),
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert!(matches!(
        decompress(b"CTGA\x01\x00\x02\x00\x00\x00x"),
        Err(error) if error.code() == ErrorCode::Validation
    ));

    let mut raw_output = [0xA5; 2];
    assert!(matches!(
        compress_to_slice(b"raw", CompressionAlgorithm::None, &mut raw_output),
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert_eq!(raw_output, [0xA5; 2]);

    let mut compressed_output = [0xA5; 10];
    assert!(matches!(
        compress_to_slice(
            b"payload that cannot fit after its compression frame header",
            CompressionAlgorithm::Gzip,
            &mut compressed_output,
        ),
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert_eq!(compressed_output, [0; 10]);
}

#[test]
fn compression_statistics_report_signed_savings_and_empty_ratio() {
    let saved = CompressionStats::new(100, 40);
    assert_eq!(saved.original_bytes(), 100);
    assert_eq!(saved.compressed_bytes(), 40);
    assert_eq!(saved.saved_bytes(), 60);
    assert!((saved.ratio() - 0.4).abs() < f64::EPSILON);

    let expanded = CompressionStats::new(4, 10);
    assert_eq!(expanded.saved_bytes(), -6);
    assert_eq!(CompressionStats::new(0, 10).ratio(), 1.0);
}
