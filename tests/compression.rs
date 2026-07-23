use catga_core::{
    CompressionAlgorithm, compress, compress_into, decompress, decompress_limited, is_compressed,
};

#[test]
fn compression_round_trips_each_supported_algorithm_without_reallocating_a_reused_output() {
    let payload = vec![b'c'; 4_096];
    for algorithm in [
        CompressionAlgorithm::Gzip,
        CompressionAlgorithm::Brotli,
        CompressionAlgorithm::Deflate,
    ] {
        let mut output = Vec::with_capacity(8_192);
        let capacity = output.capacity();

        compress_into(&payload, algorithm, &mut output).unwrap();

        assert!(is_compressed(&output));
        assert_eq!(output.capacity(), capacity);
        assert_eq!(decompress(&output).unwrap(), payload);
    }
}

#[test]
fn no_compression_keeps_the_wire_payload_and_declared_expansions_are_bounded() {
    let payload = b"small message";

    let encoded = compress(payload, CompressionAlgorithm::None).unwrap();

    assert_eq!(encoded, payload);
    assert!(!is_compressed(&encoded));
    assert_eq!(decompress(&encoded).unwrap(), payload);
    let oversized_header = [CompressionAlgorithm::Gzip as u8, 17, 0, 0, 0];
    assert!(decompress_limited(&oversized_header, 16).is_err());
}
