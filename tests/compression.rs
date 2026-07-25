use catga_core::{
    CompressionAlgorithm, compress, compress_into, compress_to_slice, decompress,
    decompress_limited, is_compressed,
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
    let oversized_header = [
        b'C',
        b'T',
        b'G',
        b'A',
        1,
        CompressionAlgorithm::Gzip as u8,
        17,
        0,
        0,
        0,
    ];
    assert!(decompress_limited(&oversized_header, 16).is_err());
}

#[test]
fn raw_payloads_with_algorithm_prefixes_are_never_mistaken_for_frames() {
    for prefix in 0_u8..=3 {
        let payload = [prefix, 10, 20, 30, 40, 50];
        let encoded = compress(&payload, CompressionAlgorithm::None).expect("raw encode");
        assert!(!is_compressed(&encoded));
        assert_eq!(decompress(&encoded).expect("raw decode"), payload);
    }
}

#[test]
fn raw_payload_with_reserved_magic_round_trips_through_a_none_envelope() {
    let payload = b"CTGA raw payload";
    let encoded = compress(payload, CompressionAlgorithm::None).expect("raw encode");
    assert_ne!(encoded, payload);
    assert!(!is_compressed(&encoded));
    assert_eq!(decompress(&encoded).expect("raw decode"), payload);
}

#[test]
fn fixed_slice_escapes_reserved_magic_for_none_like_the_vector_encoder() {
    let payload = b"CTGA fixed slice";
    let expected = compress(payload, CompressionAlgorithm::None).expect("vector encode");
    let mut output = [0_u8; 64];
    let written =
        compress_to_slice(payload, CompressionAlgorithm::None, &mut output).expect("fixed encode");
    assert_eq!(&output[..written], expected);
    assert!(!is_compressed(&output[..written]));
    assert_eq!(decompress(&output[..written]).expect("decode"), payload);
}

#[test]
fn compression_to_slice_returns_the_exact_framed_length_for_every_algorithm() {
    let payload = vec![b's'; 4_096];
    for algorithm in [
        CompressionAlgorithm::Gzip,
        CompressionAlgorithm::Brotli,
        CompressionAlgorithm::Deflate,
    ] {
        let mut output = [0_u8; 8_192];
        let written = compress_to_slice(&payload, algorithm, &mut output)
            .expect("fixed output has enough space");
        let expected = compress(&payload, algorithm).expect("existing encoder succeeds");

        assert_eq!(written, expected.len());
        assert_eq!(&output[..written], expected);
        assert!(is_compressed(&output[..written]));
        assert_eq!(
            decompress(&output[..written]).expect("frame decodes"),
            payload
        );
    }
}

#[test]
fn compression_to_slice_copies_raw_payload_exactly_without_mutating_an_insufficient_buffer() {
    let payload = b"raw payload";
    let mut output = [0xA5_u8; 3];

    let error = compress_to_slice(payload, CompressionAlgorithm::None, &mut output)
        .expect_err("raw output capacity is insufficient");

    assert_eq!(error.code(), catga_core::ErrorCode::Validation);
    assert_eq!(output, [0xA5; 3]);

    let mut exact = [0_u8; 11];
    let written = compress_to_slice(payload, CompressionAlgorithm::None, &mut exact)
        .expect("exact raw capacity succeeds");
    assert_eq!(written, payload.len());
    assert_eq!(&exact[..written], payload);
}

#[test]
fn compression_to_slice_clears_a_partial_compressed_frame_when_capacity_is_insufficient() {
    let payload = vec![b'x'; 4_096];
    let mut output = [0xA5_u8; 8];

    let error = compress_to_slice(&payload, CompressionAlgorithm::Gzip, &mut output)
        .expect_err("compressed output capacity is insufficient");

    assert_eq!(error.code(), catga_core::ErrorCode::Validation);
    assert!(!is_compressed(&output));
}
