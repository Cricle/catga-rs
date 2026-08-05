#![allow(missing_docs)]

//! Redis transport benchmarks.
//!
//! These benchmarks measure serialization overhead that can be measured without a Redis server.
//! Run with: `cargo bench -p catga-redis --bench redis_throughput`

use catga_core::{
    Envelope, EnvelopeCodec, MessageMetadata, QualityOfService, codec::memorypack::MemoryPackCodec,
};
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};

fn envelope_encoding(c: &mut Criterion) {
    let codec = MemoryPackCodec::default();

    // Create test envelopes with various payload sizes
    let payloads: Vec<(usize, Vec<u8>)> = vec![
        (16, vec![0u8; 16]),
        (64, vec![0u8; 64]),
        (256, vec![0u8; 256]),
        (1024, vec![0u8; 1024]),
        (4096, vec![0u8; 4096]),
    ];

    let mut group = c.benchmark_group("envelope_encoding");

    for (size, payload) in payloads {
        let metadata =
            MessageMetadata::new(1, None).with_quality_of_service(QualityOfService::AtLeastOnce);
        let envelope = Envelope::new(1, "test.message", payload.clone(), metadata);

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _size| {
            b.iter(|| {
                let encoded = codec
                    .encode(black_box(&envelope))
                    .expect("benchmark should not fail");
                black_box(encoded)
            });
        });
    }

    group.finish();
}

fn envelope_decoding(c: &mut Criterion) {
    let codec = MemoryPackCodec::default();

    // Pre-encode test envelopes
    let test_cases: Vec<(usize, Vec<u8>)> = vec![
        (16, {
            let metadata = MessageMetadata::new(1, None);
            let env = Envelope::new(1, "test", vec![0u8; 16], metadata);
            codec.encode(&env).expect("benchmark should not fail")
        }),
        (64, {
            let metadata = MessageMetadata::new(1, None);
            let env = Envelope::new(1, "test", vec![0u8; 64], metadata);
            codec.encode(&env).expect("benchmark should not fail")
        }),
        (256, {
            let metadata = MessageMetadata::new(1, None);
            let env = Envelope::new(1, "test", vec![0u8; 256], metadata);
            codec.encode(&env).expect("benchmark should not fail")
        }),
        (1024, {
            let metadata = MessageMetadata::new(1, None);
            let env = Envelope::new(1, "test", vec![0u8; 1024], metadata);
            codec.encode(&env).expect("benchmark should not fail")
        }),
        (4096, {
            let metadata = MessageMetadata::new(1, None);
            let env = Envelope::new(1, "test", vec![0u8; 4096], metadata);
            codec.encode(&env).expect("benchmark should not fail")
        }),
    ];

    let mut group = c.benchmark_group("envelope_decoding");

    for (size, encoded) in test_cases {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _size| {
            b.iter(|| {
                let decoded: Envelope = codec
                    .decode(black_box(&encoded))
                    .expect("benchmark should not fail");
                black_box(decoded)
            });
        });
    }

    group.finish();
}

fn envelope_round_trip(c: &mut Criterion) {
    let codec = MemoryPackCodec::default();

    let payloads: Vec<(usize, Vec<u8>)> = vec![
        (16, vec![0u8; 16]),
        (256, vec![0u8; 256]),
        (4096, vec![0u8; 4096]),
    ];

    let mut group = c.benchmark_group("envelope_round_trip");

    for (size, payload) in payloads {
        let metadata = MessageMetadata::new(1, None);
        let envelope = Envelope::new(1, "test.message", payload.clone(), metadata);

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _size| {
            b.iter(|| {
                let encoded = codec
                    .encode(black_box(&envelope))
                    .expect("benchmark should not fail");
                let decoded: Envelope = codec
                    .decode(black_box(&encoded))
                    .expect("benchmark should not fail");
                black_box(decoded)
            });
        });
    }

    group.finish();
}

fn metadata_construction(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_construction");

    group.bench_function("with_qos", |b| {
        b.iter(|| {
            let meta = MessageMetadata::new(black_box(1), black_box(None))
                .with_quality_of_service(black_box(QualityOfService::AtLeastOnce));
            black_box(meta)
        });
    });

    group.bench_function("basic_new", |b| {
        b.iter(|| {
            let meta = MessageMetadata::new(black_box(1), black_box(None));
            black_box(meta)
        });
    });

    group.finish();
}

/// Measures decode limits overhead for 256-byte payloads.
fn codec_decode_limits(c: &mut Criterion) {
    let codec = MemoryPackCodec::default();

    let mut group = c.benchmark_group("codec_decode_limits");
    group.bench_function("baseline_256", |b| {
        let metadata = MessageMetadata::new(1, None);
        let envelope = Envelope::new(1, "test.message", vec![0u8; 256], metadata);
        let encoded = codec.encode(&envelope).expect("benchmark should not fail");

        b.iter(|| {
            let decoded: Envelope = codec
                .decode(black_box(&encoded))
                .expect("benchmark should not fail");
            black_box(decoded)
        });
    });
    group.finish();
}

fn large_payload_serialization(c: &mut Criterion) {
    let codec = MemoryPackCodec::default();

    // Test larger payloads that stress serialization
    let test_cases: Vec<(usize, Vec<u8>)> = vec![
        (8192, vec![0u8; 8192]),
        (16384, vec![0u8; 16384]),
        (65536, vec![0u8; 65536]),
    ];

    // Encode all test cases upfront
    let encoded_cases: Vec<(usize, Vec<u8>)> = test_cases
        .iter()
        .map(|(size, payload)| {
            let metadata = MessageMetadata::new(1, None);
            let envelope = Envelope::new(1, "test.message", payload.clone(), metadata);
            let encoded = codec.encode(&envelope).expect("benchmark should not fail");
            (*size, encoded)
        })
        .collect();

    {
        let mut encode_group = c.benchmark_group("large_payload_encode");
        for (size, payload) in &test_cases {
            let metadata = MessageMetadata::new(1, None);
            let envelope = Envelope::new(1, "test.message", payload.clone(), metadata);
            let size = *size;

            encode_group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _size| {
                b.iter(|| {
                    let result = codec
                        .encode(black_box(&envelope))
                        .expect("benchmark should not fail");
                    black_box(result)
                });
            });
        }
        encode_group.finish();
    }

    {
        let mut decode_group = c.benchmark_group("large_payload_decode");
        for (size, encoded) in &encoded_cases {
            let size = *size;

            decode_group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _size| {
                b.iter(|| {
                    let result: Envelope = codec
                        .decode(black_box(encoded))
                        .expect("benchmark should not fail");
                    black_box(result)
                });
            });
        }
        decode_group.finish();
    }
}

criterion_group!(
    benches,
    envelope_encoding,
    envelope_decoding,
    envelope_round_trip,
    metadata_construction,
    codec_decode_limits,
    large_payload_serialization,
);
criterion_main!(benches);
