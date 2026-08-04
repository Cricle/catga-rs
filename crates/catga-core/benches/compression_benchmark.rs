//! Benchmarks for compression/decompression

#![feature(test)]

extern crate test;

use catga_core::{CompressionAlgorithm, CompressionStats, compress, decompress};

fn generate_test_data(size: usize) -> Vec<u8> {
    let pattern: Vec<u8> = b"The quick brown fox jumps over the lazy dog. ".to_vec();
    let mut data = Vec::with_capacity(size);
    while data.len() < size {
        data.extend_from_slice(&pattern);
    }
    data.truncate(size);
    data
}

// Benchmark: compress with Gzip (small data)
#[bench]
fn bench_compress_gzip_small(b: &mut test::Bencher) {
    let data = generate_test_data(100);
    b.iter(|| {
        test::black_box(compress(&data, CompressionAlgorithm::Gzip).unwrap());
    });
}

// Benchmark: compress with Gzip (medium data)
#[bench]
fn bench_compress_gzip_medium(b: &mut test::Bencher) {
    let data = generate_test_data(1024);
    b.iter(|| {
        test::black_box(compress(&data, CompressionAlgorithm::Gzip).unwrap());
    });
}

// Benchmark: compress with Gzip (large data)
#[bench]
fn bench_compress_gzip_large(b: &mut test::Bencher) {
    let data = generate_test_data(10 * 1024);
    b.iter(|| {
        test::black_box(compress(&data, CompressionAlgorithm::Gzip).unwrap());
    });
}

// Benchmark: compress with Brotli (small data)
#[bench]
fn bench_compress_brotli_small(b: &mut test::Bencher) {
    let data = generate_test_data(100);
    b.iter(|| {
        test::black_box(compress(&data, CompressionAlgorithm::Brotli).unwrap());
    });
}

// Benchmark: compress with Brotli (medium data)
#[bench]
fn bench_compress_brotli_medium(b: &mut test::Bencher) {
    let data = generate_test_data(1024);
    b.iter(|| {
        test::black_box(compress(&data, CompressionAlgorithm::Brotli).unwrap());
    });
}

// Benchmark: compress with Brotli (large data)
#[bench]
fn bench_compress_brotli_large(b: &mut test::Bencher) {
    let data = generate_test_data(10 * 1024);
    b.iter(|| {
        test::black_box(compress(&data, CompressionAlgorithm::Brotli).unwrap());
    });
}

// Benchmark: compress with Deflate (small data)
#[bench]
fn bench_compress_deflate_small(b: &mut test::Bencher) {
    let data = generate_test_data(100);
    b.iter(|| {
        test::black_box(compress(&data, CompressionAlgorithm::Deflate).unwrap());
    });
}

// Benchmark: decompress Gzip
#[bench]
fn bench_decompress_gzip(b: &mut test::Bencher) {
    let data = generate_test_data(1024);
    let compressed = compress(&data, CompressionAlgorithm::Gzip).unwrap();
    b.iter(|| {
        test::black_box(decompress(&compressed).unwrap());
    });
}

// Benchmark: decompress Brotli
#[bench]
fn bench_decompress_brotli(b: &mut test::Bencher) {
    let data = generate_test_data(1024);
    let compressed = compress(&data, CompressionAlgorithm::Brotli).unwrap();
    b.iter(|| {
        test::black_box(decompress(&compressed).unwrap());
    });
}

// Benchmark: decompress Deflate
#[bench]
fn bench_decompress_deflate(b: &mut test::Bencher) {
    let data = generate_test_data(1024);
    let compressed = compress(&data, CompressionAlgorithm::Deflate).unwrap();
    b.iter(|| {
        test::black_box(decompress(&compressed).unwrap());
    });
}

// Benchmark: CompressionStats creation
#[bench]
fn bench_compression_stats_new(b: &mut test::Bencher) {
    b.iter(|| {
        let stats = CompressionStats::new(1024, 256);
        test::black_box(&stats);
    });
}

// Benchmark: CompressionStats accessor methods
#[bench]
fn bench_compression_stats_accessors(b: &mut test::Bencher) {
    let stats = CompressionStats::new(1024, 256);
    b.iter(|| {
        test::black_box(stats.original_bytes());
        test::black_box(stats.compressed_bytes());
        test::black_box(stats.saved_bytes());
        test::black_box(stats.ratio());
    });
}

// Benchmark: CompressionStats struct size
#[bench]
fn bench_compression_stats_sizeof(b: &mut test::Bencher) {
    let stats = CompressionStats::new(1024, 256);
    b.iter(|| {
        test::black_box(std::mem::size_of_val(&stats));
    });
}
