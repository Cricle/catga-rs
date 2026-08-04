//! Benchmarks for CatgaError and error handling

#![feature(test)]

extern crate test;

use catga_core::{CatgaError, ErrorCode};

// Benchmark: CatgaError creation
#[bench]
fn bench_error_new(b: &mut test::Bencher) {
    b.iter(|| {
        let _error = CatgaError::new(ErrorCode::Validation, "test error message");
    });
}

// Benchmark: CatgaError creation with details
#[bench]
fn bench_error_new_with_details(b: &mut test::Bencher) {
    b.iter(|| {
        let _error = CatgaError::new(ErrorCode::Validation, "test error message")
            .with_details("additional details here");
    });
}

// Benchmark: CatgaError code access
#[bench]
fn bench_error_code(b: &mut test::Bencher) {
    let error = CatgaError::new(ErrorCode::Internal, "test");
    b.iter(|| {
        test::black_box(error.code());
    });
}

// Benchmark: CatgaError message access
#[bench]
fn bench_error_message(b: &mut test::Bencher) {
    let error = CatgaError::new(ErrorCode::Internal, "test error message");
    b.iter(|| {
        test::black_box(error.message());
    });
}

// Benchmark: CatgaError Clone
#[bench]
fn bench_error_clone(b: &mut test::Bencher) {
    let error = CatgaError::new(ErrorCode::Internal, "test error message");
    b.iter(|| {
        test::black_box(error.clone());
    });
}

// Benchmark: ErrorCode comparison
#[bench]
fn bench_error_code_comparison(b: &mut test::Bencher) {
    let error = CatgaError::new(ErrorCode::Internal, "test");
    b.iter(|| {
        test::black_box(error.code() == ErrorCode::Internal);
    });
}

// Benchmark: CatgaError struct size
#[bench]
fn bench_error_sizeof(b: &mut test::Bencher) {
    let error = CatgaError::new(ErrorCode::Internal, "test");
    b.iter(|| {
        test::black_box(std::mem::size_of_val(&error));
    });
}
