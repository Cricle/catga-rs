//! Benchmarks for SQL Flow Store operations
//!
//! These benchmarks test the pure overhead of the store layer without database I/O.
//! For full benchmarks with actual database operations, see the integration tests.

#![feature(test)]

extern crate test;

use catga_core::flow::FlowState;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// Benchmark: FlowState::new() creation
#[bench]
fn bench_flow_state_new(b: &mut test::Bencher) {
    b.iter(|| {
        let state = FlowState::new("flow-1", "checkout", b"test".to_vec(), "owner-1");
        test::black_box(&state);
    });
}

// Benchmark: FlowState clone
#[bench]
fn bench_flow_state_clone(b: &mut test::Bencher) {
    let state = FlowState::new("flow-1", "checkout", b"test".to_vec(), "owner-1");
    b.iter(|| {
        test::black_box(state.clone());
    });
}

// Benchmark: FlowState struct size
#[bench]
fn bench_flow_state_sizeof(b: &mut test::Bencher) {
    let state = FlowState::new("flow-1", "checkout", b"test".to_vec(), "owner-1");
    b.iter(|| {
        test::black_box(std::mem::size_of_val(&state));
    });
}

// Benchmark: FlowState id accessor
#[bench]
fn bench_flow_state_id(b: &mut test::Bencher) {
    let state = FlowState::new("flow-1", "checkout", b"test".to_vec(), "owner-1");
    b.iter(|| {
        test::black_box(state.id());
    });
}

// Benchmark: FlowState status transition
#[bench]
fn bench_flow_state_status_transition(b: &mut test::Bencher) {
    let state = FlowState::new("flow-1", "checkout", b"test".to_vec(), "owner-1");
    b.iter(|| {
        test::black_box(state.clone().running());
    });
}

// Benchmark: FlowState with large data
#[bench]
fn bench_flow_state_large_data(b: &mut test::Bencher) {
    let large_data = vec![0u8; 4096];
    b.iter(|| {
        let state = FlowState::new("flow-1", "checkout", large_data.clone(), "owner-1");
        test::black_box(&state);
    });
}

// Benchmark: UUID v4 generation (used in flow stores)
#[bench]
fn bench_uuid_v4_generation(b: &mut test::Bencher) {
    b.iter(|| {
        test::black_box(uuid::Uuid::new_v4());
    });
}

// Benchmark: SHA256 hash computation (used for identity keys)
#[bench]
fn bench_sha256_computation(b: &mut test::Bencher) {
    use sha2::{Sha256, Digest};
    let data = b"test-flow-data-for-hashing";
    b.iter(|| {
        let mut hasher = Sha256::new();
        hasher.update(data);
        test::black_box(hasher.clone().finalize());
    });
}

// Benchmark: SystemTime duration calculation
#[bench]
fn bench_system_time_duration(b: &mut test::Bencher) {
    let now = SystemTime::now();
    b.iter(|| {
        let duration = now.elapsed().unwrap_or_default();
        test::black_box(duration);
    });
}

// Benchmark: Timestamp to/from Unix epoch
#[bench]
fn bench_unix_epoch_conversion(b: &mut test::Bencher) {
    let now = SystemTime::now();
    let epoch = now.duration_since(UNIX_EPOCH).unwrap();
    b.iter(|| {
        let ts = epoch.as_millis() as u64;
        test::black_box(ts);
    });
}

// Benchmark: Duration construction
#[bench]
fn bench_duration_construction(b: &mut test::Bencher) {
    b.iter(|| {
        let dur = Duration::from_secs(300) + Duration::from_millis(500);
        test::black_box(dur);
    });
}

// Benchmark: FlowState serialize
#[bench]
fn bench_flow_state_serialize(b: &mut test::Bencher) {
    use catga_core::MemoryPackSerializer;
    let state = FlowState::new("flow-1", "checkout", b"test".to_vec(), "owner-1");
    b.iter(|| {
        let bytes = MemoryPackSerializer::serialize(&state).unwrap();
        test::black_box(&bytes);
    });
}

// Benchmark: FlowState deserialize
#[bench]
fn bench_flow_state_deserialize(b: &mut test::Bencher) {
    use catga_core::MemoryPackSerializer;
    let state = FlowState::new("flow-1", "checkout", b"test".to_vec(), "owner-1");
    let bytes = MemoryPackSerializer::serialize(&state).unwrap();
    b.iter(|| {
        let restored: FlowState = MemoryPackSerializer::deserialize(&bytes).unwrap();
        test::black_box(restored);
    });
}
