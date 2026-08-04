//! Benchmarks for MemoryPack codec

#![feature(test)]

extern crate test;

use catga_core::codec::memorypack::MemoryPackSerializer;

// Benchmark: Serialize u32
#[bench]
fn bench_serialize_u32(b: &mut test::Bencher) {
    let value: u32 = 42;
    b.iter(|| {
        test::black_box(MemoryPackSerializer::serialize(&value).unwrap());
    });
}

// Benchmark: Serialize u64
#[bench]
fn bench_serialize_u64(b: &mut test::Bencher) {
    let value: u64 = 42;
    b.iter(|| {
        test::black_box(MemoryPackSerializer::serialize(&value).unwrap());
    });
}

// Benchmark: Serialize i32
#[bench]
fn bench_serialize_i32(b: &mut test::Bencher) {
    let value: i32 = -42;
    b.iter(|| {
        test::black_box(MemoryPackSerializer::serialize(&value).unwrap());
    });
}

// Benchmark: Serialize f64
#[bench]
fn bench_serialize_f64(b: &mut test::Bencher) {
    let value: f64 = 3.14159;
    b.iter(|| {
        test::black_box(MemoryPackSerializer::serialize(&value).unwrap());
    });
}

// Benchmark: Serialize bool
#[bench]
fn bench_serialize_bool(b: &mut test::Bencher) {
    let value: bool = true;
    b.iter(|| {
        test::black_box(MemoryPackSerializer::serialize(&value).unwrap());
    });
}

// Benchmark: Serialize String (small)
#[bench]
fn bench_serialize_string_small(b: &mut test::Bencher) {
    let value = String::from("hello");
    b.iter(|| {
        test::black_box(MemoryPackSerializer::serialize(&value).unwrap());
    });
}

// Benchmark: Serialize String (medium)
#[bench]
fn bench_serialize_string_medium(b: &mut test::Bencher) {
    let value = String::from("hello world this is a test string");
    b.iter(|| {
        test::black_box(MemoryPackSerializer::serialize(&value).unwrap());
    });
}

// Benchmark: Serialize Vec<u8>
#[bench]
fn bench_serialize_vec_u8_1k(b: &mut test::Bencher) {
    let value = vec![0u8; 1024];
    b.iter(|| {
        test::black_box(MemoryPackSerializer::serialize(&value).unwrap());
    });
}

// Benchmark: Serialize Vec<u8> 4k
#[bench]
fn bench_serialize_vec_u8_4k(b: &mut test::Bencher) {
    let value = vec![0u8; 4096];
    b.iter(|| {
        test::black_box(MemoryPackSerializer::serialize(&value).unwrap());
    });
}

// Benchmark: Deserialize u32
#[bench]
fn bench_deserialize_u32(b: &mut test::Bencher) {
    let data = MemoryPackSerializer::serialize(&42u32).unwrap();
    b.iter(|| {
        test::black_box(MemoryPackSerializer::deserialize::<u32>(&data).unwrap());
    });
}

// Benchmark: Deserialize u64
#[bench]
fn bench_deserialize_u64(b: &mut test::Bencher) {
    let data = MemoryPackSerializer::serialize(&42u64).unwrap();
    b.iter(|| {
        test::black_box(MemoryPackSerializer::deserialize::<u64>(&data).unwrap());
    });
}

// Benchmark: Deserialize f64
#[bench]
fn bench_deserialize_f64(b: &mut test::Bencher) {
    let data = MemoryPackSerializer::serialize(&3.14159f64).unwrap();
    b.iter(|| {
        test::black_box(MemoryPackSerializer::deserialize::<f64>(&data).unwrap());
    });
}

// Benchmark: Deserialize String
#[bench]
fn bench_deserialize_string(b: &mut test::Bencher) {
    let data = MemoryPackSerializer::serialize(&String::from("hello world this is a test string")).unwrap();
    b.iter(|| {
        test::black_box(MemoryPackSerializer::deserialize::<String>(&data).unwrap());
    });
}

// Benchmark: Deserialize Vec<u8> 1k
#[bench]
fn bench_deserialize_vec_u8_1k(b: &mut test::Bencher) {
    let data = MemoryPackSerializer::serialize(&vec![0u8; 1024]).unwrap();
    b.iter(|| {
        test::black_box(MemoryPackSerializer::deserialize::<Vec<u8>>(&data).unwrap());
    });
}

// Benchmark: Round-trip u32
#[bench]
fn bench_roundtrip_u32(b: &mut test::Bencher) {
    let value: u32 = 42;
    b.iter(|| {
        let encoded = MemoryPackSerializer::serialize(&value).unwrap();
        test::black_box(MemoryPackSerializer::deserialize::<u32>(&encoded).unwrap());
    });
}

// Benchmark: Round-trip String
#[bench]
fn bench_roundtrip_string(b: &mut test::Bencher) {
    let value = String::from("hello world this is a test string");
    b.iter(|| {
        let encoded = MemoryPackSerializer::serialize(&value).unwrap();
        test::black_box(MemoryPackSerializer::deserialize::<String>(&encoded).unwrap());
    });
}
