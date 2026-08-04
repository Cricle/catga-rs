//! Benchmarks for Snowflake distributed ID generation

#![feature(test)]

extern crate test;

use catga_core::distributed_id::{DistributedIdGenerator, SnowflakeIdGenerator, SnowflakeLayout};

// Benchmark: Default SnowflakeLayout creation
#[bench]
fn bench_snowflake_layout_default(b: &mut test::Bencher) {
    b.iter(|| {
        let layout = SnowflakeLayout::default();
        test::black_box(&layout);
    });
}

// Benchmark: SnowflakeLayout::new (custom)
#[bench]
fn bench_snowflake_layout_new(b: &mut test::Bencher) {
    b.iter(|| {
        let layout = SnowflakeLayout::new(41, 10, 12, 1_704_067_200_000).unwrap();
        test::black_box(&layout);
    });
}

// Benchmark: SnowflakeLayout accessor methods
#[bench]
fn bench_snowflake_layout_accessors(b: &mut test::Bencher) {
    let layout = SnowflakeLayout::default();
    b.iter(|| {
        test::black_box(layout.timestamp_bits());
        test::black_box(layout.worker_id_bits());
        test::black_box(layout.sequence_bits());
        test::black_box(layout.epoch_millis());
    });
}

// Benchmark: SnowflakeIdGenerator creation
#[bench]
fn bench_snowflake_id_generator_new(b: &mut test::Bencher) {
    b.iter(|| {
        let generator = SnowflakeIdGenerator::new(1, SnowflakeLayout::default()).unwrap();
        test::black_box(generator);
    });
}

// Benchmark: SnowflakeIdGenerator next_id
#[bench]
fn bench_snowflake_id_generator_next(b: &mut test::Bencher) {
    let generator = SnowflakeIdGenerator::new(1, SnowflakeLayout::default()).unwrap();
    b.iter(|| {
        test::black_box(generator.next_id());
    });
}

// Benchmark: SnowflakeIdGenerator next_id (multiple calls)
#[bench]
fn bench_snowflake_id_generator_next_100(b: &mut test::Bencher) {
    let generator = SnowflakeIdGenerator::new(1, SnowflakeLayout::default()).unwrap();
    b.iter(|| {
        for _ in 0..100 {
            test::black_box(generator.next_id());
        }
    });
}

// Benchmark: SnowflakeIdGenerator parse
#[bench]
fn bench_snowflake_id_generator_parse(b: &mut test::Bencher) {
    let generator = SnowflakeIdGenerator::new(1, SnowflakeLayout::default()).unwrap();
    let id = generator.next_id().unwrap();
    b.iter(|| {
        test::black_box(generator.parse(id));
    });
}

// Benchmark: SnowflakeLayout struct size
#[bench]
fn bench_snowflake_layout_sizeof(b: &mut test::Bencher) {
    let layout = SnowflakeLayout::default();
    b.iter(|| {
        test::black_box(std::mem::size_of_val(&layout));
    });
}

// Benchmark: SnowflakeIdGenerator struct size
#[bench]
fn bench_snowflake_id_generator_sizeof(b: &mut test::Bencher) {
    let generator = SnowflakeIdGenerator::new(1, SnowflakeLayout::default()).unwrap();
    b.iter(|| {
        test::black_box(std::mem::size_of_val(&generator));
    });
}
