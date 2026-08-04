//! Benchmarks for message types and type IDs

#![feature(test)]

extern crate test;

use catga_core::{Message, MessageTypeId};

// Define test message types
struct PingTypeId;
impl MessageTypeId for PingTypeId {
    const NAME: &'static str = "Ping";
}

#[derive(Clone)]
struct Ping;
impl Message for Ping {}

struct QueryTypeId;
impl MessageTypeId for QueryTypeId {
    const NAME: &'static str = "Query";
}

#[derive(Clone)]
struct Query;
impl Message for Query {}

// Benchmark: Message creation (empty struct)
#[bench]
fn bench_message_creation_empty(b: &mut test::Bencher) {
    b.iter(|| {
        let msg = Ping;
        test::black_box(msg);
    });
}

// Benchmark: Message clone (empty)
#[bench]
fn bench_message_clone_empty(b: &mut test::Bencher) {
    let msg = Ping;
    b.iter(|| {
        test::black_box(msg.clone());
    });
}

// Benchmark: TypeId lookup
#[bench]
fn bench_type_id_of(b: &mut test::Bencher) {
    b.iter(|| {
        test::black_box(std::any::TypeId::of::<Ping>());
    });
}

// Benchmark: MessageTypeId name access
#[bench]
fn bench_message_type_id_name(b: &mut test::Bencher) {
    b.iter(|| {
        test::black_box(PingTypeId::NAME);
    });
}

// Benchmark: MessageTypeId name comparison
#[bench]
fn bench_message_type_id_compare(b: &mut test::Bencher) {
    b.iter(|| {
        test::black_box(PingTypeId::NAME == "Ping");
    });
}

// Benchmark: MessageTypeId name comparison (different name)
#[bench]
fn bench_message_type_id_compare_miss(b: &mut test::Bencher) {
    b.iter(|| {
        test::black_box(PingTypeId::NAME == "Query");
    });
}

// Benchmark: Multiple type comparisons in sequence
#[bench]
fn bench_message_type_id_multiple_compares(b: &mut test::Bencher) {
    b.iter(|| {
        let name = QueryTypeId::NAME;
        test::black_box(name == "Ping" || name == "Query" || name == "Command");
    });
}
