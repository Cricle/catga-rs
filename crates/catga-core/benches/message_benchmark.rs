//! Benchmarks for message types and type IDs

#![feature(test)]

extern crate test;

use catga_core::{Message, MessageTypeId};

// Define test message types
struct PingTypeId;
impl MessageTypeId for PingTypeId { const NAME: &'static str = "Ping"; }

struct Ping;
impl Message for Ping {}

struct QueryTypeId;
impl MessageTypeId for QueryTypeId { const NAME: &'static str = "Query"; }

struct Query;
impl Message for Query {}

// Benchmark: Message struct size
#[bench]
fn bench_message_sizeof_ping(b: &mut test::Bencher) {
    let msg = Ping;
    b.iter(|| {
        test::black_box(std::mem::size_of_val(&msg));
    });
}

// Benchmark: Message struct size (empty struct)
#[bench]
fn bench_message_sizeof_query(b: &mut test::Bencher) {
    let msg = Query;
    b.iter(|| {
        test::black_box(std::mem::size_of_val(&msg));
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
