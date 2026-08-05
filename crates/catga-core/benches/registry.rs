//! Registry creation and lookup benchmarks
//!
//! Run: cargo bench -p catga-core --bench registry -- --noplot

use async_trait::async_trait;
use catga_core::{CatgaResult, Handler, Message, Registry, Request};
use criterion::{Criterion, criterion_group, criterion_main};
use paste::paste;
use std::hint::black_box;

// Generate 100 unique message types
macro_rules! define_types {
    () => {
        define_types!(@gen 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
                      10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                      20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
                      30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
                      40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
                      50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
                      60, 61, 62, 63, 64, 65, 66, 67, 68, 69,
                      70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
                      80, 81, 82, 83, 84, 85, 86, 87, 88, 89,
                      90, 91, 92, 93, 94, 95, 96, 97, 98, 99);
    };

    (@gen $($n:tt),*) => {
        $(
            paste! {
                struct [<Msg $n>];
                impl Message for [<Msg $n>] {}
                impl Request for [<Msg $n>] {
                    type Response = u64;
                    type TypeId = catga_core::DefaultMessageTypeId;
                }

                struct [<H $n>];
                #[async_trait]
                impl Handler<[<Msg $n>]> for [<H $n>] {
                    async fn handle(&self, _: [<Msg $n>]) -> CatgaResult<u64> {
                        Ok(0u64)
                    }
                }
            }
        )*
    };
}

define_types!();

// Single message type for lookup benchmark
struct PingSingle;
impl Message for PingSingle {}
impl Request for PingSingle {
    type Response = u64;
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct HandlerSingle;
#[async_trait]
impl Handler<PingSingle> for HandlerSingle {
    async fn handle(&self, _: PingSingle) -> CatgaResult<u64> {
        Ok(0u64)
    }
}

/// Benchmark: Registry creation with 100 handlers
fn registry_creation(c: &mut Criterion) {
    c.bench_function("registry_with_100_handlers", |b| {
        b.iter(|| {
            let mut registry = Registry::new();
            paste! {
                registry.register_request::<[<Msg 0>], _>([<H 0>]).expect("test should not fail");
                registry.register_request::<[<Msg 1>], _>([<H 1>]).expect("test should not fail");
                registry.register_request::<[<Msg 2>], _>([<H 2>]).expect("test should not fail");
                registry.register_request::<[<Msg 3>], _>([<H 3>]).expect("test should not fail");
                registry.register_request::<[<Msg 4>], _>([<H 4>]).expect("test should not fail");
                registry.register_request::<[<Msg 5>], _>([<H 5>]).expect("test should not fail");
                registry.register_request::<[<Msg 6>], _>([<H 6>]).expect("test should not fail");
                registry.register_request::<[<Msg 7>], _>([<H 7>]).expect("test should not fail");
                registry.register_request::<[<Msg 8>], _>([<H 8>]).expect("test should not fail");
                registry.register_request::<[<Msg 9>], _>([<H 9>]).expect("test should not fail");
                registry.register_request::<[<Msg 10>], _>([<H 10>]).expect("test should not fail");
                registry.register_request::<[<Msg 11>], _>([<H 11>]).expect("test should not fail");
                registry.register_request::<[<Msg 12>], _>([<H 12>]).expect("test should not fail");
                registry.register_request::<[<Msg 13>], _>([<H 13>]).expect("test should not fail");
                registry.register_request::<[<Msg 14>], _>([<H 14>]).expect("test should not fail");
                registry.register_request::<[<Msg 15>], _>([<H 15>]).expect("test should not fail");
                registry.register_request::<[<Msg 16>], _>([<H 16>]).expect("test should not fail");
                registry.register_request::<[<Msg 17>], _>([<H 17>]).expect("test should not fail");
                registry.register_request::<[<Msg 18>], _>([<H 18>]).expect("test should not fail");
                registry.register_request::<[<Msg 19>], _>([<H 19>]).expect("test should not fail");
                registry.register_request::<[<Msg 20>], _>([<H 20>]).expect("test should not fail");
                registry.register_request::<[<Msg 21>], _>([<H 21>]).expect("test should not fail");
                registry.register_request::<[<Msg 22>], _>([<H 22>]).expect("test should not fail");
                registry.register_request::<[<Msg 23>], _>([<H 23>]).expect("test should not fail");
                registry.register_request::<[<Msg 24>], _>([<H 24>]).expect("test should not fail");
                registry.register_request::<[<Msg 25>], _>([<H 25>]).expect("test should not fail");
                registry.register_request::<[<Msg 26>], _>([<H 26>]).expect("test should not fail");
                registry.register_request::<[<Msg 27>], _>([<H 27>]).expect("test should not fail");
                registry.register_request::<[<Msg 28>], _>([<H 28>]).expect("test should not fail");
                registry.register_request::<[<Msg 29>], _>([<H 29>]).expect("test should not fail");
                registry.register_request::<[<Msg 30>], _>([<H 30>]).expect("test should not fail");
                registry.register_request::<[<Msg 31>], _>([<H 31>]).expect("test should not fail");
                registry.register_request::<[<Msg 32>], _>([<H 32>]).expect("test should not fail");
                registry.register_request::<[<Msg 33>], _>([<H 33>]).expect("test should not fail");
                registry.register_request::<[<Msg 34>], _>([<H 34>]).expect("test should not fail");
                registry.register_request::<[<Msg 35>], _>([<H 35>]).expect("test should not fail");
                registry.register_request::<[<Msg 36>], _>([<H 36>]).expect("test should not fail");
                registry.register_request::<[<Msg 37>], _>([<H 37>]).expect("test should not fail");
                registry.register_request::<[<Msg 38>], _>([<H 38>]).expect("test should not fail");
                registry.register_request::<[<Msg 39>], _>([<H 39>]).expect("test should not fail");
                registry.register_request::<[<Msg 40>], _>([<H 40>]).expect("test should not fail");
                registry.register_request::<[<Msg 41>], _>([<H 41>]).expect("test should not fail");
                registry.register_request::<[<Msg 42>], _>([<H 42>]).expect("test should not fail");
                registry.register_request::<[<Msg 43>], _>([<H 43>]).expect("test should not fail");
                registry.register_request::<[<Msg 44>], _>([<H 44>]).expect("test should not fail");
                registry.register_request::<[<Msg 45>], _>([<H 45>]).expect("test should not fail");
                registry.register_request::<[<Msg 46>], _>([<H 46>]).expect("test should not fail");
                registry.register_request::<[<Msg 47>], _>([<H 47>]).expect("test should not fail");
                registry.register_request::<[<Msg 48>], _>([<H 48>]).expect("test should not fail");
                registry.register_request::<[<Msg 49>], _>([<H 49>]).expect("test should not fail");
                registry.register_request::<[<Msg 50>], _>([<H 50>]).expect("test should not fail");
                registry.register_request::<[<Msg 51>], _>([<H 51>]).expect("test should not fail");
                registry.register_request::<[<Msg 52>], _>([<H 52>]).expect("test should not fail");
                registry.register_request::<[<Msg 53>], _>([<H 53>]).expect("test should not fail");
                registry.register_request::<[<Msg 54>], _>([<H 54>]).expect("test should not fail");
                registry.register_request::<[<Msg 55>], _>([<H 55>]).expect("test should not fail");
                registry.register_request::<[<Msg 56>], _>([<H 56>]).expect("test should not fail");
                registry.register_request::<[<Msg 57>], _>([<H 57>]).expect("test should not fail");
                registry.register_request::<[<Msg 58>], _>([<H 58>]).expect("test should not fail");
                registry.register_request::<[<Msg 59>], _>([<H 59>]).expect("test should not fail");
                registry.register_request::<[<Msg 60>], _>([<H 60>]).expect("test should not fail");
                registry.register_request::<[<Msg 61>], _>([<H 61>]).expect("test should not fail");
                registry.register_request::<[<Msg 62>], _>([<H 62>]).expect("test should not fail");
                registry.register_request::<[<Msg 63>], _>([<H 63>]).expect("test should not fail");
                registry.register_request::<[<Msg 64>], _>([<H 64>]).expect("test should not fail");
                registry.register_request::<[<Msg 65>], _>([<H 65>]).expect("test should not fail");
                registry.register_request::<[<Msg 66>], _>([<H 66>]).expect("test should not fail");
                registry.register_request::<[<Msg 67>], _>([<H 67>]).expect("test should not fail");
                registry.register_request::<[<Msg 68>], _>([<H 68>]).expect("test should not fail");
                registry.register_request::<[<Msg 69>], _>([<H 69>]).expect("test should not fail");
                registry.register_request::<[<Msg 70>], _>([<H 70>]).expect("test should not fail");
                registry.register_request::<[<Msg 71>], _>([<H 71>]).expect("test should not fail");
                registry.register_request::<[<Msg 72>], _>([<H 72>]).expect("test should not fail");
                registry.register_request::<[<Msg 73>], _>([<H 73>]).expect("test should not fail");
                registry.register_request::<[<Msg 74>], _>([<H 74>]).expect("test should not fail");
                registry.register_request::<[<Msg 75>], _>([<H 75>]).expect("test should not fail");
                registry.register_request::<[<Msg 76>], _>([<H 76>]).expect("test should not fail");
                registry.register_request::<[<Msg 77>], _>([<H 77>]).expect("test should not fail");
                registry.register_request::<[<Msg 78>], _>([<H 78>]).expect("test should not fail");
                registry.register_request::<[<Msg 79>], _>([<H 79>]).expect("test should not fail");
                registry.register_request::<[<Msg 80>], _>([<H 80>]).expect("test should not fail");
                registry.register_request::<[<Msg 81>], _>([<H 81>]).expect("test should not fail");
                registry.register_request::<[<Msg 82>], _>([<H 82>]).expect("test should not fail");
                registry.register_request::<[<Msg 83>], _>([<H 83>]).expect("test should not fail");
                registry.register_request::<[<Msg 84>], _>([<H 84>]).expect("test should not fail");
                registry.register_request::<[<Msg 85>], _>([<H 85>]).expect("test should not fail");
                registry.register_request::<[<Msg 86>], _>([<H 86>]).expect("test should not fail");
                registry.register_request::<[<Msg 87>], _>([<H 87>]).expect("test should not fail");
                registry.register_request::<[<Msg 88>], _>([<H 88>]).expect("test should not fail");
                registry.register_request::<[<Msg 89>], _>([<H 89>]).expect("test should not fail");
                registry.register_request::<[<Msg 90>], _>([<H 90>]).expect("test should not fail");
                registry.register_request::<[<Msg 91>], _>([<H 91>]).expect("test should not fail");
                registry.register_request::<[<Msg 92>], _>([<H 92>]).expect("test should not fail");
                registry.register_request::<[<Msg 93>], _>([<H 93>]).expect("test should not fail");
                registry.register_request::<[<Msg 94>], _>([<H 94>]).expect("test should not fail");
                registry.register_request::<[<Msg 95>], _>([<H 95>]).expect("test should not fail");
                registry.register_request::<[<Msg 96>], _>([<H 96>]).expect("test should not fail");
                registry.register_request::<[<Msg 97>], _>([<H 97>]).expect("test should not fail");
                registry.register_request::<[<Msg 98>], _>([<H 98>]).expect("test should not fail");
                registry.register_request::<[<Msg 99>], _>([<H 99>]).expect("test should not fail");
            }
            black_box(());
        });
    });
}

/// Benchmark: Registry lookup performance by TypeId
/// Tests HashMap lookup which is the core of registry dispatch
fn registry_lookup(c: &mut Criterion) {
    // Setup: create registry with one registered handler
    let mut registry = Registry::new();
    registry
        .register_request::<PingSingle, _>(HandlerSingle)
        .expect("test should not fail");

    c.bench_function("registry_lookup", |b| {
        let registry = &registry;
        b.iter(|| {
            let found = registry.get_handler::<PingSingle>();
            black_box(found);
        });
    });
}

criterion_group!(benches, registry_creation, registry_lookup);
criterion_main!(benches);
