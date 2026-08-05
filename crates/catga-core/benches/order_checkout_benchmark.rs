//! Business scenario benchmarks: Order Checkout Flow
//!
//! Tests the complete order fulfillment flow with inventory reservation,
//! payment processing, and compensation on failure.

#![feature(test)]

extern crate test;

use catga_core::{ErrorCode, flow::Flow};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

struct Inventory {
    reserved: AtomicBool,
    release_count: AtomicU64,
}

impl Inventory {
    fn new() -> Self {
        Self {
            reserved: AtomicBool::new(false),
            release_count: AtomicU64::new(0),
        }
    }

    fn reserve(&self) -> Result<(), catga_core::CatgaError> {
        self.reserved
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| {
                catga_core::CatgaError::new(ErrorCode::Conflict, "inventory already reserved")
            })
    }

    fn release(&self) {
        self.reserved.store(false, Ordering::Release);
        self.release_count.fetch_add(1, Ordering::Relaxed);
    }
}

struct PaymentGateway {
    captured: AtomicBool,
    refund_count: AtomicU64,
    decline: bool,
}

impl PaymentGateway {
    fn new(decline: bool) -> Self {
        Self {
            captured: AtomicBool::new(false),
            refund_count: AtomicU64::new(0),
            decline,
        }
    }

    fn capture(&self, _amount: u64) -> Result<(), catga_core::CatgaError> {
        if self.decline {
            return Err(catga_core::CatgaError::new(
                ErrorCode::Unavailable,
                "payment declined",
            ));
        }
        self.captured.store(true, Ordering::Release);
        Ok(())
    }

    fn refund(&self) {
        self.captured.store(false, Ordering::Release);
        self.refund_count.fetch_add(1, Ordering::Relaxed);
    }
}

// ============================================
// Core Flow Benchmarks
// ============================================

// Benchmark: Complete checkout flow (success case)
#[bench]
fn bench_checkout_flow_success(b: &mut test::Bencher) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    b.iter(|| {
        let inventory = Arc::new(Inventory::new());
        let payment = Arc::new(PaymentGateway::new(false));

        let inv_run = inventory.clone();
        let inv_comp = inventory.clone();
        let pay_run = payment.clone();
        let pay_comp = payment.clone();

        let flow = rt.block_on(async {
            Flow::new("checkout")
                .step(
                    move || {
                        let inv = inv_run.clone();
                        async move { inv.reserve() }
                    },
                    move || {
                        let inv = inv_comp.clone();
                        async move {
                            inv.release();
                            Ok(())
                        }
                    },
                )
                .step(
                    move || {
                        let pay = pay_run.clone();
                        async move { pay.capture(1000) }
                    },
                    move || {
                        let pay = pay_comp.clone();
                        async move {
                            pay.refund();
                            Ok(())
                        }
                    },
                )
                .run()
                .await
        });
        test::black_box(flow);
    });
}

// Benchmark: Checkout flow with payment failure (triggers compensation)
#[bench]
fn bench_checkout_flow_compensation(b: &mut test::Bencher) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    b.iter(|| {
        let inventory = Arc::new(Inventory::new());
        let payment = Arc::new(PaymentGateway::new(true));

        let inv_run = inventory.clone();
        let inv_comp = inventory.clone();
        let pay_run = payment.clone();
        let pay_comp = payment.clone();

        let flow = rt.block_on(async {
            Flow::new("checkout")
                .step(
                    move || {
                        let inv = inv_run.clone();
                        async move { inv.reserve() }
                    },
                    move || {
                        let inv = inv_comp.clone();
                        async move {
                            inv.release();
                            Ok(())
                        }
                    },
                )
                .step(
                    move || {
                        let pay = pay_run.clone();
                        async move { pay.capture(1000) }
                    },
                    move || {
                        let pay = pay_comp.clone();
                        async move {
                            pay.refund();
                            Ok(())
                        }
                    },
                )
                .run()
                .await
        });
        test::black_box(flow);
    });
}

// Benchmark: Single step execution
#[bench]
fn bench_single_step_execution(b: &mut test::Bencher) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    b.iter(|| {
        let inventory = Arc::new(Inventory::new());

        let inv_run = inventory.clone();
        let inv_comp = inventory.clone();

        let flow = rt.block_on(async {
            Flow::new("single-step")
                .step(
                    move || {
                        let inv = inv_run.clone();
                        async move { inv.reserve() }
                    },
                    move || {
                        let inv = inv_comp.clone();
                        async move {
                            inv.release();
                            Ok(())
                        }
                    },
                )
                .run()
                .await
        });
        test::black_box(flow);
    });
}

// Benchmark: Multi-step flow (5 steps)
#[bench]
fn bench_multi_step_flow_5(b: &mut test::Bencher) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    b.iter(|| {
        let flow = rt.block_on(async {
            Flow::new("multi-step")
                .step(
                    || async { Ok::<_, catga_core::CatgaError>(()) },
                    || async { Ok(()) },
                )
                .step(|| async { Ok(()) }, || async { Ok(()) })
                .step(|| async { Ok(()) }, || async { Ok(()) })
                .step(|| async { Ok(()) }, || async { Ok(()) })
                .step(|| async { Ok(()) }, || async { Ok(()) })
                .run()
                .await
        });
        test::black_box(flow);
    });
}

// Benchmark: Multi-step flow (10 steps)
#[bench]
fn bench_multi_step_flow_10(b: &mut test::Bencher) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    b.iter(|| {
        let flow = rt.block_on(async {
            Flow::new("multi-step-10")
                .step(
                    || async { Ok::<_, catga_core::CatgaError>(()) },
                    || async { Ok(()) },
                )
                .step(|| async { Ok(()) }, || async { Ok(()) })
                .step(|| async { Ok(()) }, || async { Ok(()) })
                .step(|| async { Ok(()) }, || async { Ok(()) })
                .step(|| async { Ok(()) }, || async { Ok(()) })
                .step(|| async { Ok(()) }, || async { Ok(()) })
                .step(|| async { Ok(()) }, || async { Ok(()) })
                .step(|| async { Ok(()) }, || async { Ok(()) })
                .step(|| async { Ok(()) }, || async { Ok(()) })
                .step(|| async { Ok(()) }, || async { Ok(()) })
                .run()
                .await
        });
        test::black_box(flow);
    });
}

// Benchmark: Flow creation overhead (without execution)
#[bench]
fn bench_flow_creation_overhead(b: &mut test::Bencher) {
    b.iter(|| {
        let _flow = Flow::new("overhead");
        test::black_box(&_flow);
    });
}

// ============================================
// Large Data Stress Tests
// ============================================

const LARGE_DATA_SIZE: usize = 64 * 1024; // 64KB

struct LargeContext {
    data: Vec<u8>,
}

impl LargeContext {
    fn new() -> Self {
        Self {
            data: vec![0u8; LARGE_DATA_SIZE],
        }
    }

    fn process(&self) -> Result<Vec<u8>, catga_core::CatgaError> {
        let mut result = Vec::with_capacity(self.data.len());
        for (i, &byte) in self.data.iter().enumerate() {
            result.push(byte.wrapping_add((i % 256) as u8));
        }
        Ok(result)
    }
}

// Benchmark: Large context data processing (64KB)
#[bench]
fn bench_large_context_processing(b: &mut test::Bencher) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    b.iter(|| {
        let ctx = Arc::new(LargeContext::new());

        let ctx_run = ctx.clone();
        let ctx_comp = ctx.clone();

        let flow = rt.block_on(async {
            Flow::new("large-context")
                .step(
                    move || {
                        let ctx = ctx_run.clone();
                        async move {
                            ctx.process()?;
                            Ok(())
                        }
                    },
                    move || {
                        let _ctx = ctx_comp.clone();
                        async move { Ok(()) }
                    },
                )
                .run()
                .await
        });
        test::black_box(flow);
    });
}

// Benchmark: Many small payloads (100 items)
#[bench]
fn bench_many_small_payloads(b: &mut test::Bencher) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    b.iter(|| {
        let counter = Arc::new(AtomicU64::new(0));

        let flow = rt.block_on(async {
            let mut flow_builder = Flow::new("many-payloads");

            for _ in 0..100 {
                let c = counter.clone();
                flow_builder = flow_builder.step(
                    move || {
                        let c = c.clone();
                        async move {
                            c.fetch_add(1, Ordering::Relaxed);
                            Ok(())
                        }
                    },
                    move || async { Ok(()) },
                );
            }

            flow_builder.run().await
        });
        test::black_box(flow);
    });
}

// Benchmark: Concurrent flow execution (4 flows in parallel)
#[bench]
fn bench_concurrent_flows_4(b: &mut test::Bencher) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    b.iter(|| {
        rt.block_on(async {
            let (r1, r2, r3, r4) = tokio::join!(
                {
                    let inv1 = Arc::new(Inventory::new());
                    let inv1_run = inv1.clone();
                    let inv1_comp = inv1.clone();
                    Flow::new("concurrent-1")
                        .step(
                            move || {
                                let i = inv1_run.clone();
                                async move {
                                    i.reserve();
                                    Ok(())
                                }
                            },
                            move || {
                                let i = inv1_comp.clone();
                                async move {
                                    i.release();
                                    Ok(())
                                }
                            },
                        )
                        .run()
                },
                {
                    let inv2 = Arc::new(Inventory::new());
                    let inv2_run = inv2.clone();
                    let inv2_comp = inv2.clone();
                    Flow::new("concurrent-2")
                        .step(
                            move || {
                                let i = inv2_run.clone();
                                async move {
                                    i.reserve();
                                    Ok(())
                                }
                            },
                            move || {
                                let i = inv2_comp.clone();
                                async move {
                                    i.release();
                                    Ok(())
                                }
                            },
                        )
                        .run()
                },
                {
                    let inv3 = Arc::new(Inventory::new());
                    let inv3_run = inv3.clone();
                    let inv3_comp = inv3.clone();
                    Flow::new("concurrent-3")
                        .step(
                            move || {
                                let i = inv3_run.clone();
                                async move {
                                    i.reserve();
                                    Ok(())
                                }
                            },
                            move || {
                                let i = inv3_comp.clone();
                                async move {
                                    i.release();
                                    Ok(())
                                }
                            },
                        )
                        .run()
                },
                {
                    let inv4 = Arc::new(Inventory::new());
                    let inv4_run = inv4.clone();
                    let inv4_comp = inv4.clone();
                    Flow::new("concurrent-4")
                        .step(
                            move || {
                                let i = inv4_run.clone();
                                async move {
                                    i.reserve();
                                    Ok(())
                                }
                            },
                            move || {
                                let i = inv4_comp.clone();
                                async move {
                                    i.release();
                                    Ok(())
                                }
                            },
                        )
                        .run()
                },
            );
            test::black_box((r1, r2, r3, r4));
        });
    });
}

// Benchmark: High throughput batch (20 flows sequentially)
#[bench]
fn bench_throughput_batch_20(b: &mut test::Bencher) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    b.iter(|| {
        rt.block_on(async {
            let mut results = Vec::with_capacity(20);
            for i in 0..20 {
                let flow = Flow::new(format!("batch-{}", i))
                    .step(|| async { Ok(()) }, || async { Ok(()) })
                    .step(|| async { Ok(()) }, || async { Ok(()) })
                    .run()
                    .await;
                results.push(flow);
            }
            test::black_box(results);
        });
    });
}

// Benchmark: Deep compensation chain (5 steps with all compensating)
#[bench]
fn bench_deep_compensation_chain(b: &mut test::Bencher) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    b.iter(|| {
        let c1_run = Arc::new(AtomicU64::new(0));
        let c1_comp = Arc::new(AtomicU64::new(0));
        let c2_run = Arc::new(AtomicU64::new(0));
        let c2_comp = Arc::new(AtomicU64::new(0));
        let c3_run = Arc::new(AtomicU64::new(0));
        let c3_comp = Arc::new(AtomicU64::new(0));
        let c4_run = Arc::new(AtomicU64::new(0));
        let c4_comp = Arc::new(AtomicU64::new(0));
        let c5_comp = Arc::new(AtomicU64::new(0));

        // Last step always fails to trigger all compensations
        let fail = Arc::new(AtomicBool::new(true));
        let fail_clone = fail.clone();

        let flow = rt.block_on(async {
            Flow::new("deep-comp")
                .step(
                    move || {
                        let c = c1_run.clone();
                        async move {
                            c.fetch_add(1, Ordering::Relaxed);
                            Ok(())
                        }
                    },
                    move || {
                        let c = c1_comp.clone();
                        async move {
                            c.fetch_sub(1, Ordering::Relaxed);
                            Ok(())
                        }
                    },
                )
                .step(
                    move || {
                        let c = c2_run.clone();
                        async move {
                            c.fetch_add(1, Ordering::Relaxed);
                            Ok(())
                        }
                    },
                    move || {
                        let c = c2_comp.clone();
                        async move {
                            c.fetch_sub(1, Ordering::Relaxed);
                            Ok(())
                        }
                    },
                )
                .step(
                    move || {
                        let c = c3_run.clone();
                        async move {
                            c.fetch_add(1, Ordering::Relaxed);
                            Ok(())
                        }
                    },
                    move || {
                        let c = c3_comp.clone();
                        async move {
                            c.fetch_sub(1, Ordering::Relaxed);
                            Ok(())
                        }
                    },
                )
                .step(
                    move || {
                        let c = c4_run.clone();
                        async move {
                            c.fetch_add(1, Ordering::Relaxed);
                            Ok(())
                        }
                    },
                    move || {
                        let c = c4_comp.clone();
                        async move {
                            c.fetch_sub(1, Ordering::Relaxed);
                            Ok(())
                        }
                    },
                )
                .step(
                    move || {
                        let f = fail_clone.clone();
                        async move {
                            if f.load(Ordering::Acquire) {
                                Err(catga_core::CatgaError::new(
                                    ErrorCode::Internal,
                                    "forced failure",
                                ))
                            } else {
                                Ok(())
                            }
                        }
                    },
                    move || {
                        let c = c5_comp.clone();
                        async move {
                            c.fetch_add(1, Ordering::Relaxed);
                            Ok(())
                        }
                    },
                )
                .run()
                .await
        });
        test::black_box(flow);
    });
}

// ============================================
// Memory and Edge Case Benchmarks
// ============================================

// Benchmark: Empty flow (no steps)
#[bench]
fn bench_empty_flow(b: &mut test::Bencher) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    b.iter(|| {
        let flow = rt.block_on(async { Flow::new("empty").run().await });
        test::black_box(flow);
    });
}

// Benchmark: Single step with no-op (success path only)
#[bench]
fn bench_single_step_noop(b: &mut test::Bencher) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    b.iter(|| {
        let flow = rt.block_on(async {
            Flow::new("noop")
                .step(|| async { Ok(()) }, || async { Ok(()) })
                .run()
                .await
        });
        test::black_box(flow);
    });
}
