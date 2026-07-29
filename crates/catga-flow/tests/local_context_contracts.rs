//! Contracts for local Flow steps with a shared cloneable context.

use std::sync::{Arc, Mutex};

use catga_core::CatgaResult;
use catga_flow::{Flow, compensating_flow};

#[tokio::test]
async fn local_flow_clones_context_for_forward_and_compensation_actions() -> CatgaResult<()> {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let flow = Flow::new("context")
        .step_with(
            Arc::clone(&trace),
            |trace| async move {
                trace.lock().expect("trace lock").push("reserve");
                Ok(())
            },
            |trace| async move {
                trace.lock().expect("trace lock").push("release");
                Ok(())
            },
        )
        .step_with(
            Arc::clone(&trace),
            |trace| async move {
                trace.lock().expect("trace lock").push("charge");
                Err(catga_core::CatgaError::new(
                    catga_core::ErrorCode::Unavailable,
                    "payment declined",
                ))
            },
            |trace| async move {
                trace.lock().expect("trace lock").push("refund");
                Ok(())
            },
        );

    let result = flow.run().await;
    assert!(!result.is_success());
    assert_eq!(
        trace.lock().expect("trace lock").as_slice(),
        ["reserve", "charge", "release"]
    );
    Ok(())
}

#[tokio::test]
async fn compensating_flow_macro_keeps_actions_and_compensations_visible() -> CatgaResult<()> {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let flow = compensating_flow! {
        "macro-context";
        context = Arc::clone(&trace);
        reserve_inventory => release_inventory;
        capture_payment => refund_payment;
    };

    let result = flow.run().await;
    assert!(!result.is_success());
    assert_eq!(
        trace.lock().expect("trace lock").as_slice(),
        ["reserve", "capture", "release"]
    );
    Ok(())
}

#[tokio::test]
async fn compensating_flow_macro_supports_context_methods() -> CatgaResult<()> {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let flow = compensating_flow! {
        "method-context";
        context = CheckoutTrace(Arc::clone(&trace));
        steps {
            reserve_inventory => release_inventory;
            capture_payment => refund_payment;
        }
    };

    let result = flow.run().await;
    assert!(!result.is_success());
    assert_eq!(
        trace.lock().expect("trace lock").as_slice(),
        ["reserve", "capture", "release"]
    );
    Ok(())
}

#[derive(Clone)]
struct CheckoutTrace(Arc<Mutex<Vec<&'static str>>>);

impl CheckoutTrace {
    async fn reserve_inventory(self) -> CatgaResult<()> {
        self.0.lock().expect("trace lock").push("reserve");
        Ok(())
    }

    async fn release_inventory(self) -> CatgaResult<()> {
        self.0.lock().expect("trace lock").push("release");
        Ok(())
    }

    async fn capture_payment(self) -> CatgaResult<()> {
        self.0.lock().expect("trace lock").push("capture");
        Err(catga_core::CatgaError::new(
            catga_core::ErrorCode::Unavailable,
            "payment declined",
        ))
    }

    async fn refund_payment(self) -> CatgaResult<()> {
        self.0.lock().expect("trace lock").push("refund");
        Ok(())
    }
}

async fn reserve_inventory(trace: Arc<Mutex<Vec<&'static str>>>) -> CatgaResult<()> {
    trace.lock().expect("trace lock").push("reserve");
    Ok(())
}

async fn release_inventory(trace: Arc<Mutex<Vec<&'static str>>>) -> CatgaResult<()> {
    trace.lock().expect("trace lock").push("release");
    Ok(())
}

async fn capture_payment(trace: Arc<Mutex<Vec<&'static str>>>) -> CatgaResult<()> {
    trace.lock().expect("trace lock").push("capture");
    Err(catga_core::CatgaError::new(
        catga_core::ErrorCode::Unavailable,
        "payment declined",
    ))
}

async fn refund_payment(trace: Arc<Mutex<Vec<&'static str>>>) -> CatgaResult<()> {
    trace.lock().expect("trace lock").push("refund");
    Ok(())
}
