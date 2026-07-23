use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use catga_core::{CatgaError, ErrorCode};
use catga_flow::Flow;

#[tokio::test]
async fn local_flow_compensates_completed_steps_in_reverse_order() {
    let trace = Arc::new(AtomicUsize::new(0));
    let reserve = Arc::clone(&trace);
    let release = Arc::clone(&trace);
    let charge = Arc::clone(&trace);

    let result = Flow::new("reserve")
        .step(
            move || {
                let trace = Arc::clone(&reserve);
                async move {
                    assert_eq!(trace.fetch_add(1, Ordering::Relaxed), 0);
                    Ok(())
                }
            },
            move || {
                let trace = Arc::clone(&release);
                async move {
                    assert_eq!(trace.fetch_add(1, Ordering::Relaxed), 2);
                    Ok(())
                }
            },
        )
        .step(
            move || {
                let trace = Arc::clone(&charge);
                async move {
                    assert_eq!(trace.fetch_add(1, Ordering::Relaxed), 1);
                    Err(CatgaError::new(ErrorCode::Transient, "charge"))
                }
            },
            || async { Ok(()) },
        )
        .run()
        .await;

    assert!(!result.is_success());
    assert_eq!(result.error().unwrap().message(), "charge");
    assert_eq!(trace.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn empty_local_flow_completes_successfully() {
    let result = Flow::new("empty").run().await;

    assert!(result.is_success());
    assert_eq!(result.completed_steps(), 0);
}
