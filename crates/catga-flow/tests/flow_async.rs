//! Integration tests for the `flow_async!` callback macro.

use std::sync::Arc;

use catga_core::{CatgaError, CatgaResult, ErrorCode};
use futures::{executor::block_on, future::BoxFuture};

type UnaryCallback =
    Box<dyn for<'a> Fn(&'a mut usize) -> BoxFuture<'a, CatgaResult<usize>> + Send + Sync>;
type BinaryCallback =
    Box<dyn for<'a> Fn(&'a mut usize, usize) -> BoxFuture<'a, CatgaResult<usize>> + Send + Sync>;
type TernaryCallback = Box<
    dyn for<'a> Fn(&'a mut usize, usize, usize) -> BoxFuture<'a, CatgaResult<usize>> + Send + Sync,
>;

async fn always_fails() -> CatgaResult<()> {
    Err(CatgaError::new(ErrorCode::Validation, "callback failed"))
}

#[test]
fn flow_async_builds_typed_callbacks_and_preserves_results() {
    let unary: UnaryCallback = Box::new(catga_flow::flow_async!(|value: &mut usize| async move {
        *value += 1;
        Ok(*value)
    }));
    let binary: BinaryCallback = Box::new(catga_flow::flow_async!(
        |value: &mut usize, increment: usize| async move {
            *value += increment;
            Ok(*value)
        }
    ));
    let ternary: TernaryCallback = Box::new(catga_flow::flow_async!(
        |value: &mut usize, left: usize, right: usize| async move {
            *value += left + right;
            Ok(*value)
        }
    ));

    let mut value = 1;
    assert_eq!(block_on(unary(&mut value)), Ok(2));
    assert_eq!(block_on(binary(&mut value, 3)), Ok(5));
    assert_eq!(block_on(ternary(&mut value, 4, 5)), Ok(14));
}

#[test]
fn flow_async_supports_move_closures_that_capture_arcs() {
    let multiplier = Arc::new(7_usize);
    let callback = catga_flow::flow_async!(move |value: usize| async move {
        Ok::<_, CatgaError>(*multiplier * value)
    });

    assert_eq!(block_on(callback(6)), Ok(42));
}

#[test]
fn flow_async_propagates_catga_errors() {
    let callback = catga_flow::flow_async!(|_: ()| async move {
        always_fails().await?;
        Ok::<(), CatgaError>(())
    });

    match block_on(callback(())) {
        Err(error) => {
            assert_eq!(error.code(), ErrorCode::Validation);
            assert_eq!(error.message(), "callback failed");
        }
        Ok(()) => panic!("callback should propagate the CatgaError"),
    }
}
