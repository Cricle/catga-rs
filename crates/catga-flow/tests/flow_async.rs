//! Integration tests for the `flow_async!` callback macro.

use std::sync::Arc;

use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ErrorCode, Request, RequestClient};
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

#[derive(catga_core::Message)]
struct WireOnlyDouble(u32);

impl Request for WireOnlyDouble {
    type Response = u32;
}

struct WireOnlyDoubleClient;

#[async_trait]
impl RequestClient<WireOnlyDouble> for WireOnlyDoubleClient {
    async fn request(&self, request: &WireOnlyDouble) -> CatgaResult<u32> {
        Ok(request.0.saturating_mul(2))
    }
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

#[test]
fn dsl_remote_send_accepts_a_request_without_serde_traits() {
    struct State {
        value: u32,
    }

    let flow = catga_flow::DslFlow::new().remote_send_into(
        Arc::new(WireOnlyDoubleClient),
        |state: &State| WireOnlyDouble(state.value),
        |state, response| state.value = response,
    );
    let mut state = State { value: 21 };

    assert_eq!(block_on(flow.run(&mut state)), Ok(()));
    assert_eq!(state.value, 42);
}
