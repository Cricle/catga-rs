//! Handler trait implementation tests

use async_trait::async_trait;
use catga_core::{CatgaResult, Handler, Message, Request};

struct TestRequest(u64);
impl Message for TestRequest {}
impl Request for TestRequest {
    type Response = u64;
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct TestHandler;

#[async_trait]
impl Handler<TestRequest> for TestHandler {
    async fn handle(&self, msg: TestRequest) -> CatgaResult<u64> {
        Ok(msg.0 * 3)
    }
}

#[tokio::test]
async fn handler_returns_expected_response() -> CatgaResult<()> {
    let handler = TestHandler;
    let response = handler.handle(TestRequest(10)).await?;
    assert_eq!(response, 30);
    Ok(())
}
