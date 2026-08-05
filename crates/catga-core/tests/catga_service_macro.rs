//! Comprehensive tests for #[catga_service] macro

use catga_core::{
    CatgaResult, auto::AutoApp, catga_command, catga_event, catga_request, catga_service,
};

#[catga_request(response = u64)]
struct Double(u64);

#[derive(catga_command)]
struct Log(String);

#[derive(catga_event, Clone)]
struct OrderCreated {
    #[allow(dead_code)]
    order_id: u64,
}

#[derive(Clone)]
struct TestService;

#[catga_service]
impl TestService {
    // Request: CatgaResult<T> where T != ()
    async fn double(&self, msg: Double) -> CatgaResult<u64> {
        Ok(msg.0 * 2)
    }

    // Command: CatgaResult<()>
    async fn log(&self, msg: Log) -> CatgaResult<()> {
        println!("[TestService] {}", msg.0);
        Ok(())
    }

    // Event: async fn on_*(&self, event: E) -> CatgaResult<()>
    async fn on_order_created(&self, _event: OrderCreated) -> CatgaResult<()> {
        Ok(())
    }
}

#[tokio::test]
async fn catga_service_generates_working_registry() -> CatgaResult<()> {
    let registry = TestService.registry()?;
    let app = AutoApp::from_registry(registry)?;

    let result = app.mediator().send(Double(21)).await?;
    assert_eq!(result, 42);

    app.mediator().send_command(Log("test".to_string())).await?;
    Ok(())
}

#[tokio::test]
async fn catga_service_detects_request_vs_command() -> CatgaResult<()> {
    let registry = TestService.registry()?;
    let app = AutoApp::from_registry(registry)?;

    let response: u64 = app.mediator().send(Double(5)).await?;
    assert_eq!(response, 10);

    app.mediator()
        .send_command(Log("hello".to_string()))
        .await?;
    Ok(())
}
