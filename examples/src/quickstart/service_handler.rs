//! Tests `#[catga_service]` attribute macro on impl blocks.

use catga_core::CatgaResult;

#[catga_core::catga_request(response = u64)]
struct Double(u64);

#[derive(catga_core::catga_command)]
struct Log(String);

struct OrderService;

#[catga_core::catga_service]
impl OrderService {
    async fn double(&self, msg: Double) -> CatgaResult<u64> {
        Ok(msg.0 * 2)
    }

    async fn log(&self, msg: Log) -> CatgaResult<()> {
        println!("[OrderService] {}", msg.0);
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    let registry = OrderService::registry()?;
    let app = catga_core::auto::AutoApp::from_registry(registry)?;

    let result = app.mediator().send(Double(21)).await?;
    assert_eq!(result, 42);
    println!("Double(21) = {}", result);

    app.mediator().send_command(Log("hello from OrderService!".to_string())).await?;

    Ok(())
}
