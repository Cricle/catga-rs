//! `#[catga_service(Name)]` additionally expands to a monomorphized typed mediator.

use catga_core::CatgaResult;

#[catga_core::catga_request(response = u64)]
pub struct Double(pub u64);

#[derive(catga_core::catga_command)]
pub struct Log(pub String);

#[derive(Clone, catga_core::catga_event)]
pub struct Ring;

#[derive(Clone)]
pub struct Calculator;

#[catga_core_macros::catga_service(CalcMediator)]
impl Calculator {
    async fn double(&self, msg: Double) -> CatgaResult<u64> {
        Ok(msg.0 * 2)
    }

    async fn log(&self, _cmd: Log) -> CatgaResult<()> {
        Ok(())
    }

    async fn on_ring(&self, _event: Ring) -> CatgaResult<()> {
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mediator = CalcMediator::new(Calculator);
    assert_eq!(mediator.send(Double(21)).await.unwrap(), 42);
    mediator.send_command(Log("hi".to_string())).await.unwrap();
    mediator.publish(Ring).await.unwrap();
}
