//! `#[catga_service]` with only command and event methods still builds a registry.

use catga_core::{CatgaResult, Mediator};

#[derive(catga_core::catga_command)]
pub struct Log(pub String);

#[derive(Clone, catga_core::catga_event)]
pub struct Ring;

#[derive(Clone)]
pub struct Notifier;

#[catga_core_macros::catga_service]
impl Notifier {
    async fn log(&self, _cmd: Log) -> CatgaResult<()> {
        Ok(())
    }

    async fn on_ring(&self, _event: Ring) -> CatgaResult<()> {
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let registry = Notifier.registry().expect("service handlers register");
    let mediator = Mediator::new(registry);
    mediator.send_command(Log("hi".to_string())).await.unwrap();
    mediator.publish(Ring).await.unwrap();
}
