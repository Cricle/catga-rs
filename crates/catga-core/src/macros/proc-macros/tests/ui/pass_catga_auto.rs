//! A valid `#[catga_auto]` module discovers impl-block and free-function handlers.

use catga_core::{Mediator, Registry};

#[catga_core::catga_request(response = u64)]
pub struct Ping {
    pub seed: u64,
}

#[catga_core::catga_request(response = u64)]
pub struct Probe {
    pub seed: u64,
}

#[derive(catga_core::catga_command)]
pub struct Log;

#[derive(Clone, catga_core::catga_event)]
pub struct Bell;

#[catga_core_macros::catga_auto]
pub mod discovered {
    pub struct PingHandler;

    #[async_trait::async_trait]
    impl catga_core::Handler<Ping> for PingHandler {
        async fn handle(&self, ping: Ping) -> catga_core::CatgaResult<u64> {
            Ok(ping.seed + 1)
        }
    }

    pub struct LogHandler;

    #[async_trait::async_trait]
    impl catga_core::CommandHandler<Log> for LogHandler {
        async fn handle(&self, _log: Log) -> catga_core::CatgaResult<()> {
            Ok(())
        }
    }

    pub struct BellHandler;

    #[async_trait::async_trait]
    impl catga_core::EventHandler<Bell> for BellHandler {
        async fn handle(&self, _event: Bell) -> catga_core::CatgaResult<()> {
            Ok(())
        }
    }

    pub struct Helper;

    impl std::fmt::Debug for Helper {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Helper")
        }
    }

    pub async fn argless_async() -> u32 {
        3
    }

    pub async fn free_probe(probe: Probe) -> catga_core::CatgaResult<u64> {
        Ok(probe.seed + 100)
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let registry = discovered::__catga_auto_register(Registry::new())
        .expect("discovered handlers register");
    let mediator = Mediator::new(registry);
    assert_eq!(mediator.send(Ping { seed: 1 }).await.unwrap(), 2);
    assert_eq!(mediator.send(Probe { seed: 1 }).await.unwrap(), 101);
    mediator.send_command(Log).await.unwrap();
    mediator.publish(Bell).await.unwrap();
    assert_eq!(discovered::argless_async().await, 3);
    assert_eq!(format!("{:?}", discovered::Helper), "Helper");
}
