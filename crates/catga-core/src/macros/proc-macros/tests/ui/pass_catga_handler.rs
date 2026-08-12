//! Valid `#[catga_handler]` annotations re-emit validated handler trait impls unchanged.

use catga_core::CatgaResult;

#[catga_core::catga_request(response = u64)]
pub struct Ping {
    pub seed: u64,
}

#[derive(catga_core::catga_command)]
pub struct Log;

#[derive(Clone, catga_core::catga_event)]
pub struct Bell;

pub struct PingHandler;

#[catga_core_macros::catga_handler]
#[async_trait::async_trait]
impl catga_core::Handler<Ping> for PingHandler {
    async fn handle(&self, ping: Ping) -> CatgaResult<u64> {
        Ok(ping.seed)
    }
}

pub struct LogHandler;

#[catga_core_macros::catga_handler]
#[async_trait::async_trait]
impl catga_core::CommandHandler<Log> for LogHandler {
    async fn handle(&self, _log: Log) -> CatgaResult<()> {
        Ok(())
    }
}

pub struct BellHandler;

#[catga_core_macros::catga_handler]
#[async_trait::async_trait]
impl catga_core::EventHandler<Bell> for BellHandler {
    async fn handle(&self, _event: Bell) -> CatgaResult<()> {
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    use catga_core::{CommandHandler, EventHandler, Handler};
    assert_eq!(PingHandler.handle(Ping { seed: 7 }).await.unwrap(), 7);
    LogHandler.handle(Log).await.unwrap();
    BellHandler.handle(Bell).await.unwrap();
}
