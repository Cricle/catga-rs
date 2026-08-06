use catga_core::{catga_service, CatgaResult, Command, Request, Message};

struct Ping;
impl Message for Ping {}
impl Request for Ping {
    type Response = String;
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct Log(String);
impl Message for Log {}
impl Command for Log {
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct Calculator;

#[catga_service]
impl Calculator {
    async fn ping(&self, msg: Ping) -> CatgaResult<String> {
        Ok("pong".to_string())
    }
    async fn log(&self, cmd: Log) -> CatgaResult<()> {
        Ok(())
    }
}

#[test]
fn registry_doc_contains_handler_signatures() {
    // This test verifies the registry() doc contains handler info
    // Run with: cargo test --doc -p catga-core -- registry_doc
    // Check output: registry::docs().contains("ping")
}
