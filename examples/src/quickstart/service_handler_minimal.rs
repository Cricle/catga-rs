//! Minimal test for catga_service

use catga_core::auto::AutoApp;
use catga_core::CatgaResult;

struct TestMsg;
impl catga_core::Message for TestMsg {}

struct TestService;

#[catga_core::catga_service]
impl TestService {
    async fn handle(&self, _msg: TestMsg) -> CatgaResult<()> {
        Ok(())
    }
}

fn main() {}
