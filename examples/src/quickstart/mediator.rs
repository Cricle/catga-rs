//! Minimal Catga example — define service, use mediator
//!
//! ```bash
//! cargo run -p catga-examples --bin mediator
//! ```

use catga_core::{catga_service, catga_request, CatgaResult};
use tokio;

#[catga_request(response = u64)]
struct Double(u64);

#[derive(Clone)]
struct Calculator;

#[catga_service(CalculatorMediator)]
impl Calculator {
    async fn double(&self, msg: Double) -> CatgaResult<u64> {
        Ok(msg.0 * 2)
    }
}

#[tokio::main]
async fn main() -> CatgaResult<()> {
    let mediator = CalculatorMediator::new(Calculator);
    let result = mediator.send(Double(21)).await?;
    println!("21 * 2 = {}", result);
    Ok(())
}
