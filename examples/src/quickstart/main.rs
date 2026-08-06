//! Minimal Catga example — the simplest path to a working mediator
//!
//! ```bash
//! cargo run -p catga-examples --bin quickstart
//! ```

use catga_core::{CatgaResult, catga_request, catga_service};

#[catga_request(response = u64)]
struct Double(u64);

#[catga_request(response = u64)]
struct Square(u64);

#[derive(Clone)]
struct Calculator;

#[catga_service(CalculatorMediator)]
impl Calculator {
    async fn double(&self, msg: Double) -> CatgaResult<u64> {
        Ok(msg.0 * 2)
    }

    async fn square(&self, msg: Square) -> CatgaResult<u64> {
        Ok(msg.0 * msg.0)
    }
}

#[tokio::main]
async fn main() -> CatgaResult<()> {
    let mediator = CalculatorMediator::new(Calculator);

    let doubled = mediator.send(Double(21)).await?;
    println!("21 * 2 = {}", doubled);

    let squared = mediator.send(Square(7)).await?;
    println!("7 * 7 = {}", squared);

    Ok(())
}
