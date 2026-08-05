// Minimal test to reproduce the borrow of moved value error
use catga_core::{CatgaResult, catga_command, catga_event, catga_request, catga_service};

#[catga_request(response = u64)]
struct GetBalance { account_id: u64 }

#[derive(catga_command)]
struct TransferFunds { from: u64, to: u64, amount: u64 }

#[derive(catga_event, Clone)]
struct TransferCompleted { from: u64, to: u64, amount: u64 }

#[derive(Clone)]
struct BankService;

#[catga_service(BankMediator)]
impl BankService {
    async fn get_balance(&self, msg: GetBalance) -> CatgaResult<u64> {
        Ok(msg.account_id * 1000)
    }

    async fn transfer(&self, cmd: TransferFunds) -> CatgaResult<()> {
        Ok(())
    }

    async fn on_transfer_completed(&self, event: TransferCompleted) -> CatgaResult<()> {
        Ok(())
    }
}

fn main() {
    let service = BankService;
    let _mediator = BankMediator::new(service);
}
