//! Runs an order checkout with CQRS validation, compensating Flow steps, and an acknowledged event.
//!
//! The example deliberately uses in-memory adapters so that it runs without infrastructure. In a
//! service, replace `Inventory`, `PaymentGateway`, and `MemoryTransport` with the application's
//! bounded, durable adapters while keeping the CQRS handler and flow orchestration unchanged.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, Envelope, ErrorCode, Handler, Mediator, Message, MessageMetadata,
    MessageTransport, Request, catga_handlers,
};
use catga_flow::{Flow, FlowResult};
use catga_memory::MemoryTransport;

/// The typed query that keeps price validation at the CQRS boundary.
#[derive(Clone)]
struct QuoteOrder {
    quantity: u32,
    unit_price_cents: u64,
}

impl Message for QuoteOrder {}

impl Request for QuoteOrder {
    type Response = u64;
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct QuoteOrderHandler;

#[async_trait]
impl Handler<QuoteOrder> for QuoteOrderHandler {
    async fn handle(&self, order: QuoteOrder) -> CatgaResult<u64> {
        if order.quantity == 0 {
            return Err(CatgaError::new(
                ErrorCode::Validation,
                "an order must contain at least one item",
            ));
        }
        order
            .unit_price_cents
            .checked_mul(u64::from(order.quantity))
            .ok_or_else(|| CatgaError::new(ErrorCode::Validation, "order total overflows u64"))
    }
}

#[derive(Default)]
struct Inventory {
    reserved: AtomicBool,
}

impl Inventory {
    fn reserve(&self) -> CatgaResult<()> {
        self.reserved
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| CatgaError::new(ErrorCode::Conflict, "inventory is already reserved"))
    }

    fn release(&self) -> CatgaResult<()> {
        self.reserved.store(false, Ordering::Release);
        Ok(())
    }

    fn is_reserved(&self) -> bool {
        self.reserved.load(Ordering::Acquire)
    }
}

struct PaymentGateway {
    accepts_charges: bool,
    captured: AtomicBool,
}

impl PaymentGateway {
    fn new(accepts_charges: bool) -> Self {
        Self {
            accepts_charges,
            captured: AtomicBool::new(false),
        }
    }

    fn capture(&self, _amount_cents: u64) -> CatgaResult<()> {
        if !self.accepts_charges {
            return Err(CatgaError::new(
                ErrorCode::Unavailable,
                "payment provider declined the charge",
            ));
        }
        self.captured.store(true, Ordering::Release);
        Ok(())
    }

    fn refund(&self) -> CatgaResult<()> {
        self.captured.store(false, Ordering::Release);
        Ok(())
    }

    fn is_captured(&self) -> bool {
        self.captured.load(Ordering::Acquire)
    }
}

async fn checkout(
    inventory: Arc<Inventory>,
    payments: Arc<PaymentGateway>,
    total_cents: u64,
) -> FlowResult {
    let reserve_inventory = Arc::clone(&inventory);
    let release_inventory = Arc::clone(&inventory);
    let charge_gateway = Arc::clone(&payments);
    let refund_gateway = Arc::clone(&payments);

    Flow::new("order-fulfillment")
        .step(
            move || {
                let inventory = Arc::clone(&reserve_inventory);
                async move { inventory.reserve() }
            },
            move || {
                let inventory = Arc::clone(&release_inventory);
                async move { inventory.release() }
            },
        )
        .step(
            move || {
                let payments = Arc::clone(&charge_gateway);
                async move { payments.capture(total_cents) }
            },
            move || {
                let payments = Arc::clone(&refund_gateway);
                async move { payments.refund() }
            },
        )
        .run()
        .await
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    let mediator = Mediator::new(catga_handlers! { request QuoteOrder => QuoteOrderHandler }?);
    let total_cents = mediator
        .send(QuoteOrder {
            quantity: 2,
            unit_price_cents: 1_299,
        })
        .await?;

    let inventory = Arc::new(Inventory::default());
    let payments = Arc::new(PaymentGateway::new(true));
    let completed = checkout(Arc::clone(&inventory), Arc::clone(&payments), total_cents).await;
    assert!(completed.is_success());
    assert!(inventory.is_reserved());
    assert!(payments.is_captured());

    let transport = MemoryTransport::new(4)?;
    transport
        .publish(Envelope::new(
            1,
            "order.completed",
            total_cents.to_le_bytes().to_vec(),
            MessageMetadata::new(1, None),
        ))
        .await?;
    let delivery = transport.receive().await?;
    assert_eq!(delivery.envelope().message_type(), "order.completed");
    transport.ack(delivery).await?;

    let declined_inventory = Arc::new(Inventory::default());
    let declined_payment = Arc::new(PaymentGateway::new(false));
    let declined = checkout(
        Arc::clone(&declined_inventory),
        Arc::clone(&declined_payment),
        total_cents,
    )
    .await;
    assert!(!declined.is_success());
    assert!(!declined_inventory.is_reserved());
    assert!(!declined_payment.is_captured());

    println!(
        "order completed for ${:.2}; a declined payment released inventory",
        total_cents as f64 / 100.0
    );
    Ok(())
}
