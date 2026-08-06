//! Order service — CQRS + Event Sourcing best practices
//!
//! ```bash
//! cargo run -p catga-examples --bin order
//! ```

use catga_core::CatgaResult;

use catga_examples::order::{OrderMediator, OrderService, domain::*};

#[tokio::main]
async fn main() -> CatgaResult<()> {
    let service = OrderService::new();
    let mediator = OrderMediator::new(service);

    // 1. Place an order (command)
    println!("=== Place Order ===");
    let placed = mediator
        .send(PlaceOrder {
            quantity: 2,
            unit_price_cents: 1299,
        })
        .await?;
    println!(
        "Order placed: {} - {}x${:.2} = ${:.2}",
        placed.order_id,
        placed.quantity,
        placed.total_cents as f64 / 100.0 / placed.quantity as f64,
        placed.total_cents as f64 / 100.0
    );

    // 2. Get order (query)
    println!("\n=== Get Order ===");
    let order = mediator
        .send(GetOrder {
            order_id: placed.order_id.clone(),
        })
        .await?;
    println!("Order status: {:?}", order.status);

    // 3. Confirm payment (command with compensating flow)
    println!("\n=== Confirm Payment ===");
    mediator
        .send_command(ConfirmPayment {
            order_id: placed.order_id.clone(),
            payment_id: "pay_123".into(),
        })
        .await?;
    println!("Payment confirmed");

    // 4. Check updated status (query)
    println!("\n=== Check Status ===");
    let status = mediator
        .send(GetOrderStatus {
            order_id: placed.order_id.clone(),
        })
        .await?;
    println!("Order status: {:?}", status);

    // 5. Try to cancel confirmed order (should fail)
    println!("\n=== Try Cancel Confirmed Order ===");
    match mediator
        .send_command(CancelOrder {
            order_id: placed.order_id.clone(),
        })
        .await
    {
        Ok(_) => println!("Order cancelled"),
        Err(e) => println!("Cannot cancel: {}", e),
    }

    // 6. Place another order and cancel it
    println!("\n=== Cancel Pending Order ===");
    let order2 = mediator
        .send(PlaceOrder {
            quantity: 1,
            unit_price_cents: 500,
        })
        .await?;
    mediator
        .send_command(CancelOrder {
            order_id: order2.order_id,
        })
        .await?;
    println!("Order cancelled successfully");

    println!("\n=== All tests passed! ===");
    Ok(())
}
