//! Benchmark: global dispatch vs direct mediator call (release mode, async)

use catga_auto::{AutoApp, send};
use catga_core::{CatgaResult, Mediator, MediatorHandle, Registry, catga_request};
use std::sync::Arc;
use std::time::Instant;

#[catga_request(response = String)]
struct GetUser(String);

async fn get_user_handler(msg: GetUser) -> CatgaResult<String> {
    Ok(format!("User {} found", msg.0))
}

#[tokio::main]
async fn main() -> CatgaResult<()> {
    // Setup: create app with global dispatch
    let app = AutoApp::builder().handler(get_user_handler)?.build()?;

    // Direct mediator call setup
    let mut registry = Registry::new();
    registry.register_request(get_user_handler)?;
    let mediator = Arc::new(Mediator::new(registry));
    let handle = MediatorHandle::new();
    handle.bind(Arc::clone(&mediator))?;

    let iterations = 100_000;

    // Prevent DCE by collecting results
    let mut global_results = Vec::with_capacity(iterations);
    let global_start = Instant::now();
    for i in 0..iterations {
        global_results.push(send(GetUser(format!("{i}"))).await?);
    }
    let global_elapsed = global_start.elapsed();
    drop(global_results);

    // Direct handle.send()
    let mut handle_results = Vec::with_capacity(iterations);
    let handle_start = Instant::now();
    for i in 0..iterations {
        handle_results.push(handle.send(GetUser(format!("{i}"))).await?);
    }
    let handle_elapsed = handle_start.elapsed();
    drop(handle_results);

    // Arc clone + mediator call
    let mut arc_results = Vec::with_capacity(iterations);
    let mediator_clone = Arc::clone(&mediator);
    let handle_start = Instant::now();
    for i in 0..iterations {
        arc_results.push(mediator_clone.send(GetUser(format!("{i}"))).await?);
    }
    let arc_elapsed = handle_start.elapsed();
    drop(arc_results);

    // Prevent unused warnings
    let _ = (&global_elapsed, &handle_elapsed, &arc_elapsed);

    println!(
        "=== Benchmark Results ({} iterations, release mode) ===",
        iterations
    );
    println!(
        "Global send():    {:>10.3} ns/call",
        global_elapsed.as_nanos() as f64 / iterations as f64
    );
    println!(
        "Handle send():    {:>10.3} ns/call",
        handle_elapsed.as_nanos() as f64 / iterations as f64
    );
    println!(
        "Arc+mediator:     {:>10.3} ns/call",
        arc_elapsed.as_nanos() as f64 / iterations as f64
    );
    println!();
    println!("Overhead of global send() vs direct:");
    println!(
        "  vs Handle:  {:>+10.3} ns/call ({:.1}x slower)",
        (global_elapsed.as_nanos() as f64 - handle_elapsed.as_nanos() as f64) / iterations as f64,
        global_elapsed.as_nanos() as f64 / handle_elapsed.as_nanos() as f64
    );
    println!(
        "  vs Arc:     {:>+10.3} ns/call ({:.1}x slower)",
        (global_elapsed.as_nanos() as f64 - arc_elapsed.as_nanos() as f64) / iterations as f64,
        global_elapsed.as_nanos() as f64 / arc_elapsed.as_nanos() as f64
    );

    app.shutdown();
    Ok(())
}
