//! Strict contract tests for the `auto` application facade: builder
//! composition, registration errors, and the explicit shutdown token.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use catga_core::auto::AutoApp;
use catga_core::{
    CatgaResult, Command, CommandHandler, DefaultMessageTypeId, Event, EventHandler, Handler,
    Message, Registry, Request,
};
use tokio_util::sync::CancellationToken;

struct Ping;
impl Message for Ping {}
impl Request for Ping {
    type Response = u64;
    type TypeId = DefaultMessageTypeId;
}

struct Ring;
impl Message for Ring {}
impl Command for Ring {
    type TypeId = DefaultMessageTypeId;
}

#[derive(Clone)]
struct Pinged;
impl Message for Pinged {}
impl Event for Pinged {
    type TypeId = DefaultMessageTypeId;
}

struct PingHandler;
#[async_trait]
impl Handler<Ping> for PingHandler {
    async fn handle(&self, ping: Ping) -> CatgaResult<u64> {
        let _ = ping;
        Ok(7)
    }
}

struct OtherPingHandler;
#[async_trait]
impl Handler<Ping> for OtherPingHandler {
    async fn handle(&self, ping: Ping) -> CatgaResult<u64> {
        let _ = ping;
        Ok(8)
    }
}

struct RingHandler(Arc<AtomicUsize>);
#[async_trait]
impl CommandHandler<Ring> for RingHandler {
    async fn handle(&self, ring: Ring) -> CatgaResult<()> {
        let _ = ring;
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct PingedHandler(Arc<AtomicUsize>);
#[async_trait]
impl EventHandler<Pinged> for PingedHandler {
    async fn handle(&self, event: Pinged) -> CatgaResult<()> {
        let _ = event;
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn auto_app_builder_composes_handlers_and_dispatches() {
    let rings = Arc::new(AtomicUsize::new(0));
    let pings = Arc::new(AtomicUsize::new(0));

    let app = AutoApp::builder()
        .request::<Ping, _>(PingHandler)
        .expect("request handler registers")
        .command::<Ring, _>(RingHandler(Arc::clone(&rings)))
        .expect("command handler registers")
        .event::<Pinged, _>(PingedHandler(Arc::clone(&pings)))
        .build()
        .expect("app builds");

    assert_eq!(app.mediator().send(Ping).await.expect("dispatch"), 7);
    app.mediator().send_command(Ring).await.expect("dispatch");
    app.mediator()
        .publish(Pinged)
        .await
        .expect("publish succeeds");
    assert_eq!(rings.load(Ordering::SeqCst), 1);
    assert_eq!(pings.load(Ordering::SeqCst), 1);

    // The mediator Arc clone shares the same application graph.
    let mediator = app.mediator_arc();
    assert_eq!(mediator.send(Ping).await.expect("dispatch"), 7);
}

#[test]
fn auto_app_rejects_duplicate_registrations() {
    let result = AutoApp::builder()
        .handler::<Ping, _>(PingHandler)
        .expect("first handler registers")
        .handler::<Ping, _>(OtherPingHandler);
    assert!(result.is_err(), "a duplicate request registration fails");

    let result = AutoApp::builder()
        .command::<Ring, _>(RingHandler(Arc::new(AtomicUsize::new(0))))
        .expect("first command registers")
        .command::<Ring, _>(RingHandler(Arc::new(AtomicUsize::new(0))));
    assert!(result.is_err(), "a duplicate command registration fails");
}

#[test]
fn auto_app_supports_mutable_registration_and_prebuilt_registries() {
    let mut builder = AutoApp::builder();
    builder
        .register_request::<Ping, _>(PingHandler)
        .expect("request registers")
        .register_command::<Ring, _>(RingHandler(Arc::new(AtomicUsize::new(0))))
        .expect("command registers")
        .register_event::<Pinged, _>(PingedHandler(Arc::new(AtomicUsize::new(0))));
    let app = builder.build().expect("app builds");
    let _ = app.mediator();

    // Pre-built registries plug into both the builder and the direct factory.
    let registry = Registry::new();
    let app = AutoApp::builder()
        .with_registry(registry)
        .build()
        .expect("app builds");
    let _ = app.mediator_arc();

    let registry = Registry::new();
    let app = AutoApp::from_registry(registry).expect("app builds");
    let _ = app.mediator();
}

#[tokio::test]
async fn auto_app_shutdown_token_drives_run_until_cancelled() {
    let token = CancellationToken::new();
    let app = AutoApp::builder()
        .with_shutdown_token(token.clone())
        .build()
        .expect("app builds");

    let running = tokio::spawn(async move {
        app.run_until_cancelled().await;
    });
    tokio::task::yield_now().await;
    assert!(!running.is_finished());

    let app = AutoApp::builder().build().expect("app builds");
    let shutdown = app.shutdown_token();
    assert!(!shutdown.is_cancelled());
    app.shutdown();
    assert!(shutdown.is_cancelled());

    // The default builder token is independent of the application above.
    token.cancel();
    running.await.expect("run_until_cancelled resolves");
}
