//! Typed application facade contracts.

use async_trait::async_trait;
use catga_auto::AutoApp;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use catga_core::{
    CatgaResult, Command, CommandHandler, Event, EventHandler, Handler, Message, Request,
};

#[derive(Clone)]
struct Ping;
impl Message for Ping {}
impl Request for Ping {
    type Response = &'static str;
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct PingHandler;
#[async_trait]
impl Handler<Ping> for PingHandler {
    async fn handle(&self, _: Ping) -> CatgaResult<&'static str> {
        Ok("pong")
    }
}

struct RefreshCache;
impl Message for RefreshCache {}
impl Command for RefreshCache { type TypeId = catga_core::DefaultMessageTypeId; }

struct RefreshCacheHandler(Arc<AtomicUsize>);
#[async_trait]
impl CommandHandler<RefreshCache> for RefreshCacheHandler {
    async fn handle(&self, _: RefreshCache) -> CatgaResult<()> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Clone)]
struct CacheRefreshed;
impl Message for CacheRefreshed {}
impl Event for CacheRefreshed { type TypeId = catga_core::DefaultMessageTypeId; }

struct CacheRefreshedHandler(Arc<AtomicUsize>);
#[async_trait]
impl EventHandler<CacheRefreshed> for CacheRefreshedHandler {
    async fn handle(&self, _: CacheRefreshed) -> CatgaResult<()> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::test]
async fn auto_app_builds_a_typed_mediator_and_handle() -> CatgaResult<()> {
    let mut builder = AutoApp::builder();
    builder.register_request::<Ping, _>(PingHandler)?;
    let app = builder.build()?;

    assert_eq!(app.mediator().send(Ping).await?, "pong");
    assert_eq!(app.handle().send(Ping).await?, "pong");
    Ok(())
}

#[tokio::test]
async fn fluent_auto_app_registration_infers_the_message_type() -> CatgaResult<()> {
    let app = AutoApp::builder().request(PingHandler)?.build()?;

    assert_eq!(app.mediator().send(Ping).await?, "pong");
    assert_eq!(app.handle().send(Ping).await?, "pong");
    Ok(())
}

#[tokio::test]
async fn mediator_arc_keeps_the_built_application_graph_alive() -> CatgaResult<()> {
    let mediator = AutoApp::builder()
        .request(PingHandler)?
        .build()?
        .mediator_arc();

    assert_eq!(mediator.send(Ping).await?, "pong");
    Ok(())
}

#[cfg(feature = "axum")]
#[tokio::test]
async fn axum_state_uses_the_auto_app_mediator() -> CatgaResult<()> {
    let app = AutoApp::builder().request(PingHandler)?.build()?;
    let state = catga_auto::web::mediator_state(&app);

    assert_eq!(state.send(Ping).await?, "pong");
    Ok(())
}

#[tokio::test]
async fn fluent_auto_app_registration_chains_command_and_event_handlers() -> CatgaResult<()> {
    let commands = Arc::new(AtomicUsize::new(0));
    let events = Arc::new(AtomicUsize::new(0));
    let app = AutoApp::builder()
        .command(RefreshCacheHandler(Arc::clone(&commands)))?
        .event(CacheRefreshedHandler(Arc::clone(&events)))
        .build()?;

    app.mediator().send_command(RefreshCache).await?;
    app.mediator().publish(CacheRefreshed).await?;

    assert_eq!(commands.load(Ordering::Relaxed), 1);
    assert_eq!(events.load(Ordering::Relaxed), 1);
    Ok(())
}

#[test]
fn duplicate_typed_registration_is_rejected_before_build() -> CatgaResult<()> {
    let mut builder = AutoApp::builder();
    builder.register_request::<Ping, _>(PingHandler)?;
    let error = match builder.register_request::<Ping, _>(PingHandler) {
        Ok(_) => panic!("duplicate request registration must be rejected"),
        Err(error) => error,
    };
    assert_eq!(error.code(), catga_core::ErrorCode::Conflict);
    Ok(())
}

#[tokio::test]
async fn app_shutdown_is_explicit_and_does_not_spawn_a_hidden_task() {
    let app = AutoApp::builder().build().expect("empty app is valid");
    let shutdown = app.shutdown_token();
    app.shutdown();
    tokio::time::timeout(std::time::Duration::from_secs(1), shutdown.cancelled())
        .await
        .expect("explicit shutdown must cancel the app token");
}
