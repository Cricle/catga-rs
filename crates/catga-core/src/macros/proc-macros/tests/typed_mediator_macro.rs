//! Behavioral contracts for `catga_typed_mediator!`: typed dispatch, positional constructor,
//! event fan-out order, and first-error semantics.

#[path = "support/messages.rs"]
mod messages;

use std::sync::{Arc, Mutex};

use catga_core::{CatgaError, CatgaResult, CommandHandler, ErrorCode, EventHandler, Handler};
use messages::{Ask, Bell, Log};

struct AskHandler;

#[async_trait::async_trait]
impl Handler<Ask> for AskHandler {
    async fn handle(&self, ask: Ask) -> CatgaResult<u64> {
        Ok(ask.value + 1)
    }
}

struct LogHandler;

#[async_trait::async_trait]
impl CommandHandler<Log> for LogHandler {
    async fn handle(&self, _log: Log) -> CatgaResult<()> {
        Ok(())
    }
}

#[derive(Clone)]
struct BellRecorder {
    id: u8,
    fail_with: Option<&'static str>,
    log: Arc<Mutex<Vec<u8>>>,
}

#[async_trait::async_trait]
impl EventHandler<Bell> for BellRecorder {
    async fn handle(&self, _event: Bell) -> CatgaResult<()> {
        self.log
            .lock()
            .expect("recorder log stays unlocked")
            .push(self.id);
        match self.fail_with {
            Some(message) => Err(CatgaError::new(ErrorCode::Internal, message)),
            None => Ok(()),
        }
    }
}

catga_core::catga_typed_mediator! {
    pub struct FullMediator;
    request Ask => AskHandler;
    command Log => LogHandler;
    event Bell => [BellRecorder, BellRecorder];
}

catga_core::catga_typed_mediator! {
    struct RequestOnlyMediator;
    request Ask => AskHandler;
}

fn recorder(id: u8, fail_with: Option<&'static str>, log: &Arc<Mutex<Vec<u8>>>) -> BellRecorder {
    BellRecorder {
        id,
        fail_with,
        log: Arc::clone(log),
    }
}

#[tokio::test]
async fn typed_mediator_dispatches_all_three_kinds_without_dyn() -> CatgaResult<()> {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mediator = FullMediator::new(
        AskHandler,
        LogHandler,
        [recorder(1, None, &log), recorder(2, None, &log)],
    );

    assert_eq!(mediator.send(Ask { value: 41 }).await?, 42);
    mediator.send_command(Log).await?;
    mediator.publish(Bell).await?;

    assert_eq!(*log.lock().expect("log readable"), vec![1, 2]);
    Ok(())
}

#[tokio::test]
async fn typed_mediator_fans_out_past_first_error_and_returns_it() -> CatgaResult<()> {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mediator = FullMediator::new(
        AskHandler,
        LogHandler,
        [
            recorder(1, Some("first failed"), &log),
            recorder(2, None, &log),
        ],
    );

    let error = mediator
        .publish(Bell)
        .await
        .expect_err("first handler fails");
    assert_eq!(error.message(), "first failed");
    assert_eq!(
        *log.lock().expect("log readable"),
        vec![1, 2],
        "the second handler still receives the event"
    );
    Ok(())
}

#[tokio::test]
async fn typed_mediator_reports_last_handler_error_when_others_succeed() -> CatgaResult<()> {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mediator = FullMediator::new(
        AskHandler,
        LogHandler,
        [
            recorder(1, None, &log),
            recorder(2, Some("second failed"), &log),
        ],
    );

    let error = mediator
        .publish(Bell)
        .await
        .expect_err("second handler fails");
    assert_eq!(error.message(), "second failed");
    assert_eq!(*log.lock().expect("log readable"), vec![1, 2]);
    Ok(())
}

#[tokio::test]
async fn typed_mediator_keeps_the_first_error_when_all_handlers_fail() -> CatgaResult<()> {
    let log = Arc::new(Mutex::new(Vec::new()));
    let mediator = FullMediator::new(
        AskHandler,
        LogHandler,
        [
            recorder(1, Some("first failed"), &log),
            recorder(2, Some("second failed"), &log),
        ],
    );

    let error = mediator
        .publish(Bell)
        .await
        .expect_err("both handlers fail");
    assert_eq!(error.message(), "first failed");
    assert_eq!(*log.lock().expect("log readable"), vec![1, 2]);
    Ok(())
}

#[tokio::test]
async fn typed_mediator_supports_private_single_registration_mediators() -> CatgaResult<()> {
    let mediator = RequestOnlyMediator::new(AskHandler);
    assert_eq!(mediator.send(Ask { value: 1 }).await?, 2);
    Ok(())
}
