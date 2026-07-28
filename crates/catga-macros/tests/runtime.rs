//! Runtime coverage for the public expansions emitted by `catga-macros`.

use std::time::Duration;

use async_trait::async_trait;
use catga_core::{
    AuthorizedRequest, BatchKeyProvider, BatchOptionsProvider, CatgaResult, Command,
    CommandHandler, Event, EventHandler, Handler, Message, MessagePriority, Request,
};
use catga_macros::Message as DeriveMessage;

#[derive(DeriveMessage)]
struct DefaultMessage;

#[derive(DeriveMessage)]
#[catga(version = 7, priority = low)]
struct LowPriority;

#[derive(DeriveMessage)]
#[catga(priority = normal)]
struct NormalPriority;

#[derive(DeriveMessage)]
#[catga(priority = high)]
struct HighPriority;

#[derive(DeriveMessage)]
#[catga(priority = critical)]
struct CriticalPriority;

#[derive(DeriveMessage)]
struct GenericMessage<T>(T);

#[derive(DeriveMessage)]
struct ConstGenericMessage<const LIMIT: usize>;

#[derive(DeriveMessage)]
#[catga(authorize)]
struct Authenticated;

impl Request for Authenticated {
    type Response = ();
}

#[derive(DeriveMessage)]
#[catga(roles("operator"))]
struct RoleProtected;

impl Request for RoleProtected {
    type Response = ();
}

#[derive(DeriveMessage)]
#[catga(policy("orders.write"))]
struct PolicyProtected;

impl Request for PolicyProtected {
    type Response = ();
}

#[derive(DeriveMessage)]
#[catga(authorize, roles("operator", "auditor"), policy("orders.read"))]
struct FullyProtected;

impl Request for FullyProtected {
    type Response = ();
}

#[derive(DeriveMessage)]
#[catga(
    batch_key = "tenant",
    batch(
        max_batch_size = 8,
        timeout_ms = 12,
        max_queue_length = 32,
        max_shards = 4,
        flush_concurrency = 2
    )
)]
struct Batched {
    tenant: u64,
}

#[derive(DeriveMessage)]
#[catga(trace_tags(prefix = "request.", exclude = ["secret"]))]
struct Traced {
    pub id: u64,
    pub region: &'static str,
    secret: &'static str,
    #[catga(trace_tag = "request.explicit")]
    pub account: u64,
}

#[derive(DeriveMessage)]
#[catga(trace_tags(
    prefix = "included.",
    include = ["public", "private"],
    exclude = ["private"],
    all_public = false
))]
struct IncludedTraceTags {
    pub public: u64,
    private: u64,
}

#[derive(DeriveMessage)]
#[catga(trace_tags(all_public = false))]
struct NoTraceTags {
    value: u64,
}

#[derive(DeriveMessage)]
struct TupleMessage(u64);

#[derive(DeriveMessage)]
enum ChoiceMessage {
    First,
    Second,
}

#[derive(DeriveMessage)]
struct MacroRequest;

impl Request for MacroRequest {
    type Response = &'static str;
}

struct RequestHandler;

#[async_trait]
impl Handler<MacroRequest> for RequestHandler {
    async fn handle(&self, _: MacroRequest) -> CatgaResult<&'static str> {
        Ok("ok")
    }
}

#[derive(DeriveMessage)]
struct MacroCommand;

impl Command for MacroCommand {}

struct CommandHandlerImpl;

#[async_trait]
impl CommandHandler<MacroCommand> for CommandHandlerImpl {
    async fn handle(&self, _: MacroCommand) -> CatgaResult<()> {
        Ok(())
    }
}

#[derive(Clone, DeriveMessage)]
struct MacroEvent;

impl Event for MacroEvent {}

struct FirstEventHandler;
struct SecondEventHandler;

#[async_trait]
impl EventHandler<MacroEvent> for FirstEventHandler {
    async fn handle(&self, _: MacroEvent) -> CatgaResult<()> {
        Ok(())
    }
}

#[async_trait]
impl EventHandler<MacroEvent> for SecondEventHandler {
    async fn handle(&self, _: MacroEvent) -> CatgaResult<()> {
        Ok(())
    }
}

#[test]
fn message_expansion_preserves_defaults_generics_versions_and_priorities() {
    assert_eq!(
        DefaultMessage.message_type(),
        std::any::type_name::<DefaultMessage>()
    );
    assert_eq!(DefaultMessage.schema_version(), 1);
    assert_eq!(
        GenericMessage(3_u64).message_type(),
        std::any::type_name::<GenericMessage<u64>>()
    );
    assert_eq!(ConstGenericMessage::<4>.schema_version(), 1);
    assert_eq!(LowPriority.schema_version(), 7);
    assert_eq!(LowPriority.priority(), MessagePriority::Low);
    assert_eq!(NormalPriority.priority(), MessagePriority::Normal);
    assert_eq!(HighPriority.priority(), MessagePriority::High);
    assert_eq!(CriticalPriority.priority(), MessagePriority::Critical);
    let tuple = TupleMessage(1);
    assert_eq!(tuple.0, 1);
    assert_eq!(tuple.schema_version(), 1);
    assert!(matches!(ChoiceMessage::First, ChoiceMessage::First));
    assert!(matches!(ChoiceMessage::Second, ChoiceMessage::Second));
}

#[test]
fn message_expansion_builds_authorization_and_batch_contracts() {
    assert!(Authenticated::authorization().roles().is_empty());
    assert_eq!(Authenticated::authorization().policy(), None);
    assert_eq!(RoleProtected::authorization().roles(), ["operator"]);
    assert_eq!(RoleProtected::authorization().policy(), None);
    assert!(PolicyProtected::authorization().roles().is_empty());
    assert_eq!(
        PolicyProtected::authorization().policy(),
        Some("orders.write")
    );
    assert_eq!(
        FullyProtected::authorization().roles(),
        ["operator", "auditor"]
    );
    assert_eq!(
        FullyProtected::authorization().policy(),
        Some("orders.read")
    );

    let message = Batched { tenant: 42 };
    assert_eq!(message.batch_key().as_deref(), Some("42"));
    let options = Batched::batch_options();
    assert_eq!(options.max_batch_size, 8);
    assert_eq!(options.batch_timeout, Duration::from_millis(12));
    assert_eq!(options.max_queue_length, 32);
    assert_eq!(options.max_shards, 4);
    assert_eq!(options.flush_concurrency, 2);
}

#[test]
fn message_expansion_respects_explicit_and_bulk_trace_tags() {
    let message = Traced {
        id: 9,
        region: "cn-north-1",
        secret: "do-not-export",
        account: 44,
    };
    let mut tags = Vec::new();
    message.visit_trace_tags(&mut |name, value| {
        tags.push((name.to_owned(), value.to_string()));
    });
    assert_eq!(
        tags,
        [
            ("request.explicit".to_owned(), "44".to_owned()),
            ("request.id".to_owned(), "9".to_owned()),
            ("request.region".to_owned(), "cn-north-1".to_owned()),
        ]
    );
    assert_eq!(message.secret, "do-not-export");

    let included = IncludedTraceTags {
        public: 5,
        private: 6,
    };
    let mut included_tags = Vec::new();
    included.visit_trace_tags(&mut |name, value| {
        included_tags.push((name.to_owned(), value.to_string()));
    });
    assert_eq!(
        included_tags,
        [("included.public".to_owned(), "5".to_owned())]
    );
    assert_eq!(included.private, 6);

    let without_tags = NoTraceTags { value: 7 };
    let mut no_tags = Vec::new();
    without_tags.visit_trace_tags(&mut |name, value| {
        no_tags.push((name.to_owned(), value.to_string()));
    });
    assert!(no_tags.is_empty());
    assert_eq!(without_tags.value, 7);
}

#[test]
fn handler_expansion_registers_request_command_and_multiple_event_handlers() {
    let registry = catga_macros::catga_handlers! {
        request MacroRequest => RequestHandler;
        command MacroCommand => CommandHandlerImpl;
        event MacroEvent => [FirstEventHandler, SecondEventHandler];
    }
    .expect("unique registrations build a registry");
    drop(registry);
}
