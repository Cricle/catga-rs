//! Macro ergonomics tests.

use std::time::Duration;

use async_trait::async_trait;
use catga_core::{
    AuthorizedRequest, AutoBatchingBehavior, BatchKeyProvider, BatchOptionsProvider, CatgaResult,
    Event, EventHandler, Handler, Mediator, Message, MessagePriority, Pipeline, Request,
    RetryBehavior, TimeoutBehavior,
};
use catga_flow::{FlowDefinition, FlowStepOutcome};

#[derive(Clone, catga_core::Message)]
struct Ping;

#[derive(catga_core::Message)]
#[catga(version = 2, priority = high)]
struct VersionedPing;

#[derive(catga_core::Message)]
struct GenericPing<T>(T);

impl Request for Ping {
    type Response = &'static str;
}

#[derive(catga_core::Message)]
#[catga(
    authorize,
    roles("administrator", "operator"),
    policy("manage-reports")
)]
struct ManageReport;

impl Request for ManageReport {
    type Response = ();
}

#[derive(catga_core::Message)]
#[catga(batch_key = "account_id", batch(max_batch_size = 2))]
struct AccountScopedRequest {
    account_id: u64,
}

impl Request for AccountScopedRequest {
    type Response = ();
}

#[derive(catga_core::Message)]
#[catga(batch(
    max_batch_size = 16,
    timeout_ms = 25,
    max_queue_length = 256,
    max_shards = 64,
    flush_concurrency = 4
))]
struct ConfiguredBatchRequest;

impl Request for ConfiguredBatchRequest {
    type Response = ();
}

#[derive(catga_core::Message)]
struct TracedRequest {
    #[catga(trace_tag = "order.id")]
    order_id: u64,
    #[catga(trace_tag)]
    region: &'static str,
    #[allow(dead_code)]
    secret: &'static str,
}

impl Request for TracedRequest {
    type Response = ();
}

#[derive(catga_core::Message)]
#[catga(trace_tags(
    prefix = "checkout.",
    include = ["order_id", "region", "secret"],
    exclude = ["secret"]
))]
struct BulkTracedRequest {
    #[catga(trace_tag = "order.id")]
    order_id: u64,
    region: &'static str,
    secret: &'static str,
    ignored: &'static str,
}

struct PingHandler;

#[async_trait]
impl Handler<Ping> for PingHandler {
    async fn handle(&self, _: Ping) -> CatgaResult<&'static str> {
        Ok("pong")
    }
}

struct ConfiguredPingHandler {
    response: &'static str,
}

#[async_trait]
impl Handler<Ping> for ConfiguredPingHandler {
    async fn handle(&self, _: Ping) -> CatgaResult<&'static str> {
        Ok(self.response)
    }
}

#[derive(Clone, catga_core::Message)]
struct Notified;

impl Event for Notified {}

struct Noop;

#[async_trait]
impl EventHandler<Notified> for Noop {
    async fn handle(&self, _: Notified) -> CatgaResult<()> {
        Ok(())
    }
}

#[tokio::test]
async fn derive_and_registration_macros_keep_setup_explicit_and_short() {
    let registry = catga_core::catga_handlers! {
        request Ping => PingHandler;
        event Notified => [Noop];
    }
    .expect("unique registrations must build a registry");
    let mediator = Mediator::new(registry);

    assert_eq!(Ping.message_type(), "Ping");
    assert_eq!(mediator.send(Ping).await.unwrap(), "pong");
    mediator.publish(Notified).await.unwrap();
}

#[tokio::test]
async fn registration_macro_accepts_a_configured_handler_expression() {
    let registry = catga_core::catga_handlers! {
        request Ping => ConfiguredPingHandler { response: "configured" };
    }
    .expect("configured handler registration must build a registry");

    assert_eq!(
        Mediator::new(registry).send(Ping).await.unwrap(),
        "configured"
    );
}

#[test]
fn message_derive_supports_send_sync_static_generic_payloads() {
    assert_eq!(GenericPing(7_u64).message_type(), "GenericPing");
    assert_eq!(VersionedPing.schema_version(), 2);
    assert_eq!(VersionedPing.priority(), MessagePriority::High);
}

#[test]
fn flow_definition_macro_registers_named_async_steps() {
    let definition: FlowDefinition = catga_flow::flow_definition! {
        "macro-flow";
        "choose" => |_| async { Ok(FlowStepOutcome::goto("done")) };
        "done" => |_| async { Ok(FlowStepOutcome::complete()) };
    };

    assert_eq!(definition.name(), "macro-flow");
}

#[test]
fn message_derive_can_declare_static_authorization_without_runtime_registration() {
    let requirements = ManageReport::authorization();

    assert_eq!(requirements.roles(), ["administrator", "operator"]);
    assert_eq!(requirements.policy(), Some("manage-reports"));
}

#[test]
fn message_derive_can_generate_a_batch_key_from_a_named_field() {
    let message = AccountScopedRequest { account_id: 42 };

    assert_eq!(message.batch_key().as_deref(), Some("42"));
    assert!(AutoBatchingBehavior::<AccountScopedRequest>::from_message_options_with_key().is_ok());
}

#[test]
fn message_derive_can_configure_automatic_batching_without_runtime_registration() {
    let options = ConfiguredBatchRequest::batch_options();

    assert_eq!(options.max_batch_size, 16);
    assert_eq!(options.batch_timeout, Duration::from_millis(25));
    assert_eq!(options.max_queue_length, 256);
    assert_eq!(options.max_shards, 64);
    assert_eq!(options.flush_concurrency, 4);
    assert!(AutoBatchingBehavior::<ConfiguredBatchRequest>::from_message_options().is_ok());
}

#[test]
fn pipeline_macro_builds_one_bounded_startup_pipeline_from_existing_behaviors() {
    let pipeline: Pipeline<Ping> = catga_core::catga_pipeline!(
        Ping;
        RetryBehavior::new(2, Duration::ZERO),
        TimeoutBehavior::new(Duration::from_secs(1)),
    )
    .expect("configured pipeline fits the supported depth");

    assert_eq!(pipeline.len(), 2);
}

#[test]
fn message_derive_exports_only_explicit_trace_tags_without_allocating_tag_names() {
    let request = TracedRequest {
        order_id: 42,
        region: "cn-north-1",
        secret: "never-exported",
    };
    let mut tags = Vec::new();

    request.visit_trace_tags(&mut |name, value| {
        tags.push((name.to_owned(), value.to_string()));
    });

    assert_eq!(
        tags,
        [
            ("order.id".to_owned(), "42".to_owned()),
            ("catga.message.region".to_owned(), "cn-north-1".to_owned()),
        ]
    );
}

#[test]
fn message_derive_bulk_trace_tags_applies_include_exclude_and_prefix_rules() {
    let request = BulkTracedRequest {
        order_id: 42,
        region: "cn-north-1",
        secret: "never-exported",
        ignored: "not-included",
    };
    assert_eq!(request.secret, "never-exported");
    assert_eq!(request.ignored, "not-included");
    let mut tags = Vec::new();

    request.visit_trace_tags(&mut |name, value| {
        tags.push((name.to_owned(), value.to_string()));
    });

    assert_eq!(
        tags,
        [
            ("order.id".to_owned(), "42".to_owned()),
            ("checkout.region".to_owned(), "cn-north-1".to_owned()),
        ]
    );
}
