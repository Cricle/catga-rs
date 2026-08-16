//! Behavioral contracts for `#[derive(Message)]`: schema versions, priorities, authorization,
//! batch keys, batch options, and trace tags.

use std::time::Duration;

use catga_core::{
    AuthorizationRequirements, AuthorizedRequest, BatchKeyProvider, BatchOptionsProvider, Message,
    MessagePriority, Request,
};

#[derive(Message)]
struct Bare;

fn assert_message<M: Message>(message: &M) {
    assert_eq!(message.message_type(), std::any::type_name::<M>());
}

fn collect_tags(message: &impl Message) -> Vec<(String, String)> {
    let mut tags = Vec::new();
    message.visit_trace_tags(&mut |name, value| {
        tags.push((name.to_owned(), value.to_string()));
    });
    tags
}

#[test]
fn bare_derive_keeps_trait_defaults() {
    let message = Bare;
    assert_message(&message);
    assert_eq!(message.schema_version(), 1);
    assert_eq!(message.priority(), MessagePriority::Normal);
    assert!(collect_tags(&message).is_empty());
}

#[derive(Message)]
#[catga(version = 7, priority = critical)]
pub struct VersionedCritical;

#[derive(Message)]
#[catga(priority = low)]
pub struct LowPriority;

#[derive(Message)]
#[catga(priority = normal)]
pub struct NormalPriority;

#[derive(Message)]
#[catga(priority = high)]
pub struct HighPriority;

#[test]
fn version_and_priority_overrides_apply() {
    assert_eq!(VersionedCritical.schema_version(), 7);
    assert_eq!(VersionedCritical.priority(), MessagePriority::Critical);
    assert_eq!(LowPriority.priority(), MessagePriority::Low);
    assert_eq!(NormalPriority.priority(), MessagePriority::Normal);
    assert_eq!(HighPriority.priority(), MessagePriority::High);
}

#[derive(Message)]
#[catga(version = 3)]
#[catga(priority = high)]
pub struct SplitAttributes;

#[test]
fn options_split_across_multiple_catga_attributes_merge() {
    assert_eq!(SplitAttributes.schema_version(), 3);
    assert_eq!(SplitAttributes.priority(), MessagePriority::High);
}

#[derive(Message)]
#[catga(authorize)]
pub struct SecurePing;

impl Request for SecurePing {
    type Response = ();
}

#[derive(Message)]
#[catga(roles("admin", "ops"))]
pub struct RolePing;

impl Request for RolePing {
    type Response = ();
}

#[derive(Message)]
#[catga(policy("billing"))]
pub struct PolicyPing;

impl Request for PolicyPing {
    type Response = ();
}

#[derive(Message)]
#[catga(roles("admin"), policy("billing"))]
pub struct RolePolicyPing;

impl Request for RolePolicyPing {
    type Response = ();
}

#[test]
fn authorization_forms_map_to_requirement_constructors() {
    assert_eq!(
        SecurePing::authorization(),
        AuthorizationRequirements::authenticated()
    );

    let roles = RolePing::authorization();
    assert_eq!(
        roles,
        AuthorizationRequirements::with_roles(&["admin", "ops"])
    );
    assert_eq!(roles.roles(), &["admin", "ops"]);
    assert_eq!(roles.policy(), None);

    let policy = PolicyPing::authorization();
    assert_eq!(policy, AuthorizationRequirements::with_policy("billing"));
    assert!(policy.roles().is_empty());
    assert_eq!(policy.policy(), Some("billing"));

    let both = RolePolicyPing::authorization();
    assert_eq!(
        both,
        AuthorizationRequirements::with_roles_and_policy(&["admin"], "billing")
    );
    assert_eq!(both.roles(), &["admin"]);
    assert_eq!(both.policy(), Some("billing"));
}

#[derive(Message)]
#[catga(batch_key = "tenant")]
pub struct Sharded {
    pub tenant: String,
}

#[test]
fn batch_key_stringifies_the_named_field() {
    let message = Sharded {
        tenant: "tenant-9".to_owned(),
    };
    assert_eq!(message.batch_key(), Some("tenant-9".into()));
    assert_eq!(message.tenant, "tenant-9");
}

#[derive(Message)]
#[catga(batch(
    max_batch_size = 8,
    timeout_ms = 250,
    max_queue_length = 128,
    max_shards = 4,
    flush_concurrency = 2
))]
pub struct Batched;

#[test]
fn batch_options_apply_every_declared_limit() {
    let options = Batched::batch_options();
    assert_eq!(options.max_batch_size, 8);
    assert_eq!(options.batch_timeout, Duration::from_millis(250));
    assert_eq!(options.max_queue_length, 128);
    assert_eq!(options.max_shards, 4);
    assert_eq!(options.flush_concurrency, 2);
}

#[derive(Message)]
pub struct FieldTags {
    /// Order identifier exported under the default tag prefix.
    #[catga(trace_tag)]
    pub order_id: u64,
    #[catga(trace_tag = "custom.amount")]
    pub amount_cents: u32,
    pub hidden: u8,
}

#[test]
fn field_trace_tags_use_default_and_custom_names() {
    let message = FieldTags {
        order_id: 7,
        amount_cents: 250,
        hidden: 3,
    };
    assert_eq!(message.hidden, 3);
    assert_eq!(
        collect_tags(&message),
        [
            ("catga.message.order_id".to_owned(), "7".to_owned()),
            ("custom.amount".to_owned(), "250".to_owned()),
        ]
    );
}

#[derive(Message)]
#[catga(trace_tags(prefix = "order.", include = ["state", "region"], exclude = ["region"], all_public = false))]
pub struct BulkSelected {
    pub state: u32,
    pub region: u32,
    pub other: u32,
}

#[test]
fn bulk_trace_tags_honor_include_exclude_and_prefix() {
    let message = BulkSelected {
        state: 1,
        region: 2,
        other: 3,
    };
    assert_eq!(message.region, 2);
    assert_eq!(message.other, 3);
    assert_eq!(
        collect_tags(&message),
        [("order.state".to_owned(), "1".to_owned())]
    );
}

#[derive(Message)]
#[catga(trace_tags(exclude = ["skipped"]))]
pub struct PublicByDefault {
    pub tagged: u8,
    pub skipped: u8,
}

#[test]
fn bulk_trace_tags_default_to_all_public_fields() {
    let message = PublicByDefault {
        tagged: 9,
        skipped: 10,
    };
    assert_eq!(message.skipped, 10);
    assert_eq!(
        collect_tags(&message),
        [("catga.message.tagged".to_owned(), "9".to_owned())]
    );
}

#[derive(Message)]
#[catga(trace_tags(prefix = "mixed."))]
pub struct ExplicitWinsOverBulk {
    #[catga(trace_tag = "explicit.one")]
    pub one: u8,
    pub two: u8,
}

#[test]
fn explicit_field_tags_take_precedence_over_bulk_tags() {
    let message = ExplicitWinsOverBulk { one: 1, two: 2 };
    assert_eq!(
        collect_tags(&message),
        [
            ("explicit.one".to_owned(), "1".to_owned()),
            ("mixed.two".to_owned(), "2".to_owned()),
        ]
    );
}

#[derive(Message)]
#[repr(C)]
#[catga(
    version = 2,
    priority = high,
    authorize,
    roles("admin"),
    policy("ops"),
    batch_key = "tenant",
    batch(max_batch_size = 4),
    trace_tags(prefix = "sink.")
)]
pub struct KitchenSink {
    pub tenant: String,
    pub load: u64,
}

impl Request for KitchenSink {
    type Response = ();
}

#[test]
fn every_option_form_combines_in_one_attribute() {
    let message = KitchenSink {
        tenant: "t-1".to_owned(),
        load: 64,
    };
    assert_eq!(message.schema_version(), 2);
    assert_eq!(message.priority(), MessagePriority::High);
    let auth = KitchenSink::authorization();
    assert_eq!(auth.roles(), &["admin"]);
    assert_eq!(auth.policy(), Some("ops"));
    assert_eq!(message.batch_key(), Some("t-1".into()));
    assert_eq!(KitchenSink::batch_options().max_batch_size, 4);
    assert_eq!(
        collect_tags(&message),
        [
            ("sink.tenant".to_owned(), "t-1".to_owned()),
            ("sink.load".to_owned(), "64".to_owned()),
        ]
    );
}

#[derive(Message)]
pub struct GenericPayload<T> {
    pub payload: T,
}

#[derive(Message)]
pub struct SizedPayload<const N: usize> {
    pub data: [u8; N],
}

#[derive(Message)]
pub struct BorrowedPayload<'a>
where
    'a: 'static,
{
    pub text: &'a str,
}

#[test]
fn generics_gain_message_bounds_and_keep_kind_specifics() {
    let generic = GenericPayload { payload: 5u32 };
    assert_message(&generic);
    assert_eq!(generic.payload, 5);

    let sized = SizedPayload::<2> { data: [1, 2] };
    assert_message(&sized);
    assert_eq!(sized.data, [1, 2]);

    let borrowed = BorrowedPayload { text: "hi" };
    assert_message(&borrowed);
    assert_eq!(borrowed.text, "hi");
}

#[derive(Message)]
pub enum EitherMessage {
    Left,
    Right,
}

#[derive(Message)]
pub struct TupleMessage(pub u64);

#[test]
fn non_struct_and_tuple_messages_export_no_trace_tags() {
    assert_eq!(EitherMessage::Left.schema_version(), 1);
    assert!(collect_tags(&EitherMessage::Right).is_empty());
    assert_eq!(TupleMessage(4).schema_version(), 1);
    assert!(collect_tags(&TupleMessage(4)).is_empty());
    assert_eq!(TupleMessage(4).0, 4);
}
