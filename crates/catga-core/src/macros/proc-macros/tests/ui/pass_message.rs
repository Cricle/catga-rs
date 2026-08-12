//! A maximal valid `#[derive(Message)]` expansion compiles and honors every option.

use catga_core::{
    AuthorizedRequest, BatchKeyProvider, BatchOptionsProvider, DefaultMessageTypeId, Message,
    MessagePriority, Request,
};

#[derive(catga_core_macros::Message)]
#[catga(
    version = 3,
    priority = critical,
    authorize,
    roles("admin", "ops"),
    policy("payments"),
    batch_key = "account_id",
    batch(
        max_batch_size = 64,
        timeout_ms = 10,
        max_queue_length = 100,
        max_shards = 4,
        flush_concurrency = 2
    ),
    trace_tags(prefix = "custom.", include = ["amount_cents"], exclude = ["secret"], all_public = false)
)]
pub struct Payment {
    #[catga(trace_tag)]
    pub account_id: u64,
    pub amount_cents: u64,
    pub secret: u64,
}

impl Request for Payment {
    type Response = ();
    type TypeId = DefaultMessageTypeId;
}

fn main() {
    let payment = Payment {
        account_id: 7,
        amount_cents: 100,
        secret: 1,
    };

    assert_eq!(payment.schema_version(), 3);
    assert_eq!(payment.priority(), MessagePriority::Critical);

    let requirements = Payment::authorization();
    assert_eq!(requirements.roles(), &["admin", "ops"]);
    assert_eq!(requirements.policy(), Some("payments"));

    assert_eq!(payment.batch_key().as_deref(), Some("7"));

    let options = Payment::batch_options();
    assert_eq!(options.max_batch_size, 64);
    assert_eq!(options.batch_timeout, std::time::Duration::from_millis(10));
    assert_eq!(options.max_queue_length, 100);
    assert_eq!(options.max_shards, 4);
    assert_eq!(options.flush_concurrency, 2);

    let mut tags = Vec::new();
    payment.visit_trace_tags(&mut |name, _| tags.push(name.to_string()));
    tags.sort();
    assert_eq!(tags, ["catga.message.account_id", "custom.amount_cents"]);
}
