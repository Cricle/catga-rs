//! Message contract tests.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use catga_core::{
    DeliveryMode, Envelope, EnvelopeHeaders, ErrorCode, MAX_ENVELOPE_HEADER_BYTES,
    MAX_ENVELOPE_HEADERS, Message, MessageMetadata, MessagePriority, QualityOfService, Request,
    TraceContext,
};

#[derive(Debug)]
struct CreateOrder {
    id: u64,
}

impl Message for CreateOrder {}

impl Request for CreateOrder {
    type Response = u64;
}

#[test]
fn request_metadata_preserves_message_and_correlation_ids() {
    let metadata = MessageMetadata::new(11, Some(3));

    assert_eq!(metadata.message_id(), 11);
    assert_eq!(metadata.correlation_id(), Some(3));
    let request = CreateOrder { id: 1 };
    assert_eq!(request.id, 1);
    assert!(request.message_type().ends_with("CreateOrder"));
}

#[test]
fn metadata_uses_reliable_normal_defaults_and_can_carry_delivery_options() {
    let metadata = MessageMetadata::new(11, Some(3));

    assert_eq!(metadata.quality_of_service(), QualityOfService::AtLeastOnce);
    assert_eq!(metadata.delivery_mode(), DeliveryMode::WaitForResult);
    assert_eq!(metadata.priority(), MessagePriority::Normal);
    assert!(metadata.quality_of_service().requires_ack());
    assert!(!metadata.quality_of_service().requires_deduplication());

    let configured = metadata
        .with_quality_of_service(QualityOfService::ExactlyOnce)
        .with_delivery_mode(DeliveryMode::AsyncRetry)
        .with_priority(MessagePriority::Critical);
    assert_eq!(
        configured.quality_of_service(),
        QualityOfService::ExactlyOnce
    );
    assert_eq!(configured.delivery_mode(), DeliveryMode::AsyncRetry);
    assert_eq!(configured.priority(), MessagePriority::Critical);
    assert!(configured.quality_of_service().requires_deduplication());
}

#[test]
fn envelope_headers_are_immutable_ordered_and_shareable() {
    let headers = EnvelopeHeaders::try_new([("tenant", "blue"), ("route", "priority")])
        .expect("valid headers are accepted");
    let envelope = Envelope::new(7, "order.created", vec![], MessageMetadata::new(7, None))
        .with_headers(headers.clone());

    assert_eq!(headers.get("tenant"), Some("blue"));
    assert_eq!(envelope.header("route"), Some("priority"));
    assert_eq!(
        envelope.headers().collect::<Vec<_>>(),
        vec![("tenant", "blue"), ("route", "priority")]
    );
    assert_eq!(
        envelope.clone().headers().collect::<Vec<_>>(),
        vec![("tenant", "blue"), ("route", "priority")]
    );
    assert!(
        Envelope::new(8, "order.created", vec![], MessageMetadata::new(8, None))
            .headers()
            .next()
            .is_none()
    );
}

#[test]
fn envelope_header_merge_overrides_existing_keys_and_preserves_resource_limits() {
    let inherited = EnvelopeHeaders::try_new([("tenant", "blue"), ("route", "priority")])
        .expect("valid inherited headers");
    let explicit = EnvelopeHeaders::try_new([("tenant", "green"), ("role", "worker")])
        .expect("valid explicit headers");
    let merged = inherited
        .merge_overrides(&explicit)
        .expect("bounded headers merge");

    assert_eq!(
        merged.iter().collect::<Vec<_>>(),
        vec![
            ("tenant", "green"),
            ("route", "priority"),
            ("role", "worker"),
        ]
    );

    let full = EnvelopeHeaders::try_new(
        (0..MAX_ENVELOPE_HEADERS).map(|index| (format!("header-{index}"), String::from("x"))),
    )
    .expect("header set reaches the configured limit");
    let extra = EnvelopeHeaders::try_new([("extra", "x")]).expect("valid extra header");
    let error = full
        .merge_overrides(&extra)
        .expect_err("merged headers retain the configured limit");

    assert_eq!(error.code(), ErrorCode::Validation);
}

#[test]
fn trace_context_round_trips_through_bounded_envelope_headers() {
    let context = TraceContext::parse(
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        Some("vendor=state"),
    )
    .expect("a valid W3C context is accepted");
    let headers = EnvelopeHeaders::try_new([
        ("tenant", "blue"),
        (
            "traceparent",
            "00-00000000000000000000000000000000-0000000000000000-00",
        ),
    ])
    .expect("valid application headers");

    let propagated = context
        .inject_into_envelope_headers(Some(&headers))
        .expect("trace headers stay within the envelope budget");
    let extracted = TraceContext::from_envelope_headers(&propagated)
        .expect("the injected context is extracted");

    assert_eq!(propagated.get("tenant"), Some("blue"));
    assert_eq!(extracted, context);
}

#[test]
fn trace_context_parses_w3c_grammar_and_drops_invalid_tracestate() {
    let traceparent = "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra";
    let context = TraceContext::parse(traceparent, Some("\t1tenant@system= state, vendor1=ok\t"))
        .expect("valid traceparent and multi-tenant tracestate are accepted");
    assert_eq!(
        context.tracestate(),
        Some("\t1tenant@system= state, vendor1=ok\t")
    );

    let oversized = "a=b".repeat(300);
    for invalid_tracestate in [
        "1vendor=invalid",
        "tenant@1system=invalid",
        "vendor =invalid",
        "vendor=contains\ttab",
        "vendor=state,,next=value",
        "",
        " \t ",
        oversized.as_str(),
    ] {
        let context = TraceContext::parse(traceparent, Some(invalid_tracestate))
            .expect("an invalid tracestate does not invalidate its traceparent");
        assert_eq!(context.tracestate(), None);
    }
    assert!(
        TraceContext::parse(
            "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-",
            None,
        )
        .is_none()
    );
    assert!(TraceContext::parse("not-a-traceparent", None).is_none());
    assert!(
        TraceContext::parse(
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            None,
        )
        .is_none()
    );
}

#[test]
fn envelope_headers_reject_invalid_and_unbounded_input() {
    let duplicate = EnvelopeHeaders::try_new([("tenant", "blue"), ("tenant", "green")])
        .expect_err("duplicate keys are rejected");
    assert_eq!(duplicate.code(), ErrorCode::Validation);

    let blank = EnvelopeHeaders::try_new([("", "blue")]).expect_err("blank keys are rejected");
    assert_eq!(blank.code(), ErrorCode::Validation);

    let too_many = EnvelopeHeaders::try_new(
        (0..=MAX_ENVELOPE_HEADERS).map(|index| (format!("header-{index}"), String::from("x"))),
    )
    .expect_err("header count is bounded");
    assert_eq!(too_many.code(), ErrorCode::Validation);

    let too_large = EnvelopeHeaders::try_new([(
        String::from("header"),
        "x".repeat(MAX_ENVELOPE_HEADER_BYTES),
    )])
    .expect_err("header bytes are bounded");
    assert_eq!(too_large.code(), ErrorCode::Validation);
}

#[test]
fn envelope_sent_at_is_automatic_overridable_and_validated() {
    let before = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the test clock is after the Unix epoch")
        .as_millis();
    let envelope = Envelope::new(9, "order.created", vec![], MessageMetadata::new(9, None));
    let after = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the test clock is after the Unix epoch")
        .as_millis();
    let observed = envelope
        .sent_at_unix_ms()
        .expect("new envelopes have a sent timestamp");
    assert!(u128::from(observed) >= before && u128::from(observed) <= after);

    let epoch = Envelope::new(10, "order.created", vec![], MessageMetadata::new(10, None))
        .with_sent_at(UNIX_EPOCH)
        .expect("the Unix epoch is representable");
    assert_eq!(epoch.sent_at_unix_ms(), Some(0));
    assert_eq!(epoch.sent_at(), Some(UNIX_EPOCH));

    let pre_epoch = UNIX_EPOCH
        .checked_sub(Duration::from_millis(1))
        .expect("one millisecond before epoch is representable");
    let error = Envelope::new(11, "order.created", vec![], MessageMetadata::new(11, None))
        .with_sent_at(pre_epoch)
        .expect_err("pre-epoch timestamps are rejected");
    assert_eq!(error.code(), ErrorCode::Validation);
}
