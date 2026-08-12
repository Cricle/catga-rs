//! Contract coverage for bounded W3C trace-context propagation.

use catga_core::{
    EnvelopeHeaders, TRACEPARENT_HEADER, TRACESTATE_HEADER, TraceContext, assert_success,
};

const VALID_PARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

fn headers(entries: &[(&str, &str)]) -> EnvelopeHeaders {
    assert_success(EnvelopeHeaders::try_new(entries.iter().copied()))
}

#[test]
fn trace_context_parses_valid_values_and_rejects_invalid_ones() {
    let context =
        TraceContext::parse(VALID_PARENT, Some("congo=t61rcWkgMzE")).expect("valid traceparent");
    assert_eq!(context.traceparent(), VALID_PARENT);
    assert_eq!(context.tracestate(), Some("congo=t61rcWkgMzE"));
    assert!(format!("{context:?}").contains("TraceContext"));
    assert_eq!(context.clone(), context);

    // No tracestate is a valid parent context.
    let bare = TraceContext::parse(VALID_PARENT, None).expect("valid");
    assert_eq!(bare.tracestate(), None);

    // Structurally invalid parents are rejected.
    assert!(TraceContext::parse("invalid", None).is_none());
    assert!(TraceContext::parse("", None).is_none());
    // Uppercase hex is non-canonical.
    assert!(
        TraceContext::parse(
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
            None
        )
        .is_none()
    );
    // Version ff is reserved.
    assert!(
        TraceContext::parse(
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            None
        )
        .is_none()
    );
    // All-zero trace identifiers are invalid.
    assert!(
        TraceContext::parse(
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            None
        )
        .is_none()
    );
    assert!(
        TraceContext::parse(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            None
        )
        .is_none()
    );
    // Version 00 must not carry trailing extensions.
    assert!(
        TraceContext::parse(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
            None
        )
        .is_none()
    );
    // Future versions may carry one graphic, dash-free extension segment.
    let extended = TraceContext::parse(
        "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-ext1",
        None,
    );
    assert!(extended.is_some());
    // But the extension must not be empty and must not contain dashes.
    assert!(
        TraceContext::parse(
            "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-",
            None
        )
        .is_none()
    );
    assert!(
        TraceContext::parse(
            "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-a-b",
            None
        )
        .is_none()
    );
    // Oversized parents are rejected by the byte bound.
    let oversized = format!("{VALID_PARENT}{}", "x".repeat(600));
    assert!(TraceContext::parse(&oversized, None).is_none());
}

#[test]
fn trace_context_discards_invalid_tracestate_but_keeps_the_parent() {
    // Whitespace-only and malformed members are dropped.
    let dropped = TraceContext::parse(VALID_PARENT, Some("   ")).expect("valid parent");
    assert_eq!(dropped.tracestate(), None);
    let malformed = TraceContext::parse(VALID_PARENT, Some("no-equals-sign")).expect("parent");
    assert_eq!(malformed.tracestate(), None);
    // Duplicate keys are invalid.
    let duplicate = TraceContext::parse(VALID_PARENT, Some("a=1,a=2")).expect("parent");
    assert_eq!(duplicate.tracestate(), None);
    // Too many members are invalid.
    let many_members: Vec<String> = (0..33).map(|index| format!("k{index}=v")).collect();
    let too_many =
        TraceContext::parse(VALID_PARENT, Some(&many_members.join(","))).expect("parent");
    assert_eq!(too_many.tracestate(), None);
    // Non-ASCII values are invalid.
    let non_ascii = TraceContext::parse(VALID_PARENT, Some("a=\u{00e9}")).expect("parent");
    assert_eq!(non_ascii.tracestate(), None);
    // Oversized values are invalid.
    let huge_value = format!("a={}", "v".repeat(600));
    let oversized = TraceContext::parse(VALID_PARENT, Some(&huge_value)).expect("parent");
    assert_eq!(oversized.tracestate(), None);
    // Keys with two @ sections are invalid; one is allowed.
    let bad_key = TraceContext::parse(VALID_PARENT, Some("a@b@c=1")).expect("parent");
    assert_eq!(bad_key.tracestate(), None);
    let vendored = TraceContext::parse(VALID_PARENT, Some("vendor@tenant=1")).expect("parent");
    assert_eq!(vendored.tracestate(), Some("vendor@tenant=1"));
    // Uppercase keys are invalid.
    let uppercase = TraceContext::parse(VALID_PARENT, Some("Key=1")).expect("parent");
    assert_eq!(uppercase.tracestate(), None);
    // Valid multi-member state round-trims.
    let multi = TraceContext::parse(VALID_PARENT, Some("a=1, b=2")).expect("parent");
    assert_eq!(multi.tracestate(), Some("a=1, b=2"));
}

#[test]
fn trace_context_extracts_from_envelope_headers_case_insensitively() {
    let mixed_case = headers(&[
        ("TraceParent", VALID_PARENT),
        ("TRACESTATE", "congo=t61rcWkgMzE"),
        ("x-other", "keep"),
    ]);
    let context = TraceContext::from_envelope_headers(&mixed_case).expect("headers carry parent");
    assert_eq!(context.traceparent(), VALID_PARENT);
    assert_eq!(context.tracestate(), Some("congo=t61rcWkgMzE"));

    // Missing parent headers yield nothing.
    let none = headers(&[("x-other", "keep")]);
    assert!(TraceContext::from_envelope_headers(&none).is_none());

    // An invalid parent header yields nothing even with a valid tracestate.
    let bad_parent = headers(&[(TRACEPARENT_HEADER, "garbage"), (TRACESTATE_HEADER, "a=1")]);
    assert!(TraceContext::from_envelope_headers(&bad_parent).is_none());

    // An invalid tracestate is dropped while the parent survives.
    let bad_state = headers(&[
        (TRACEPARENT_HEADER, VALID_PARENT),
        (TRACESTATE_HEADER, "@@@"),
    ]);
    let rescued = TraceContext::from_envelope_headers(&bad_state).expect("parent valid");
    assert_eq!(rescued.tracestate(), None);
}

#[test]
fn trace_context_injects_into_headers_without_losing_application_entries() {
    let context = TraceContext::parse(VALID_PARENT, Some("congo=t61rcWkgMzE")).expect("valid");

    // Injection into nothing produces exactly the W3C pair.
    let fresh = assert_success(context.inject_into_envelope_headers(None));
    assert_eq!(fresh.get(TRACEPARENT_HEADER), Some(VALID_PARENT));
    assert_eq!(fresh.get(TRACESTATE_HEADER), Some("congo=t61rcWkgMzE"));
    assert_eq!(fresh.len(), 2);

    // Existing W3C values (any casing) are replaced, other headers retained.
    let existing = headers(&[
        ("Traceparent", "stale-parent"),
        ("tracestate", "stale=1"),
        ("x-tenant", "acme"),
    ]);
    let merged = assert_success(context.inject_into_envelope_headers(Some(&existing)));
    assert_eq!(merged.get(TRACEPARENT_HEADER), Some(VALID_PARENT));
    assert_eq!(merged.get(TRACESTATE_HEADER), Some("congo=t61rcWkgMzE"));
    assert_eq!(merged.get("x-tenant"), Some("acme"));
    assert_eq!(merged.len(), 3);

    // A context without tracestate injects only the parent header.
    let bare = TraceContext::parse(VALID_PARENT, None).expect("valid");
    let bare_headers = assert_success(bare.inject_into_envelope_headers(None));
    assert_eq!(bare_headers.len(), 1);
    assert!(bare_headers.get(TRACESTATE_HEADER).is_none());
}

#[test]
fn trace_context_converts_to_transport_contexts() {
    let with_state = TraceContext::parse(VALID_PARENT, Some("congo=t61rcWkgMzE")).expect("valid");
    let transport = assert_success(with_state.to_transport_context());
    let transport_headers = transport.headers().expect("headers carried");
    assert_eq!(
        transport_headers.get(TRACEPARENT_HEADER),
        Some(VALID_PARENT)
    );
    assert_eq!(
        transport_headers.get(TRACESTATE_HEADER),
        Some("congo=t61rcWkgMzE")
    );

    let bare = TraceContext::parse(VALID_PARENT, None).expect("valid");
    let bare_transport = assert_success(bare.to_transport_context());
    let bare_headers = bare_transport.headers().expect("headers carried");
    assert_eq!(bare_headers.get(TRACEPARENT_HEADER), Some(VALID_PARENT));
    assert_eq!(bare_headers.len(), 1);
}
