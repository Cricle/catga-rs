//! Header-driven destination routing tests.

use catga_core::{CatgaResult, Destination, EnvelopeHeaders, ErrorCode, MessageRouter};

#[test]
fn message_router_validates_route_headers_and_retains_the_first_matching_rule() -> CatgaResult<()> {
    let mut router = MessageRouter::new(Some(Destination::parse("fallback")?));
    router.add_route("tenant", "gold", Destination::parse("priority-orders")?)?;
    router.add_route("tenant", "gold", Destination::parse("secondary-orders")?)?;
    router.add_route("kind", "invoice", Destination::parse("billing")?)?;

    assert_eq!(
        router
            .resolve(&[("tenant", "gold"), ("kind", "invoice")])
            .map(Destination::as_str),
        Some("priority-orders")
    );
    assert_eq!(
        router
            .resolve(&[("kind", "invoice")])
            .map(Destination::as_str),
        Some("billing")
    );
    assert_eq!(
        router
            .resolve(&[("tenant", "standard")])
            .map(Destination::as_str),
        Some("fallback")
    );

    assert!(matches!(
        router.add_route("", "gold", Destination::parse("ignored")?),
        Err(error) if error.code() == ErrorCode::Validation
    ));
    Ok(())
}

#[test]
fn message_router_without_a_match_or_default_returns_none() -> CatgaResult<()> {
    let mut router = MessageRouter::new(None);
    router.add_route("kind", "invoice", Destination::parse("billing")?)?;

    assert!(router.resolve(&[("kind", "receipt")]).is_none());
    assert!(router.resolve(&[]).is_none());
    Ok(())
}

#[test]
fn message_router_resolves_immutable_envelope_headers_without_collecting() -> CatgaResult<()> {
    let mut router = MessageRouter::new(Some(Destination::parse("fallback")?));
    router.add_route("tenant", "gold", Destination::parse("priority-orders")?)?;
    let headers = EnvelopeHeaders::try_new([("tenant", "gold"), ("kind", "invoice")])?;

    assert_eq!(
        router
            .resolve_envelope_headers(&headers)
            .map(Destination::as_str),
        Some("priority-orders")
    );
    Ok(())
}
