//! A minimal clonable request type plus a counting handler mediator for
//! pipeline-behavior contract tests.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use catga_core::{Mediator, Message, MessageTypeId, Registry, Request, request_handler};

pub(crate) struct BumpTypeId;

impl MessageTypeId for BumpTypeId {
    const NAME: &'static str = "Bump";
}

#[derive(Clone)]
pub(crate) struct Bump(pub(crate) u64);

impl Message for Bump {}

impl Request for Bump {
    type Response = u64;
    type TypeId = BumpTypeId;
}

/// Registers a `Bump` handler that counts invocations and returns `value + 1`.
pub(crate) fn bump_mediator(calls: &Arc<AtomicUsize>) -> Mediator {
    let mut registry = Registry::new();
    registry
        .register_request::<Bump, _>(request_handler({
            let calls = Arc::clone(calls);
            move |bump: Bump| {
                let calls = Arc::clone(&calls);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(bump.0 + 1)
                }
            }
        }))
        .expect("bump handler must register");
    Mediator::new(registry)
}
