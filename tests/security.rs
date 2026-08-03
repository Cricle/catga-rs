//! Task-scoped authorization pipeline tests.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use catga_core::{
    AuthorizationBehavior, AuthorizationPolicies, AuthorizationPolicy, AuthorizationRequirements,
    AuthorizedRequest, CatgaResult, ErrorCode, Handler, Mediator, Pipeline, Registry, Request,
    SecurityIdentity, current_security_identity, scope_security_identity,
};

#[derive(Clone)]
struct DeleteReport {
    id: u64,
}

impl catga_core::Message for DeleteReport {}

impl Request for DeleteReport {
    type Response = u64;
    type TypeId = catga_core::DefaultMessageTypeId;
}

impl AuthorizedRequest for DeleteReport {
    fn authorization() -> AuthorizationRequirements {
        AuthorizationRequirements::with_roles(&["administrator"])
    }
}

#[derive(Clone)]
struct ExportReport {
    id: u64,
}

impl catga_core::Message for ExportReport {}

impl Request for ExportReport {
    type Response = u64;
    type TypeId = catga_core::DefaultMessageTypeId;
}

impl AuthorizedRequest for ExportReport {
    fn authorization() -> AuthorizationRequirements {
        AuthorizationRequirements::with_policy("export-reports")
    }
}

struct CountingHandler(Arc<AtomicUsize>);

#[async_trait]
impl Handler<DeleteReport> for CountingHandler {
    async fn handle(&self, request: DeleteReport) -> CatgaResult<u64> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(request.id)
    }
}

#[async_trait]
impl Handler<ExportReport> for CountingHandler {
    async fn handle(&self, request: ExportReport) -> CatgaResult<u64> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(request.id)
    }
}

struct DenyExports;

#[async_trait]
impl AuthorizationPolicy<ExportReport> for DenyExports {
    fn name(&self) -> &str {
        "EXPORT-REPORTS"
    }

    async fn authorize(&self, _: &SecurityIdentity, _: &ExportReport) -> CatgaResult<bool> {
        Ok(false)
    }
}

fn delete_mediator(calls: Arc<AtomicUsize>) -> Mediator {
    let mut registry = Registry::new();
    registry
        .register_request::<DeleteReport, _>(CountingHandler(calls))
        .unwrap();
    Mediator::new(registry)
}

fn export_mediator(calls: Arc<AtomicUsize>) -> Mediator {
    let mut registry = Registry::new();
    registry
        .register_request::<ExportReport, _>(CountingHandler(calls))
        .unwrap();
    Mediator::new(registry)
}

#[tokio::test]
async fn authorization_rejects_anonymous_requests_without_invoking_the_handler() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mediator = delete_mediator(Arc::clone(&calls));
    let pipeline = Pipeline::new().with(AuthorizationBehavior::<DeleteReport>::new());

    let error = mediator
        .send_with(DeleteReport { id: 41 }, &pipeline)
        .await
        .expect_err("anonymous request must be rejected");

    assert_eq!(error.code(), ErrorCode::Unauthorized);
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn authorization_accepts_any_required_role_and_keeps_the_identity_in_its_task_scope() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mediator = delete_mediator(Arc::clone(&calls));
    let pipeline = Pipeline::new().with(AuthorizationBehavior::<DeleteReport>::new());

    let response = scope_security_identity(
        SecurityIdentity::new("lena", ["operator", "administrator"]),
        mediator.send_with(DeleteReport { id: 42 }, &pipeline),
    )
    .await
    .unwrap();

    assert_eq!(response, 42);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert!(current_security_identity().is_none());
}

#[tokio::test]
async fn authorization_denies_roles_and_registered_policies_without_invoking_the_handler() {
    let delete_calls = Arc::new(AtomicUsize::new(0));
    let delete_mediator = delete_mediator(Arc::clone(&delete_calls));
    let delete_pipeline = Pipeline::new().with(AuthorizationBehavior::<DeleteReport>::new());
    let role_error = scope_security_identity(
        SecurityIdentity::new("mika", ["reader"]),
        delete_mediator.send_with(DeleteReport { id: 1 }, &delete_pipeline),
    )
    .await
    .expect_err("unmatched roles must be rejected");

    let export_calls = Arc::new(AtomicUsize::new(0));
    let export_mediator = export_mediator(Arc::clone(&export_calls));
    let export_pipeline = Pipeline::new().with(AuthorizationBehavior::with_policies(
        AuthorizationPolicies::new([Arc::new(DenyExports)]),
    ));
    let policy_error = scope_security_identity(
        SecurityIdentity::new("mika", ["administrator"]),
        export_mediator.send_with(ExportReport { id: 2 }, &export_pipeline),
    )
    .await
    .expect_err("denied policies must be rejected");

    assert_eq!(role_error.code(), ErrorCode::Forbidden);
    assert_eq!(policy_error.code(), ErrorCode::Forbidden);
    assert_eq!(delete_calls.load(Ordering::Relaxed), 0);
    assert_eq!(export_calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn authorization_allows_an_authenticated_request_when_its_named_policy_is_unregistered() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mediator = export_mediator(Arc::clone(&calls));
    let pipeline = Pipeline::new().with(AuthorizationBehavior::<ExportReport>::new());

    let response = scope_security_identity(
        SecurityIdentity::new("mika", ["administrator"]),
        mediator.send_with(ExportReport { id: 3 }, &pipeline),
    )
    .await
    .unwrap();

    assert_eq!(response, 3);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}
