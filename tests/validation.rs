//! Pipeline validation behavior tests.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, Handler, Mediator, Pipeline, Registry, Request, ValidationBehavior,
    Validator,
};

#[derive(Clone)]
struct CreateInvoice {
    customer: String,
    amount: i64,
}

impl catga_core::Message for CreateInvoice {}

impl Request for CreateInvoice {
    type Response = &'static str;
    type TypeId = catga_core::DefaultMessageTypeId;
}

struct CountingHandler(Arc<AtomicUsize>);

#[async_trait]
impl Handler<CreateInvoice> for CountingHandler {
    async fn handle(&self, _: CreateInvoice) -> CatgaResult<&'static str> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok("created")
    }
}

struct CustomerValidator;

#[async_trait]
impl Validator<CreateInvoice> for CustomerValidator {
    async fn validate(
        &self,
        request: &CreateInvoice,
        errors: &mut Vec<Box<str>>,
    ) -> CatgaResult<()> {
        if request.customer.trim().is_empty() {
            errors.push("customer is required".into());
        }
        Ok(())
    }
}

struct AmountValidator;

#[async_trait]
impl Validator<CreateInvoice> for AmountValidator {
    async fn validate(
        &self,
        request: &CreateInvoice,
        errors: &mut Vec<Box<str>>,
    ) -> CatgaResult<()> {
        if request.amount <= 0 {
            errors.push("amount must be positive".into());
        }
        Ok(())
    }
}

struct UnavailableValidator;

#[async_trait]
impl Validator<CreateInvoice> for UnavailableValidator {
    async fn validate(&self, _: &CreateInvoice, _: &mut Vec<Box<str>>) -> CatgaResult<()> {
        Err(CatgaError::new(
            catga_core::ErrorCode::Transient,
            "validation service is unavailable",
        ))
    }
}

fn mediator(calls: Arc<AtomicUsize>) -> Mediator {
    let mut registry = Registry::new();
    registry
        .register_request::<CreateInvoice, _>(CountingHandler(calls))
        .unwrap();
    Mediator::new(registry)
}

#[tokio::test]
async fn empty_validation_behavior_does_not_change_the_handler_fast_path() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mediator = mediator(Arc::clone(&calls));
    let pipeline = Pipeline::new().with(ValidationBehavior::<CreateInvoice>::empty());

    let response = mediator
        .send_with(
            CreateInvoice {
                customer: "acme".to_owned(),
                amount: 5,
            },
            &pipeline,
        )
        .await
        .unwrap();

    assert_eq!(response, "created");
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn validation_behavior_aggregates_errors_and_skips_the_handler() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mediator = mediator(Arc::clone(&calls));
    let pipeline = Pipeline::new().with(ValidationBehavior::new([
        Arc::new(CustomerValidator) as Arc<dyn Validator<CreateInvoice>>,
        Arc::new(AmountValidator),
    ]));

    let error = mediator
        .send_with(
            CreateInvoice {
                customer: " ".to_owned(),
                amount: 0,
            },
            &pipeline,
        )
        .await
        .expect_err("invalid input must not reach the handler");

    assert_eq!(error.code(), catga_core::ErrorCode::Validation);
    assert_eq!(
        error.message(),
        "validation failed: customer is required; amount must be positive"
    );
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn validator_execution_failure_is_returned_without_invoking_the_handler() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mediator = mediator(Arc::clone(&calls));
    let pipeline = Pipeline::new().with(ValidationBehavior::single(UnavailableValidator));

    let error = mediator
        .send_with(
            CreateInvoice {
                customer: "acme".to_owned(),
                amount: 5,
            },
            &pipeline,
        )
        .await
        .expect_err("validator failures must be returned");

    assert_eq!(error.code(), catga_core::ErrorCode::Transient);
    assert_eq!(error.message(), "validation service is unavailable");
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}
