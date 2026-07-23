//! Typed mediator pipeline tests.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use catga_core::{Behavior, CatgaResult, Handler, Mediator, Next, Pipeline, Registry, Request};

#[derive(Debug)]
struct Double(u64);

impl catga_core::Message for Double {}

impl Request for Double {
    type Response = u64;
}

struct DoubleHandler(Arc<Mutex<Vec<&'static str>>>);

#[async_trait]
impl Handler<Double> for DoubleHandler {
    async fn handle(&self, message: Double) -> CatgaResult<u64> {
        self.0.lock().unwrap().push("handler");
        Ok(message.0 * 2)
    }
}

struct TraceBehavior {
    entered: &'static str,
    exited: &'static str,
    trace: Arc<Mutex<Vec<&'static str>>>,
}

#[async_trait]
impl Behavior<Double> for TraceBehavior {
    async fn handle(&self, message: Double, next: Next<Double>) -> CatgaResult<u64> {
        self.trace.lock().unwrap().push(self.entered);
        let response = next.run(message).await?;
        self.trace.lock().unwrap().push(self.exited);
        Ok(response)
    }
}

#[tokio::test]
async fn pipeline_behaviors_wrap_a_registered_handler_in_registration_order() {
    let trace = Arc::new(Mutex::new(Vec::new()));
    let mut registry = Registry::new();
    registry
        .register_request::<Double, _>(DoubleHandler(trace.clone()))
        .unwrap();
    let mediator = Mediator::new(registry);
    let pipeline = Pipeline::new()
        .with(TraceBehavior {
            entered: "a+",
            exited: "a-",
            trace: trace.clone(),
        })
        .with(TraceBehavior {
            entered: "b+",
            exited: "b-",
            trace: trace.clone(),
        });

    assert_eq!(mediator.send_with(Double(4), &pipeline).await.unwrap(), 8);
    assert_eq!(*trace.lock().unwrap(), ["a+", "b+", "handler", "b-", "a-"]);
}
