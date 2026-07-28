//! Durable state-machine behavior integration tests.

use std::{
    any::{Any, TypeId},
    sync::Arc,
};

use catga_core::{ErrorCode, Event, Message};
use catga_flow::{
    StateMachine, StateMachineEventRouter, StateMachineExecutor, StateMachineSnapshot,
    StateMachineState, StateMachineStore,
};
use catga_memory::MemoryStateMachines;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum State {
    Pending,
    Paid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Order {
    state: State,
    payment_allowed: bool,
    trace: Vec<&'static str>,
}

impl StateMachineState<State> for Order {
    fn current_state(&self) -> &State {
        &self.state
    }

    fn set_current_state(&mut self, state: State) {
        self.state = state;
    }
}

impl Default for Order {
    fn default() -> Self {
        Self {
            state: State::Pending,
            payment_allowed: true,
            trace: Vec::new(),
        }
    }
}

#[derive(Clone)]
struct Paid;

impl Message for Paid {}
impl Event for Paid {}

#[derive(Clone)]
struct Started;

impl Message for Started {}
impl Event for Started {}

#[derive(Clone)]
struct RoutedPaid {
    instance_id: String,
}

impl Message for RoutedPaid {}
impl Event for RoutedPaid {}

struct PaymentEvent;

const PAYMENT_EVENT_CATEGORIES: &[TypeId] = &[TypeId::of::<PaymentEvent>()];

#[derive(Clone)]
struct CardPaid;

impl Message for CardPaid {}

impl Event for CardPaid {
    fn categories(&self) -> &'static [TypeId] {
        PAYMENT_EVENT_CATEGORIES
    }
}

#[derive(Clone)]
struct WirePaid;

impl Message for WirePaid {}

impl Event for WirePaid {
    fn categories(&self) -> &'static [TypeId] {
        PAYMENT_EVENT_CATEGORIES
    }
}

#[derive(Clone)]
struct UnrelatedPaymentEvent;

impl Message for UnrelatedPaymentEvent {}
impl Event for UnrelatedPaymentEvent {}

fn machine() -> StateMachine<Order, State> {
    let mut definition = StateMachine::<Order, State>::builder(State::Pending);
    definition
        .state(State::Pending)
        .on::<Paid>()
        .when(|order, _| order.payment_allowed)
        .execute(|order, _| {
            order.trace.push("transition");
            Ok(())
        })
        .transition_to(State::Paid);
    definition
        .state(State::Pending)
        .on::<RoutedPaid>()
        .transition_to(State::Paid);
    definition.state(State::Pending).on_exit(|order| {
        order.trace.push("exit");
        Ok(())
    });
    definition.state(State::Paid).on_enter(|order| {
        order.trace.push("enter");
        Ok(())
    });
    definition.build()
}

fn starting_machine() -> StateMachine<Order, State> {
    let mut definition = StateMachine::<Order, State>::builder(State::Pending);
    definition.starts_with::<Started, _>(State::Pending, |_, _| Order {
        state: State::Paid,
        payment_allowed: true,
        trace: Vec::new(),
    });
    definition
        .state(State::Pending)
        .on::<Started>()
        .transition_to(State::Paid);
    definition.build()
}

fn factory_machine() -> StateMachine<Order, State> {
    let mut definition = StateMachine::<Order, State>::builder(State::Pending);
    definition.create_instance_from::<Started, _>(|_, _| Order {
        state: State::Paid,
        payment_allowed: true,
        trace: Vec::new(),
    });
    definition
        .state(State::Pending)
        .on::<Started>()
        .transition_to(State::Paid);
    definition.build()
}

fn category_machine() -> StateMachine<Order, State> {
    let mut definition = StateMachine::<Order, State>::builder(State::Pending);
    definition
        .state(State::Pending)
        .on_category::<PaymentEvent, _>(|event| {
            if let Some(event) = event.downcast_ref::<CardPaid>() {
                Some(event as &dyn Any)
            } else {
                event
                    .downcast_ref::<WirePaid>()
                    .map(|event| event as &dyn Any)
            }
        })
        .execute(|order, _| {
            order.trace.push("category");
            Ok(())
        })
        .finish()
        .on::<CardPaid>()
        .execute(|order, _| {
            order.trace.push("exact");
            Ok(())
        })
        .finish();
    definition.build()
}

fn declining_category_machine() -> StateMachine<Order, State> {
    let mut definition = StateMachine::<Order, State>::builder(State::Pending);
    definition
        .state(State::Pending)
        .on_category::<PaymentEvent, _>(|_| None)
        .execute(|order, _| {
            order.trace.push("category");
            Ok(())
        })
        .finish();
    definition.build()
}

#[tokio::test]
async fn category_transition_prefers_an_exact_transition() {
    let mut order = Order::default();

    let result = category_machine()
        .handle(&mut order, &CardPaid)
        .await
        .unwrap();

    assert!(result.handled());
    assert_eq!(order.trace, ["exact"]);
}

#[tokio::test]
async fn category_transition_handles_a_declared_event_category() {
    let mut order = Order::default();

    let result = category_machine()
        .handle(&mut order, &WirePaid)
        .await
        .unwrap();

    assert!(result.handled());
    assert_eq!(order.trace, ["category"]);
}

#[tokio::test]
async fn category_transition_leaves_unrelated_events_unhandled() {
    let mut order = Order::default();

    let result = category_machine()
        .handle(&mut order, &UnrelatedPaymentEvent)
        .await
        .unwrap();

    assert!(!result.handled());
    assert!(order.trace.is_empty());
}

#[tokio::test]
async fn category_transition_leaves_events_unhandled_when_extraction_declines() {
    let mut order = Order::default();

    let result = declining_category_machine()
        .handle(&mut order, &CardPaid)
        .await
        .unwrap();

    assert!(!result.handled());
    assert!(order.trace.is_empty());
}

#[tokio::test]
async fn state_machine_runs_exit_transition_and_entry_actions_in_order() {
    let mut order = Order {
        state: State::Pending,
        payment_allowed: true,
        trace: Vec::new(),
    };

    let result = machine().handle(&mut order, &Paid).await.unwrap();

    assert!(result.handled());
    assert_eq!(result.current(), State::Paid);
    assert!(result.transitioned());
    assert_eq!(order.trace, ["exit", "transition", "enter"]);
}

#[tokio::test]
async fn guarded_transition_leaves_state_and_actions_untouched() {
    let mut order = Order {
        state: State::Pending,
        payment_allowed: false,
        trace: Vec::new(),
    };

    let result = machine().handle(&mut order, &Paid).await.unwrap();

    assert!(!result.handled());
    assert_eq!(result.previous(), State::Pending);
    assert_eq!(result.current(), State::Pending);
    assert!(order.trace.is_empty());
}

#[tokio::test]
async fn state_machine_allows_async_actions_to_update_state() {
    let mut definition = StateMachine::<Order, State>::builder(State::Pending);
    definition
        .state(State::Pending)
        .on::<Paid>()
        .execute_async(|order, _| {
            Box::pin(async move {
                tokio::task::yield_now().await;
                order.trace.push("async-transition");
                Ok(())
            })
        })
        .transition_to(State::Paid);
    let mut order = Order {
        state: State::Pending,
        payment_allowed: true,
        trace: Vec::new(),
    };

    let result = definition.build().handle(&mut order, &Paid).await.unwrap();

    assert!(result.handled());
    assert_eq!(order.trace, ["async-transition"]);
}

#[tokio::test]
async fn memory_store_uses_versions_and_executor_persists_handled_events() {
    let store = Arc::new(MemoryStateMachines::default());
    let initial = Order {
        state: State::Pending,
        payment_allowed: true,
        trace: Vec::new(),
    };
    let snapshot = StateMachineSnapshot::new("order-7", initial.clone());

    assert!(store.create(snapshot.clone()).await.unwrap());
    assert!(!store.create(snapshot.clone()).await.unwrap());
    assert!(
        store
            .update(
                snapshot.version(),
                snapshot.clone().next_version(initial.clone()).unwrap()
            )
            .await
            .unwrap()
    );
    assert!(
        !store
            .update(
                snapshot.version(),
                snapshot.next_version(initial.clone()).unwrap()
            )
            .await
            .unwrap()
    );

    let executor = StateMachineExecutor::new(machine(), Arc::clone(&store));
    assert!(executor.initialize("order-8", initial).await.unwrap());
    let result = executor.handle("order-8", &Paid).await.unwrap();
    let saved = store.get("order-8").await.unwrap().unwrap();

    assert!(result.handled());
    assert_eq!(saved.version(), 1);
    assert_eq!(saved.state().current_state(), &State::Paid);
}

#[tokio::test]
async fn state_machine_versions_cannot_saturate() -> catga_core::CatgaResult<()> {
    let snapshot = StateMachineSnapshot::restore(
        "order-version-limit",
        Order::default(),
        i64::MAX,
        std::time::SystemTime::UNIX_EPOCH,
        std::time::SystemTime::UNIX_EPOCH,
    )?;
    let error = snapshot
        .next_version(Order::default())
        .expect_err("the maximum state-machine version cannot advance");
    assert_eq!(error.code(), ErrorCode::Conflict);

    let store = MemoryStateMachines::default();
    assert!(store.create(snapshot.clone()).await?);
    assert!(!store.update(snapshot.version(), snapshot).await?);
    Ok(())
}

#[test]
fn restored_state_machine_snapshots_reject_negative_versions() {
    let now = std::time::SystemTime::now();
    let error = StateMachineSnapshot::restore(
        "invalid-order",
        Order {
            state: State::Pending,
            payment_allowed: true,
            trace: Vec::new(),
        },
        -1,
        now,
        now,
    )
    .expect_err("negative persisted version must fail");

    assert_eq!(error.code(), ErrorCode::Validation);
}

#[tokio::test]
async fn executor_creates_an_instance_from_a_configured_initial_event() {
    let store = Arc::new(MemoryStateMachines::default());
    let executor = StateMachineExecutor::new(starting_machine(), Arc::clone(&store));

    let result = executor.handle("order-9", &Started).await.unwrap();
    let saved = store.get("order-9").await.unwrap().unwrap();

    assert!(result.handled());
    assert_eq!(result.previous(), State::Pending);
    assert_eq!(saved.version(), 0);
    assert_eq!(saved.state().current_state(), &State::Paid);
}

#[tokio::test]
async fn executor_uses_the_default_initial_state_for_a_registered_factory() {
    let store = Arc::new(MemoryStateMachines::default());
    let executor = StateMachineExecutor::new(factory_machine(), Arc::clone(&store));

    let result = executor.handle("order-9a", &Started).await.unwrap();

    assert!(result.handled());
    assert_eq!(result.previous(), State::Pending);
    assert_eq!(
        store
            .get("order-9a")
            .await
            .unwrap()
            .unwrap()
            .state()
            .current_state(),
        &State::Paid
    );
}

#[tokio::test]
async fn event_router_uses_a_typed_instance_id_resolver() {
    let store = Arc::new(MemoryStateMachines::default());
    let executor = Arc::new(StateMachineExecutor::new(machine(), Arc::clone(&store)));
    assert!(
        executor
            .initialize(
                "order-10",
                Order {
                    state: State::Pending,
                    payment_allowed: true,
                    trace: Vec::new(),
                },
            )
            .await
            .unwrap()
    );
    let router = StateMachineEventRouter::new(Arc::clone(&executor))
        .for_event::<RoutedPaid, _>(|event| event.instance_id.clone());

    let result = router
        .route(&RoutedPaid {
            instance_id: "order-10".to_owned(),
        })
        .await
        .unwrap();

    assert!(result.handled());
    assert_eq!(
        store
            .get("order-10")
            .await
            .unwrap()
            .unwrap()
            .state()
            .current_state(),
        &State::Paid
    );
}

#[tokio::test]
async fn event_router_uses_an_optional_fallback_for_unregistered_event_types() {
    let store = Arc::new(MemoryStateMachines::default());
    let executor = Arc::new(StateMachineExecutor::new(machine(), Arc::clone(&store)));
    assert!(
        executor
            .initialize("order-fallback", Order::default())
            .await
            .unwrap()
    );
    let router = StateMachineEventRouter::new(executor).with_fallback(|event| {
        event
            .downcast_ref::<RoutedPaid>()
            .map(|event| event.instance_id.clone())
            .ok_or_else(|| catga_core::CatgaError::new(ErrorCode::Unsupported, "unroutable"))
    });

    let result = router
        .route(&RoutedPaid {
            instance_id: "order-fallback".to_owned(),
        })
        .await
        .unwrap();

    assert!(result.handled());
}

#[tokio::test]
async fn executor_lazily_initializes_default_state_without_a_custom_factory() {
    let store = Arc::new(MemoryStateMachines::default());
    let mut definition = StateMachine::<Order, State>::builder(State::Pending);
    definition.default_initial_state();
    definition
        .state(State::Pending)
        .on::<Started>()
        .transition_to(State::Paid);
    let executor = StateMachineExecutor::new(definition.build(), Arc::clone(&store));

    let result = executor.handle("order-default", &Started).await.unwrap();

    assert!(result.handled());
    assert_eq!(
        store
            .get("order-default")
            .await
            .unwrap()
            .unwrap()
            .state()
            .current_state(),
        &State::Paid
    );
}
