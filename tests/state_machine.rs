use std::sync::Arc;

use catga_core::{Event, Message};
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
                snapshot.clone().next_version(initial.clone())
            )
            .await
            .unwrap()
    );
    assert!(
        !store
            .update(snapshot.version(), snapshot.next_version(initial.clone()))
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
