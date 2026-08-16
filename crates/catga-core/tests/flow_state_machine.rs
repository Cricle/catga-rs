//! Strict contracts for the typed state-machine subtree: definition builders,
//! exact/category transition selection, executor optimistic concurrency, event
//! routing, snapshot versioning, and the durable persistence frame.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use catga_core::{
    CatgaError, CatgaResult, ErrorCode, Event, Message, SnapshotCodec,
    flow::{
        StateMachine, StateMachineEventRouter, StateMachineExecutor, StateMachineSnapshot,
        StateMachineState, StateMachineStore, decode_state_machine_snapshot,
        encode_state_machine_snapshot,
    },
};
use futures::FutureExt;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// State discriminator for the order workflow.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
enum St {
    #[default]
    New,
    Paid,
    Shipped,
    Cancelled,
    Limbo,
}

/// Mutable order payload carried by a state-machine instance.
#[derive(Clone, Debug, Default)]
struct Order {
    state: St,
    total: u64,
    log: Vec<&'static str>,
}

impl StateMachineState<St> for Order {
    fn current_state(&self) -> &St {
        &self.state
    }

    fn set_current_state(&mut self, state: St) {
        self.state = state;
    }
}

macro_rules! sm_event {
    ($name:ident) => {
        #[derive(Clone)]
        struct $name;
        impl Message for $name {}
        impl Event for $name {}
    };
}

/// Event carrying the placed order amount.
#[derive(Clone)]
struct Placed(u64);
impl Message for Placed {}
impl Event for Placed {}

sm_event!(PaidEvt);
sm_event!(ShipEvt);
sm_event!(CancelEvt);
sm_event!(Ignored);

/// Marker category declared by [`Audited`].
struct AuditCat;

/// Event handled through an explicit category transition.
#[derive(Clone)]
struct Audited(u64);
impl Message for Audited {}
impl Event for Audited {
    fn categories(&self) -> &'static [TypeId] {
        static CATS: std::sync::OnceLock<[TypeId; 1]> = std::sync::OnceLock::new();
        CATS.get_or_init(|| [TypeId::of::<AuditCat>()])
    }
}

/// Extracts the audit amount from an erased [`Audited`] event.
fn audit_extractor(event: &(dyn Any + Send + Sync)) -> Option<&dyn Any> {
    event
        .downcast_ref::<Audited>()
        .map(|audited| &audited.0 as &dyn Any)
}

/// Builds the canonical order workflow used by most transition tests.
fn order_machine() -> StateMachine<Order, St> {
    let mut builder = StateMachine::<Order, St>::builder(St::New);
    let mut fresh = builder.state(St::New);
    fresh.on_exit(|state| {
        state.log.push("exit-new");
        Ok(())
    });
    fresh
        .on::<Placed>()
        .when(|_, event| event.0 > 0)
        .execute(|state, event| {
            state.total += event.0;
            Ok(())
        })
        .transition_to(St::Paid)
        .on::<ShipEvt>()
        .execute(|state, _| {
            state.log.push("ship-stays");
            Ok(())
        })
        .and();
    let mut paid = builder.state(St::Paid);
    paid.on_enter(|state| {
        state.log.push("enter-paid");
        Ok(())
    });
    paid.on::<PaidEvt>()
        .execute_async(|state, _| {
            async move {
                state.log.push("paid-async");
                Ok(())
            }
            .boxed()
        })
        .transition_to(St::Shipped)
        .on_category::<AuditCat, _>(audit_extractor)
        .when(|_, value| value.downcast_ref::<u64>().copied().unwrap_or(0) > 0)
        .execute(|state, value| {
            state.total += value.downcast_ref::<u64>().copied().unwrap_or(0);
            Ok(())
        })
        .transition_to(St::Cancelled);
    let mut shipped = builder.state(St::Shipped);
    shipped.on_enter_async(|state| {
        async move {
            state.log.push("enter-shipped-async");
            Ok(())
        }
        .boxed()
    });
    shipped.on::<CancelEvt>().transition_to(St::Cancelled);
    builder.build()
}

// ---------------------------------------------------------------------------
// StateMachine definition contracts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn state_machine_reports_initial_state_and_unhandled_events() {
    let machine = StateMachine::<Order, St>::builder(St::New).build();
    assert_eq!(machine.initial(), St::New);
    let cloned = machine.clone();
    assert_eq!(cloned.initial(), St::New);

    // No configured state matches, so the event passes unhandled.
    let mut order = Order::default();
    let result = machine
        .handle(&mut order, &Placed(5))
        .await
        .expect("handles");
    assert!(!result.handled());
    assert!(!result.transitioned());
    assert_eq!(result.previous(), St::New);
    assert_eq!(result.current(), St::New);

    // A state without configured transitions also passes events through.
    let mut order = Order {
        state: St::Limbo,
        ..Default::default()
    };
    let result = machine
        .handle(&mut order, &Placed(5))
        .await
        .expect("handles");
    assert!(!result.handled());
    assert_eq!(result.current(), St::Limbo);
}

#[tokio::test]
async fn state_machine_runs_exit_action_transition_and_entry_in_order() {
    let machine = order_machine();
    let mut order = Order::default();

    // The guard rejects a zero amount; nothing runs.
    let result = machine
        .handle(&mut order, &Placed(0))
        .await
        .expect("handles");
    assert!(!result.handled());
    assert!(order.log.is_empty());

    // A positive amount exits New, applies the action, and enters Paid.
    let result = machine
        .handle(&mut order, &Placed(7))
        .await
        .expect("handles");
    assert!(result.handled());
    assert!(result.transitioned());
    assert_eq!(result.previous(), St::New);
    assert_eq!(result.current(), St::Paid);
    assert_eq!(order.total, 7);
    assert_eq!(order.log, vec!["exit-new", "enter-paid"]);
}

#[tokio::test]
async fn state_machine_transitions_without_targets_keep_the_state() {
    let machine = order_machine();
    let mut order = Order::default();
    let result = machine.handle(&mut order, &ShipEvt).await.expect("handles");
    assert!(result.handled());
    assert!(!result.transitioned());
    assert_eq!(order.log, vec!["exit-new", "ship-stays"]);
}

#[tokio::test]
async fn state_machine_async_actions_drive_later_states() {
    let machine = order_machine();
    let mut order = Order::default();
    machine
        .handle(&mut order, &Placed(1))
        .await
        .expect("enters paid");

    let result = machine.handle(&mut order, &PaidEvt).await.expect("handles");
    assert!(result.transitioned());
    assert_eq!(result.current(), St::Shipped);
    assert_eq!(
        order.log,
        vec![
            "exit-new",
            "enter-paid",
            "paid-async",
            "enter-shipped-async"
        ]
    );
}

#[tokio::test]
async fn exact_event_transitions_take_precedence_over_categories() {
    let mut builder = StateMachine::<Order, St>::builder(St::New);
    builder
        .state(St::New)
        .on::<Audited>()
        .execute(|state, _| {
            state.log.push("exact");
            Ok(())
        })
        .finish()
        .on_category::<AuditCat, _>(audit_extractor)
        .execute(|state, _| {
            state.log.push("category");
            Ok(())
        })
        .finish();
    let machine = builder.build();

    let mut order = Order::default();
    let result = machine
        .handle(&mut order, &Audited(3))
        .await
        .expect("handles");
    assert!(result.handled());
    assert_eq!(order.log, vec!["exact"]);
}

#[tokio::test]
async fn category_transitions_guard_extract_and_run_async_actions() {
    let mut builder = StateMachine::<Order, St>::builder(St::New);
    builder
        .state(St::New)
        .on_category::<AuditCat, _>(audit_extractor)
        .when(|_, value| value.downcast_ref::<u64>().copied().unwrap_or(0) > 5)
        .execute_async(|state, value| {
            let amount = value.downcast_ref::<u64>().copied().unwrap_or(0);
            async move {
                state.total += amount;
                Ok(())
            }
            .boxed()
        })
        .transition_to(St::Cancelled);
    let machine = builder.build();

    // The guard rejects small amounts.
    let mut order = Order::default();
    let result = machine
        .handle(&mut order, &Audited(5))
        .await
        .expect("handles");
    assert!(!result.handled());

    // A passing guard extracts, runs the async action, and transitions.
    let result = machine
        .handle(&mut order, &Audited(6))
        .await
        .expect("handles");
    assert!(result.handled());
    assert_eq!(order.total, 6);
    assert_eq!(result.current(), St::Cancelled);

    // Events without the declared category never reach the extractor.
    let mut order = Order::default();
    let result = machine
        .handle(&mut order, &Placed(9))
        .await
        .expect("handles");
    assert!(!result.handled());
}

#[tokio::test]
async fn category_transitions_without_actions_still_transition() {
    let mut builder = StateMachine::<Order, St>::builder(St::New);
    builder
        .state(St::New)
        .on_category::<AuditCat, _>(audit_extractor)
        .transition_to(St::Cancelled);
    let machine = builder.build();
    let mut order = Order::default();
    let result = machine
        .handle(&mut order, &Audited(1))
        .await
        .expect("handles");
    assert!(result.handled());
    assert_eq!(result.current(), St::Cancelled);
}

#[tokio::test]
async fn a_declined_extractor_after_selection_is_an_internal_error() {
    // The extractor cooperates during selection and refuses during execution,
    // surfacing the defensive error branch.
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let mut builder = StateMachine::<Order, St>::builder(St::New);
    builder
        .state(St::New)
        .on_category::<AuditCat, _>(move |event| {
            if observed.fetch_add(1, Ordering::SeqCst) == 0 {
                audit_extractor(event)
            } else {
                None
            }
        })
        .finish();
    let machine = builder.build();

    let mut order = Order::default();
    let error = machine
        .handle(&mut order, &Audited(1))
        .await
        .expect_err("a declined extractor fails");
    assert_eq!(error.code(), ErrorCode::Internal);
    assert!(error.message().contains("declined"));
}

#[tokio::test]
async fn failing_actions_propagate_their_errors() {
    // A failing exit action aborts the transition.
    let mut builder = StateMachine::<Order, St>::builder(St::New);
    let mut fresh = builder.state(St::New);
    fresh.on_exit(|_| Err(CatgaError::new(ErrorCode::Validation, "exit refuses")));
    fresh.on::<Placed>().transition_to(St::Paid);
    let machine = builder.build();
    let mut order = Order::default();
    let error = machine
        .handle(&mut order, &Placed(1))
        .await
        .expect_err("the exit action failure surfaces");
    assert_eq!(error.code(), ErrorCode::Validation);
    assert_eq!(order.state, St::New);

    // A failing transition action keeps the previous state.
    let mut builder = StateMachine::<Order, St>::builder(St::New);
    builder
        .state(St::New)
        .on::<Placed>()
        .execute_async(|_, _| {
            async move { Err(CatgaError::new(ErrorCode::Unavailable, "action refuses")) }.boxed()
        })
        .transition_to(St::Paid);
    let machine = builder.build();
    let mut order = Order::default();
    let error = machine
        .handle(&mut order, &Placed(1))
        .await
        .expect_err("the action failure surfaces");
    assert_eq!(error.code(), ErrorCode::Unavailable);
    assert_eq!(order.state, St::New);

    // A failing entry action surfaces after the transition action ran.
    let mut builder = StateMachine::<Order, St>::builder(St::New);
    builder
        .state(St::New)
        .on::<Placed>()
        .transition_to(St::Paid);
    builder.state(St::Paid).on_enter_async(|_| {
        async move { Err(CatgaError::new(ErrorCode::Internal, "enter refuses")) }.boxed()
    });
    let machine = builder.build();
    let mut order = Order::default();
    let error = machine
        .handle(&mut order, &Placed(1))
        .await
        .expect_err("the entry action failure surfaces");
    assert_eq!(error.code(), ErrorCode::Internal);
}

// ---------------------------------------------------------------------------
// In-memory optimistic store stub
// ---------------------------------------------------------------------------

/// Snapshot store with selectable faults around a hash map.
struct MemStore {
    snapshots: Mutex<HashMap<String, StateMachineSnapshot<Order>>>,
    mode: &'static str,
}

impl MemStore {
    fn new(mode: &'static str) -> Arc<Self> {
        Arc::new(Self {
            snapshots: Mutex::new(HashMap::new()),
            mode,
        })
    }
}

#[async_trait]
impl StateMachineStore<Order> for MemStore {
    async fn create(&self, snapshot: StateMachineSnapshot<Order>) -> CatgaResult<bool> {
        match self.mode {
            "create-error" => Err(CatgaError::new(ErrorCode::Unavailable, "create failed")),
            "create-false" => Ok(false),
            _ => {
                let mut snapshots = self.snapshots.lock().expect("snapshot lock");
                if snapshots.contains_key(snapshot.instance_id()) {
                    return Ok(false);
                }
                snapshots.insert(snapshot.instance_id().into(), snapshot);
                Ok(true)
            }
        }
    }

    async fn get(&self, instance_id: &str) -> CatgaResult<Option<StateMachineSnapshot<Order>>> {
        if self.mode == "get-error" {
            return Err(CatgaError::new(ErrorCode::Unavailable, "get failed"));
        }
        Ok(self
            .snapshots
            .lock()
            .expect("snapshot lock")
            .get(instance_id)
            .cloned())
    }

    async fn update(
        &self,
        expected_version: i64,
        next: StateMachineSnapshot<Order>,
    ) -> CatgaResult<bool> {
        if self.mode == "update-false" {
            return Ok(false);
        }
        let mut snapshots = self.snapshots.lock().expect("snapshot lock");
        let Some(current) = snapshots.get(next.instance_id()) else {
            return Ok(false);
        };
        if current.version() != expected_version
            || !StateMachineSnapshot::<Order>::is_next_version(expected_version, next.version())
        {
            return Ok(false);
        }
        snapshots.insert(next.instance_id().into(), next);
        Ok(true)
    }
}

// ---------------------------------------------------------------------------
// StateMachineExecutor contracts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn executor_initializes_instances_and_advances_versions() {
    let machine = order_machine();
    let store = MemStore::new("ok");
    let executor = StateMachineExecutor::new(machine, Arc::clone(&store));

    assert!(
        executor
            .initialize("order-1", Order::default())
            .await
            .expect("initialize succeeds"),
        "the first create wins"
    );
    assert!(
        !executor
            .initialize("order-1", Order::default())
            .await
            .expect("initialize succeeds"),
        "a duplicate create loses"
    );

    let result = executor
        .handle("order-1", &Placed(4))
        .await
        .expect("the event applies");
    assert!(result.transitioned());
    let snapshot = executor
        .get("order-1")
        .await
        .expect("get succeeds")
        .expect("the instance exists");
    assert_eq!(snapshot.version(), 1);
    assert_eq!(snapshot.state().total, 4);
    assert_eq!(snapshot.state().current_state(), &St::Paid);

    // Store read errors propagate to the caller.
    let failing = StateMachineExecutor::new(order_machine(), MemStore::new("get-error"));
    let error = failing
        .handle("order-1", &Placed(4))
        .await
        .expect_err("a get failure surfaces");
    assert_eq!(error.code(), ErrorCode::Unavailable);
}

#[tokio::test]
async fn executor_rejects_unknown_instances_without_factories() {
    let executor = StateMachineExecutor::new(order_machine(), MemStore::new("ok"));
    let error = executor
        .handle("missing", &Placed(4))
        .await
        .expect_err("no factory means no instance");
    assert_eq!(error.code(), ErrorCode::NotFound);
    assert!(error.message().contains("missing"));

    // An unhandled event on a missing instance still reports NotFound.
    let error = executor
        .handle("missing", &Ignored)
        .await
        .expect_err("still missing");
    assert_eq!(error.code(), ErrorCode::NotFound);
}

#[tokio::test]
async fn executor_unhandled_events_persist_nothing() {
    let executor = StateMachineExecutor::new(order_machine(), MemStore::new("ok"));
    executor
        .initialize("seeded", Order::default())
        .await
        .expect("initialize succeeds");
    let result = executor
        .handle("seeded", &Ignored)
        .await
        .expect("unhandled passes");
    assert!(!result.handled());
    assert_eq!(
        executor
            .get("seeded")
            .await
            .expect("get succeeds")
            .expect("the instance exists")
            .version(),
        0,
        "an unhandled event keeps the stored version"
    );
}

#[tokio::test]
async fn executor_creates_instances_through_event_factories() {
    let mut builder = StateMachine::<Order, St>::builder(St::New);
    builder
        .state(St::New)
        .on::<Placed>()
        .execute(|state, event| {
            state.total += event.0;
            Ok(())
        })
        .transition_to(St::Paid)
        .on::<PaidEvt>()
        .transition_to(St::Shipped);
    // Re-registering the same event type replaces the earlier factory.
    builder.starts_with::<Placed, _>(St::New, |_, _| Order {
        total: 100,
        ..Default::default()
    });
    builder.starts_with::<Placed, _>(St::New, |event, instance_id| Order {
        total: event.0 * 2 + instance_id.len() as u64,
        ..Default::default()
    });
    // create_instance_from starts from the machine's default initial state.
    builder.create_instance_from::<PaidEvt, _>(|_, _| Order {
        total: 55,
        ..Default::default()
    });
    let executor = StateMachineExecutor::new(builder.build(), MemStore::new("ok"));

    let result = executor
        .handle("factory-1", &Placed(4))
        .await
        .expect("the factory hydrates");
    assert!(result.handled());
    let snapshot = executor
        .get("factory-1")
        .await
        .expect("get succeeds")
        .expect("the instance exists");
    // Replacement factory hydrated 4 * 2 + len("factory-1"), then the
    // Placed(4) transition action added the event amount once more.
    assert_eq!(snapshot.state().total, 8 + 9 + 4);
    assert_eq!(snapshot.version(), 0);

    let result = executor
        .handle("factory-2", &PaidEvt)
        .await
        .expect("the default-initial factory hydrates");
    assert!(result.transitioned());
    let snapshot = executor
        .get("factory-2")
        .await
        .expect("get succeeds")
        .expect("the instance exists");
    assert_eq!(snapshot.state().total, 55);
}

#[tokio::test]
async fn executor_default_initial_state_keeps_factory_precedence() {
    let mut builder = StateMachine::<Order, St>::builder(St::New);
    builder
        .state(St::New)
        .on::<Placed>()
        .execute(|state, event| {
            state.total += event.0;
            Ok(())
        })
        .transition_to(St::Paid)
        .on::<PaidEvt>()
        .transition_to(St::Shipped);
    builder.starts_with::<Placed, _>(St::New, |_, _| Order {
        total: 777,
        ..Default::default()
    });
    builder.default_initial_state();
    let executor = StateMachineExecutor::new(builder.build(), MemStore::new("ok"));

    // The event-specific factory beats the default factory.
    executor
        .handle("specific", &Placed(1))
        .await
        .expect("specific factory wins");
    let snapshot = executor
        .get("specific")
        .await
        .expect("get succeeds")
        .expect("the instance exists");
    assert_eq!(snapshot.state().total, 777 + 1);

    // Other events fall back to Default::default state.
    executor
        .handle("defaulted", &PaidEvt)
        .await
        .expect("default factory wins");
    let snapshot = executor
        .get("defaulted")
        .await
        .expect("get succeeds")
        .expect("the instance exists");
    assert_eq!(snapshot.state().total, 0);
}

#[tokio::test]
async fn executor_surfaces_store_conflicts_and_errors() {
    // A losing create reports a concurrent creation.
    let mut builder = StateMachine::<Order, St>::builder(St::New);
    builder
        .state(St::New)
        .on::<Placed>()
        .transition_to(St::Paid);
    builder.starts_with::<Placed, _>(St::New, |_, _| Order::default());
    let executor = StateMachineExecutor::new(builder.build(), MemStore::new("create-false"));
    let error = executor
        .handle("raced", &Placed(1))
        .await
        .expect_err("the create race surfaces");
    assert_eq!(error.code(), ErrorCode::Conflict);
    assert!(error.message().contains("created concurrently"));

    // Store create errors propagate.
    let executor = StateMachineExecutor::new(order_machine(), MemStore::new("create-error"));
    let error = executor
        .initialize("broken", Order::default())
        .await
        .expect_err("the create failure surfaces");
    assert_eq!(error.code(), ErrorCode::Unavailable);

    // A losing update reports a concurrent change.
    let executor = StateMachineExecutor::new(order_machine(), MemStore::new("update-false"));
    executor
        .initialize("raced", Order::default())
        .await
        .expect("initialize succeeds");
    let error = executor
        .handle("raced", &Placed(1))
        .await
        .expect_err("the update race surfaces");
    assert_eq!(error.code(), ErrorCode::Conflict);
    assert!(error.message().contains("changed while handling"));
}

// ---------------------------------------------------------------------------
// StateMachineEventRouter contracts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn router_resolves_instance_ids_and_routes_events() {
    let mut builder = StateMachine::<Order, St>::builder(St::New);
    builder
        .state(St::New)
        .on::<Placed>()
        .execute(|state, event| {
            state.total += event.0;
            Ok(())
        })
        .transition_to(St::Paid)
        .on_category::<AuditCat, _>(audit_extractor)
        .execute(|state, value| {
            state.total += value.downcast_ref::<u64>().copied().unwrap_or(0);
            Ok(())
        })
        .finish();
    builder.starts_with::<Placed, _>(St::New, |_, _| Order::default());
    builder.create_instance_from::<Audited, _>(|_, _| Order::default());
    let executor = Arc::new(StateMachineExecutor::new(
        builder.build(),
        MemStore::new("ok"),
    ));

    let router = StateMachineEventRouter::new(Arc::clone(&executor))
        .for_event::<Placed, _>(|event| format!("order-{}", event.0))
        .with_fallback(|event| {
            event
                .downcast_ref::<Audited>()
                .map(|audited| format!("audit-{}", audited.0))
                .ok_or_else(|| CatgaError::new(ErrorCode::Unsupported, "unknown fallback event"))
        });

    let result = router
        .route(&Placed(9))
        .await
        .expect("the typed route runs");
    assert!(result.handled());
    assert!(
        executor
            .get("order-9")
            .await
            .expect("get succeeds")
            .is_some()
    );

    // The fallback resolves erased events through category transitions.
    let result = router
        .route(&Audited(3))
        .await
        .expect("the fallback route runs");
    assert!(result.handled());
    let snapshot = executor
        .get("audit-3")
        .await
        .expect("get succeeds")
        .expect("the fallback instance exists");
    assert_eq!(snapshot.state().total, 3);

    // Blank typed ids are rejected before any store access.
    let blank = StateMachineEventRouter::new(Arc::clone(&executor))
        .for_event::<Placed, _>(|_| "   ".to_string());
    let error = blank.route(&Placed(1)).await.expect_err("a blank id fails");
    assert_eq!(error.code(), ErrorCode::Validation);
    assert!(error.message().contains("cannot be empty"));

    // Blank fallback ids are rejected too.
    let blank_fallback = StateMachineEventRouter::new(Arc::clone(&executor))
        .with_fallback(|_| Ok(String::from(" ")));
    let error = blank_fallback
        .route(&Audited(1))
        .await
        .expect_err("a blank fallback id fails");
    assert_eq!(error.code(), ErrorCode::Validation);

    // Fallback resolver errors propagate.
    let error = router
        .route(&Ignored)
        .await
        .expect_err("an unknown fallback event fails");
    assert_eq!(error.code(), ErrorCode::Unsupported);
    assert!(error.message().contains("unknown fallback event"));
}

#[tokio::test]
async fn router_rejects_unregistered_events_without_a_fallback() {
    let executor = Arc::new(StateMachineExecutor::new(
        order_machine(),
        MemStore::new("ok"),
    ));
    let router = StateMachineEventRouter::new(executor).for_event::<Placed, _>(|_| "x".into());
    let error = router
        .route(&PaidEvt)
        .await
        .expect_err("no route is registered");
    assert_eq!(error.code(), ErrorCode::Unsupported);
    assert!(error.message().contains("no state-machine route"));
}

// ---------------------------------------------------------------------------
// StateMachineSnapshot contracts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn snapshot_new_and_restore_validate_their_inputs() {
    let snapshot = StateMachineSnapshot::new("instance", Order::default());
    assert_eq!(snapshot.instance_id(), "instance");
    assert_eq!(snapshot.version(), 0);
    assert_eq!(snapshot.created_at(), snapshot.updated_at());

    let created = SystemTime::now();
    let updated = created + Duration::from_secs(2);
    let restored = StateMachineSnapshot::restore("i2", Order::default(), 3, created, updated)
        .expect("valid restore succeeds");
    assert_eq!(restored.version(), 3);
    assert_eq!(restored.created_at(), created);
    assert_eq!(restored.updated_at(), updated);

    let error = StateMachineSnapshot::restore("i3", Order::default(), -1, created, updated)
        .expect_err("negative versions fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    let error = StateMachineSnapshot::restore("i4", Order::default(), 0, updated, created)
        .expect_err("rewound clocks fail");
    assert_eq!(error.code(), ErrorCode::Validation);
    assert!(error.message().contains("precede"));
}

#[tokio::test]
async fn snapshot_versions_advance_and_saturate() {
    let snapshot = StateMachineSnapshot::new("instance", Order::default());
    let next = snapshot
        .next_version(Order {
            total: 1,
            ..Default::default()
        })
        .expect("the version advances");
    assert_eq!(next.version(), 1);
    assert_eq!(next.created_at(), snapshot.created_at());
    assert!(next.updated_at() >= snapshot.updated_at());
    assert_eq!(next.state().total, 1);

    let maxed = StateMachineSnapshot::restore(
        "instance",
        Order::default(),
        i64::MAX,
        SystemTime::now(),
        SystemTime::now(),
    )
    .expect("restore succeeds");
    let error = maxed
        .next_version(Order::default())
        .expect_err("the version saturates");
    assert_eq!(error.code(), ErrorCode::Conflict);

    assert!(StateMachineSnapshot::<Order>::is_next_version(0, 1));
    assert!(StateMachineSnapshot::<Order>::is_next_version(41, 42));
    assert!(!StateMachineSnapshot::<Order>::is_next_version(1, 1));
    assert!(!StateMachineSnapshot::<Order>::is_next_version(1, 3));
    assert!(!StateMachineSnapshot::<Order>::is_next_version(
        i64::MAX,
        i64::MIN
    ));
    assert!(!StateMachineSnapshot::<Order>::is_next_version(
        i64::MAX,
        i64::MAX
    ));
}

// ---------------------------------------------------------------------------
// Persistence frame contracts
// ---------------------------------------------------------------------------

/// Little-endian order codec used by the persistence tests.
struct OrderCodec;

impl SnapshotCodec<Order> for OrderCodec {
    fn encode_state(&self, state: &Order) -> CatgaResult<Vec<u8>> {
        let mut bytes = state.total.to_le_bytes().to_vec();
        bytes.push(state.state as u8);
        Ok(bytes)
    }

    fn decode_state(&self, bytes: &[u8]) -> CatgaResult<Order> {
        if bytes.len() != 9 {
            return Err(CatgaError::new(
                ErrorCode::Internal,
                "order payload is corrupt",
            ));
        }
        let state = match bytes[8] {
            0 => St::New,
            1 => St::Paid,
            2 => St::Shipped,
            3 => St::Cancelled,
            _ => return Err(CatgaError::new(ErrorCode::Internal, "unknown state byte")),
        };
        Ok(Order {
            state,
            total: u64::from_le_bytes(bytes[..8].try_into().expect("sized slice")),
            log: Vec::new(),
        })
    }
}

/// Codec whose decoder always fails, exercising propagation.
struct BrokenCodec;

impl SnapshotCodec<Order> for BrokenCodec {
    fn encode_state(&self, _: &Order) -> CatgaResult<Vec<u8>> {
        Ok(vec![0])
    }

    fn decode_state(&self, _: &[u8]) -> CatgaResult<Order> {
        Err(CatgaError::new(ErrorCode::Internal, "decoder unavailable"))
    }
}

/// Encodes a fresh snapshot and returns its frame for corruption tests.
fn encoded_frame(total: u64) -> Vec<u8> {
    let snapshot = StateMachineSnapshot::new(
        "frame",
        Order {
            state: St::Paid,
            total,
            log: Vec::new(),
        },
    );
    encode_state_machine_snapshot(&snapshot, &OrderCodec).expect("encoding succeeds")
}

#[tokio::test]
async fn snapshot_frames_round_trip_through_the_codec() {
    let snapshot = StateMachineSnapshot::new(
        "round",
        Order {
            state: St::Shipped,
            total: 123,
            log: Vec::new(),
        },
    );
    let bytes = encode_state_machine_snapshot(&snapshot, &OrderCodec).expect("encodes");
    let decoded = decode_state_machine_snapshot("round", &bytes, &OrderCodec).expect("decodes");
    assert_eq!(decoded.instance_id(), "round");
    assert_eq!(decoded.version(), snapshot.version());
    assert_eq!(decoded.created_at(), snapshot.created_at());
    assert_eq!(decoded.updated_at(), snapshot.updated_at());
    assert_eq!(decoded.state().total, 123);
    assert_eq!(decoded.state().current_state(), &St::Shipped);
}

#[tokio::test]
async fn snapshot_decoding_rejects_corrupt_frames() {
    // A truncated frame misses its metadata.
    let error = decode_state_machine_snapshot::<Order, _>("x", &[1, 0, 0], &OrderCodec)
        .expect_err("short frames fail");
    assert_eq!(error.code(), ErrorCode::Internal);
    assert!(error.message().contains("missing metadata"));

    // An unknown format version is rejected.
    let mut frame = encoded_frame(1);
    frame[0] = 9;
    let error = decode_state_machine_snapshot::<Order, _>("x", &frame, &OrderCodec)
        .expect_err("unknown versions fail");
    assert_eq!(error.code(), ErrorCode::Internal);
    assert!(error.message().contains("unsupported"));

    // A malformed creation time wire surfaces with its position named.
    let mut frame = encoded_frame(2);
    frame[9] = 2; // invalid epoch flag in the created-at wire
    let error = decode_state_machine_snapshot::<Order, _>("x", &frame, &OrderCodec)
        .expect_err("a corrupt creation time fails");
    assert_eq!(error.code(), ErrorCode::Internal);
    assert!(error.message().contains("creation time"));

    // A malformed update time wire surfaces with its position named.
    let mut frame = encoded_frame(3);
    frame[22] = 2; // invalid epoch flag in the updated-at wire
    let error = decode_state_machine_snapshot::<Order, _>("x", &frame, &OrderCodec)
        .expect_err("a corrupt update time fails");
    assert_eq!(error.code(), ErrorCode::Internal);
    assert!(error.message().contains("update time"));

    // Out-of-range nanoseconds also poison the update time wire.
    let mut frame = encoded_frame(4);
    frame[31..35].copy_from_slice(&u32::MAX.to_be_bytes());
    let error = decode_state_machine_snapshot::<Order, _>("x", &frame, &OrderCodec)
        .expect_err("out-of-range nanoseconds fail");
    assert_eq!(error.code(), ErrorCode::Internal);
    assert!(error.message().contains("update time"));

    // State codec failures propagate.
    let frame = encoded_frame(5);
    let error = decode_state_machine_snapshot::<Order, _>("x", &frame, &BrokenCodec)
        .expect_err("a broken codec fails");
    assert_eq!(error.code(), ErrorCode::Internal);
    assert!(error.message().contains("decoder unavailable"));
}
