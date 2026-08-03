//! Tests for Bus ↔ StateMachine integration via StateMachineHandler.
#![cfg(feature = "flow")]

use std::sync::Arc;

use catga_auto::{Bus, StateMachineHandler};
use catga_codec_memorypack::{MemoryPackCodec, MemoryPackable};
use catga_core::{Event, Message};
use catga_flow::{
    StateMachine, StateMachineEventRouter, StateMachineExecutor, StateMachineState,
    StateMachineStore,
};
use catga_memory::{MemoryStateMachines, MemoryTransport};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum OrderState {
    Pending,
    Paid,
}

#[derive(Clone, Debug)]
struct OrderInstance {
    state: OrderState,
    total: u32,
}

impl StateMachineState<OrderState> for OrderInstance {
    fn current_state(&self) -> &OrderState {
        &self.state
    }
    fn set_current_state(&mut self, state: OrderState) {
        self.state = state;
    }
}

#[derive(Clone, MemoryPackable)]
struct PaymentReceived {
    order_id: String,
    amount: u32,
}
impl Message for PaymentReceived {}
impl Event for PaymentReceived { type TypeId = catga_core::DefaultMessageTypeId; }

fn build_machine() -> StateMachine<OrderInstance, OrderState> {
    let mut definition = StateMachine::<OrderInstance, OrderState>::builder(OrderState::Pending);
    definition
        .state(OrderState::Pending)
        .on::<PaymentReceived>()
        .execute(|instance, event| {
            instance.total = event.amount;
            Ok(())
        })
        .transition_to(OrderState::Paid);
    definition.build()
}

#[tokio::test(flavor = "current_thread")]
async fn event_drives_state_machine_transition_via_bus() {
    let machine = build_machine();
    let store = Arc::new(MemoryStateMachines::default());
    let executor = Arc::new(StateMachineExecutor::new(machine, Arc::clone(&store)));
    executor
        .initialize(
            "order-1",
            OrderInstance {
                state: OrderState::Pending,
                total: 0,
            },
        )
        .await
        .expect("initialize");

    let router = Arc::new(
        StateMachineEventRouter::new(Arc::clone(&executor))
            .for_event::<PaymentReceived, _>(|e| e.order_id.clone()),
    );

    let transport = Arc::new(MemoryTransport::new(64).expect("transport"));
    let handler = Arc::new(StateMachineHandler::new(router));

    let bus = Bus::builder(Arc::clone(&transport))
        .endpoint::<PaymentReceived, _, _>(
            "payments",
            handler,
            Arc::new(MemoryPackCodec::default()),
            1,
        )
        .expect("endpoint")
        .build();

    let publisher = {
        let ids = Arc::new(
            catga_core::SnowflakeIdGenerator::new(1, catga_core::SnowflakeLayout::default())
                .expect("ids"),
        );
        catga_core::TypedTransport::<MemoryTransport, MemoryPackCodec>::new(
            Arc::clone(&transport),
            ids,
        )
    };
    publisher
        .publish(&PaymentReceived {
            order_id: "order-1".into(),
            amount: 99,
        })
        .await
        .expect("publish");

    let token = bus.shutdown_token();
    let run = async { bus.run_until_cancelled().await };
    let stop = async {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        token.cancel();
    };
    let (result, _) = tokio::join!(run, stop);
    let runs = result.expect("bus run");
    assert_eq!(runs[0].acknowledged(), 1);

    let snapshot = store.get("order-1").await.expect("get").expect("exists");
    assert_eq!(*snapshot.state().current_state(), OrderState::Paid);
    assert_eq!(snapshot.state().total, 99);
}

#[tokio::test(flavor = "current_thread")]
async fn unregistered_event_type_errors() {
    let machine = build_machine();
    let store = Arc::new(MemoryStateMachines::default());
    let executor = Arc::new(StateMachineExecutor::new(machine, store));
    let router = Arc::new(StateMachineEventRouter::new(executor));

    let transport = Arc::new(MemoryTransport::new(64).expect("transport"));
    let handler = Arc::new(StateMachineHandler::new(router));

    let bus = Bus::builder(Arc::clone(&transport))
        .endpoint::<PaymentReceived, _, _>(
            "payments",
            handler,
            Arc::new(MemoryPackCodec::default()),
            1,
        )
        .expect("endpoint")
        .build();

    let publisher = {
        let ids = Arc::new(
            catga_core::SnowflakeIdGenerator::new(1, catga_core::SnowflakeLayout::default())
                .expect("ids"),
        );
        catga_core::TypedTransport::<MemoryTransport, MemoryPackCodec>::new(
            Arc::clone(&transport),
            ids,
        )
    };
    publisher
        .publish(&PaymentReceived {
            order_id: "order-1".into(),
            amount: 50,
        })
        .await
        .expect("publish");

    let token = bus.shutdown_token();
    let run = async { bus.run_until_cancelled().await };
    let stop = async {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        token.cancel();
    };
    let (result, _) = tokio::join!(run, stop);
    // MemoryTransport nack → Unsupported error propagates.
    assert!(result.is_err());
}
