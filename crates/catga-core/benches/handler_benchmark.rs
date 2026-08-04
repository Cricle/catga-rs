//! Benchmarks for message handler traits

#![feature(test)]

extern crate test;

use async_trait::async_trait;
use catga_core::{
    CatgaResult, Command, CommandHandler, Event, EventHandler, Handler, Message, MessageTypeId,
    Request,
};

// Define test types
struct PingTypeId;
impl MessageTypeId for PingTypeId {
    const NAME: &'static str = "Ping";
}

struct Ping(u64);
impl Message for Ping {}
impl Request for Ping {
    type Response = u64;
    type TypeId = PingTypeId;
}

#[derive(Clone)]
struct PingHandler;
#[async_trait]
impl Handler<Ping> for PingHandler {
    async fn handle(&self, msg: Ping) -> CatgaResult<u64> {
        Ok(msg.0)
    }
}

struct CommandTypeId;
impl MessageTypeId for CommandTypeId {
    const NAME: &'static str = "Command";
}

struct MyCommand;
impl Message for MyCommand {}
impl Command for MyCommand {
    type TypeId = CommandTypeId;
}

#[derive(Clone)]
struct CommandHandlerImpl;
#[async_trait]
impl CommandHandler<MyCommand> for CommandHandlerImpl {
    async fn handle(&self, _: MyCommand) -> CatgaResult<()> {
        Ok(())
    }
}

#[derive(Clone)]
struct EventTypeId;
impl MessageTypeId for EventTypeId {
    const NAME: &'static str = "Event";
}

#[derive(Clone)]
struct MyEvent;
impl Message for MyEvent {}
impl Event for MyEvent {
    type TypeId = EventTypeId;
}

#[derive(Clone)]
struct EventHandlerImpl;
#[async_trait]
impl EventHandler<MyEvent> for EventHandlerImpl {
    async fn handle(&self, _: MyEvent) -> CatgaResult<()> {
        Ok(())
    }
}

// Benchmark: Handler creation
#[bench]
fn bench_handler_creation(b: &mut test::Bencher) {
    b.iter(|| {
        let handler = PingHandler;
        test::black_box(handler);
    });
}

// Benchmark: Handler clone (for registry storage)
#[bench]
fn bench_handler_clone(b: &mut test::Bencher) {
    let handler = PingHandler;
    b.iter(|| {
        test::black_box(handler.clone());
    });
}

// Benchmark: Command handler clone
#[bench]
fn bench_command_handler_clone(b: &mut test::Bencher) {
    let handler = CommandHandlerImpl;
    b.iter(|| {
        test::black_box(handler.clone());
    });
}

// Benchmark: Event handler clone
#[bench]
fn bench_event_handler_clone(b: &mut test::Bencher) {
    let handler = EventHandlerImpl;
    b.iter(|| {
        test::black_box(handler.clone());
    });
}

// Benchmark: Message creation (with payload)
#[bench]
fn bench_message_creation_with_payload(b: &mut test::Bencher) {
    b.iter(|| {
        let msg = Ping(42);
        test::black_box(msg);
    });
}

// Benchmark: Message creation (empty)
#[bench]
fn bench_message_creation_empty(b: &mut test::Bencher) {
    b.iter(|| {
        let msg = MyCommand;
        test::black_box(msg);
    });
}
