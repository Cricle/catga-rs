//! Benchmarks for message handler traits

#![feature(test)]

extern crate test;

use catga_core::{
    CatgaResult, Command, CommandHandler, Event, EventHandler, Handler,
    Message, MessageTypeId, Request,
};
use async_trait::async_trait;

// Define test types
struct PingTypeId;
impl MessageTypeId for PingTypeId { const NAME: &'static str = "Ping"; }

struct Ping(u64);
impl Message for Ping {}
impl Request for Ping {
    type Response = u64;
    type TypeId = PingTypeId;
}

struct PingHandler;
#[async_trait]
impl Handler<Ping> for PingHandler {
    async fn handle(&self, msg: Ping) -> CatgaResult<u64> {
        Ok(msg.0)
    }
}

struct CommandTypeId;
impl MessageTypeId for CommandTypeId { const NAME: &'static str = "Command"; }

struct MyCommand;
impl Message for MyCommand {}
impl Command for MyCommand { type TypeId = CommandTypeId; }

struct CommandHandlerImpl;
#[async_trait]
impl CommandHandler<MyCommand> for CommandHandlerImpl {
    async fn handle(&self, _: MyCommand) -> CatgaResult<()> {
        Ok(())
    }
}

#[derive(Clone)]
struct EventTypeId;
impl MessageTypeId for EventTypeId { const NAME: &'static str = "Event"; }

#[derive(Clone)]
struct MyEvent;
impl Message for MyEvent {}
impl Event for MyEvent { type TypeId = EventTypeId; }

struct EventHandlerImpl;
#[async_trait]
impl EventHandler<MyEvent> for EventHandlerImpl {
    async fn handle(&self, _: MyEvent) -> CatgaResult<()> {
        Ok(())
    }
}

// Benchmark: Handler implementation creation
#[bench]
fn bench_handler_creation(b: &mut test::Bencher) {
    b.iter(|| {
        let handler = PingHandler;
        test::black_box(handler);
    });
}

// Benchmark: Handler struct size
#[bench]
fn bench_handler_sizeof(b: &mut test::Bencher) {
    let handler = PingHandler;
    b.iter(|| {
        test::black_box(std::mem::size_of_val(&handler));
    });
}

// Benchmark: CommandHandler implementation size
#[bench]
fn bench_command_handler_sizeof(b: &mut test::Bencher) {
    let handler = CommandHandlerImpl;
    b.iter(|| {
        test::black_box(std::mem::size_of_val(&handler));
    });
}

// Benchmark: EventHandler implementation size
#[bench]
fn bench_event_handler_sizeof(b: &mut test::Bencher) {
    let handler = EventHandlerImpl;
    b.iter(|| {
        test::black_box(std::mem::size_of_val(&handler));
    });
}

// Benchmark: Message struct sizes
#[bench]
fn bench_message_struct_sizes(b: &mut test::Bencher) {
    b.iter(|| {
        let ping = Ping(42);
        let cmd = MyCommand;
        let evt = MyEvent;
        test::black_box(std::mem::size_of_val(&ping));
        test::black_box(std::mem::size_of_val(&cmd));
        test::black_box(std::mem::size_of_val(&evt));
    });
}

// Benchmark: Request message with payload size
#[bench]
fn bench_request_message_sizeof(b: &mut test::Bencher) {
    let msg = Ping(123456789);
    b.iter(|| {
        test::black_box(std::mem::size_of_val(&msg));
    });
}
