//! Memory layout tests for Registry

use catga_core::{CatgaResult, Handler, Message, MessageTypeId, Registry, Request};

struct PingTypeId;
impl MessageTypeId for PingTypeId { const NAME: &'static str = "Ping"; }
struct Ping;
impl Message for Ping {}
impl Request for Ping { type Response = String; type TypeId = PingTypeId; }

struct QueryTypeId;
impl MessageTypeId for QueryTypeId { const NAME: &'static str = "Query"; }
struct Query;
impl Message for Query {}
impl Request for Query { type Response = String; type TypeId = QueryTypeId; }

struct HeavyTypeId;
impl MessageTypeId for HeavyTypeId { const NAME: &'static str = "Heavy"; }
struct Heavy;
impl Message for Heavy {}
impl Request for Heavy { type Response = String; type TypeId = HeavyTypeId; }

struct PingHandler;
#[async_trait::async_trait]
impl Handler<Ping> for PingHandler {
    async fn handle(&self, _: Ping) -> CatgaResult<String> { Ok("ok".to_string()) }
}

struct QueryHandler;
#[async_trait::async_trait]
impl Handler<Query> for QueryHandler {
    async fn handle(&self, _: Query) -> CatgaResult<String> { Ok("query result".to_string()) }
}

struct HeavyHandler;
#[async_trait::async_trait]
impl Handler<Heavy> for HeavyHandler {
    async fn handle(&self, _: Heavy) -> CatgaResult<String> { Ok("heavy".to_string()) }
}

#[test]
fn print_struct_sizes() {
    println!("\n=== Registry Memory Layout ===");
    println!("Registry: {} bytes", std::mem::size_of::<Registry>());
    println!("PingHandler: {} bytes", std::mem::size_of::<PingHandler>());
    println!("QueryHandler: {} bytes", std::mem::size_of::<QueryHandler>());
    println!("HeavyHandler: {} bytes", std::mem::size_of::<HeavyHandler>());
}

#[test]
fn compare_handler_sizes() {
    println!("\n=== Handler Size Comparison ===");

    struct EmptyHandler;
    #[async_trait::async_trait]
    impl Handler<Ping> for EmptyHandler {
        async fn handle(&self, _: Ping) -> CatgaResult<String> { Ok("ok".to_string()) }
    }

    struct DataHandler { data: [u8; 64] }
    #[async_trait::async_trait]
    impl Handler<Ping> for DataHandler {
        async fn handle(&self, _: Ping) -> CatgaResult<String> { Ok("ok".to_string()) }
    }

    println!("EmptyHandler: {} bytes", std::mem::size_of::<EmptyHandler>());
    println!("DataHandler (64 bytes data): {} bytes", std::mem::size_of::<DataHandler>());
    println!("");
    println!("Note: Handler size directly affects Arc<Handler> allocation");
    println!("Empty handler = 1 byte on stack, no heap allocation");
    println!("DataHandler = 64 bytes on stack, may heap allocate if >75% of Box");
}

#[test]
fn heap_allocations() {
    println!("\n=== Heap Allocation Analysis ===");

    let mut registry = Registry::new();
    registry.register_request::<Ping, _>(PingHandler).unwrap();
    registry.register_request::<Query, _>(QueryHandler).unwrap();
    registry.register_request::<Heavy, _>(HeavyHandler).unwrap();

    println!("Registry with 3 handlers:");
    println!("  - HashMap: 3 * (key + RequestSlot) entries");
    println!("  - Each RequestSlot contains Arc<dyn ErasedRequestHandler>");
    println!("  - Each Arc points to RequestHandlerAdapter on heap");
    println!("  - RequestHandlerAdapter contains handler (ZST in this case)");
    println!("");
    println!("Total heap allocations per handler: 2 (Arc inner + Adapter)");
}

#[test]
fn zero_sized_types() {
    println!("\n=== Zero-Sized Types (ZST) ===");
    println!("PingHandler is ZST: {}", std::mem::size_of::<PingHandler>() == 0);
    println!("QueryHandler is ZST: {}", std::mem::size_of::<QueryHandler>() == 0);
    println!("HeavyHandler is ZST: {}", std::mem::size_of::<HeavyHandler>() == 0);
    println!("");
    println!("ZST benefit: Arc can optimize small types to avoid heap allocation");
    println!("Rule of 3: Box/ZST/Arc can inline <= 3 pointers worth of data");
}
