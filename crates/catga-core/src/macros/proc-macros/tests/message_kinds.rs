//! Behavioral contracts for `#[catga_request]`, `#[derive(catga_command)]`, and
//! `#[derive(catga_event)]`: response types, generics, visibility, and dispatch.

use catga_core::{
    CatgaResult, Mediator, Registry, Request, command_handler, event_handler, request_handler,
};

#[catga_core::catga_request(response = u64)]
pub struct GetPrice {
    pub item: u32,
}

#[catga_core::catga_request(response = std::string::String)]
pub struct GetName;

#[catga_core::catga_request(response = Vec<u64>)]
pub struct GetList;

#[catga_core::catga_request(response = u32)]
pub enum Choice {
    A,
    B,
}

#[catga_core::catga_request(response = u64)]
pub struct SizedRequest<const N: usize>(pub [u8; N]);

#[derive(catga_core::catga_command)]
pub enum Toggle {
    On,
    Off,
}

#[derive(catga_core::catga_command)]
pub struct SizedCommand<const N: usize>(pub [u8; N]);

#[derive(Clone, catga_core::catga_event)]
pub struct Envelope<T> {
    pub payload: T,
}

#[derive(Clone, catga_core::catga_event)]
pub enum SysEvent {
    Started,
    Stopped,
}

mod scoped_a {
    #[derive(catga_core::catga_command)]
    pub struct Ping;
}

mod scoped_b {
    #[derive(catga_core::catga_command)]
    pub struct Ping;
}

mod scoped_c {
    #[catga_core::catga_request(response = u8)]
    pub(crate) struct Internal;
}

// Generic messages: bounded type parameters must not break expansion, and unbounded
// parameters must still satisfy `Message`'s `Send + Sync + 'static` supertraits through the
// bounds the macros add.
#[catga_core::catga_request(response = u64)]
pub struct GenericAsk<T: std::fmt::Debug> {
    pub payload: T,
}

#[catga_core::catga_request(response = u64)]
pub struct OpenAsk<T> {
    pub payload: T,
}

#[derive(catga_core::catga_command)]
pub struct BoundDo<T: Clone> {
    pub payload: T,
}

#[derive(Clone, catga_core::catga_event)]
pub struct BoundHappened<T: std::fmt::Debug> {
    pub payload: T,
}

#[test]
fn response_types_accept_paths_and_generics() {
    let _internal = scoped_c::Internal;
    assert_eq!(
        std::any::type_name::<<GetPrice as Request>::Response>(),
        std::any::type_name::<u64>()
    );
    assert_eq!(
        std::any::type_name::<<GetName as Request>::Response>(),
        std::any::type_name::<String>()
    );
    assert_eq!(
        std::any::type_name::<<GetList as Request>::Response>(),
        std::any::type_name::<Vec<u64>>()
    );
}

#[tokio::test]
async fn derived_messages_dispatch_end_to_end() -> CatgaResult<()> {
    let mut registry = Registry::new();
    registry.register_request::<GetPrice, _>(request_handler(|q: GetPrice| async move {
        Ok(u64::from(q.item) * 2)
    }))?;
    registry.register_request::<GetName, _>(request_handler(|_: GetName| async {
        Ok("catga".to_owned())
    }))?;
    registry.register_request::<GetList, _>(request_handler(|_: GetList| async {
        Ok(vec![1, 2, 3])
    }))?;
    registry.register_request::<Choice, _>(request_handler(|choice: Choice| async move {
        Ok(match choice {
            Choice::A => 1,
            Choice::B => 2,
        })
    }))?;
    registry.register_request::<SizedRequest<2>, _>(request_handler(
        |q: SizedRequest<2>| async move { Ok(q.0.len() as u64) },
    ))?;
    registry.register_command::<Toggle, _>(command_handler(|_: Toggle| async { Ok(()) }))?;
    registry.register_command::<SizedCommand<3>, _>(command_handler(
        |_: SizedCommand<3>| async { Ok(()) },
    ))?;
    registry.register_command::<scoped_a::Ping, _>(command_handler(|_: scoped_a::Ping| async {
        Ok(())
    }))?;
    registry.register_command::<scoped_b::Ping, _>(command_handler(|_: scoped_b::Ping| async {
        Ok(())
    }))?;
    registry.register_event::<Envelope<u8>, _>(event_handler(|_: Envelope<u8>| async { Ok(()) }));
    registry.register_event::<SysEvent, _>(event_handler(|_: SysEvent| async { Ok(()) }));

    let mediator = Mediator::new(registry);
    assert_eq!(mediator.send(GetPrice { item: 21 }).await?, 42);
    assert_eq!(mediator.send(GetName).await?, "catga");
    assert_eq!(mediator.send(GetList).await?, vec![1, 2, 3]);
    assert_eq!(mediator.send(Choice::A).await?, 1);
    assert_eq!(mediator.send(Choice::B).await?, 2);
    assert_eq!(mediator.send(SizedRequest([1u8, 2])).await?, 2);
    mediator.send_command(Toggle::On).await?;
    mediator.send_command(Toggle::Off).await?;
    mediator.send_command(SizedCommand([1u8, 2, 3])).await?;
    mediator.send_command(scoped_a::Ping).await?;
    mediator.send_command(scoped_b::Ping).await?;
    mediator.publish(Envelope { payload: 1u8 }).await?;
    mediator.publish(SysEvent::Started).await?;
    mediator.publish(SysEvent::Stopped).await?;
    Ok(())
}

#[tokio::test]
async fn generic_messages_dispatch_end_to_end() -> CatgaResult<()> {
    let mut registry = Registry::new();
    registry.register_request::<GenericAsk<u32>, _>(request_handler(
        |q: GenericAsk<u32>| async move { Ok(u64::from(q.payload) + 1) },
    ))?;
    registry.register_request::<OpenAsk<u32>, _>(request_handler(
        |q: OpenAsk<u32>| async move { Ok(u64::from(q.payload) + 2) },
    ))?;
    registry
        .register_command::<BoundDo<u8>, _>(command_handler(|_: BoundDo<u8>| async { Ok(()) }))?;
    registry.register_event::<BoundHappened<u8>, _>(event_handler(|_: BoundHappened<u8>| async {
        Ok(())
    }));

    let mediator = Mediator::new(registry);
    assert_eq!(mediator.send(GenericAsk { payload: 41u32 }).await?, 42);
    assert_eq!(mediator.send(OpenAsk { payload: 40u32 }).await?, 42);
    mediator.send_command(BoundDo { payload: 1u8 }).await?;
    mediator.publish(BoundHappened { payload: 2u8 }).await?;
    Ok(())
}
