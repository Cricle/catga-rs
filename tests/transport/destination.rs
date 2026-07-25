//! Destination transport compatibility tests.

use catga_core::{
    CatgaResult, Destination, DestinationTransport, Envelope, ErrorCode, MessageMetadata,
    MessageTransport, Stoppable,
};
use catga_memory::MemoryTransport;

fn envelope(id: u64) -> Envelope {
    Envelope::new(
        id,
        "order.created",
        vec![id as u8],
        MessageMetadata::new(id, None),
    )
}

#[test]
fn destination_rejects_empty_or_whitespace_only_names() {
    for name in ["", " ", "\t\n"] {
        assert!(matches!(
            Destination::parse(name),
            Err(error) if error.code() == ErrorCode::Validation
        ));
    }
}

#[tokio::test]
async fn destination_batch_rejects_zero_concurrency_before_attempting_any_send() -> CatgaResult<()>
{
    let transport = MemoryTransport::new(1)?;
    let destination = Destination::parse("orders")?;
    transport.declare_destination(destination.clone())?;

    assert!(matches!(
        transport
            .send_batch_to_with_concurrency(&destination, vec![envelope(1)], 0)
            .await,
        Err(error) if error.code() == ErrorCode::Validation
    ));
    Ok(())
}

#[tokio::test]
async fn memory_destination_is_explicit_fifo_and_rejects_sends_after_stopping() -> CatgaResult<()> {
    let transport = MemoryTransport::new(1)?;
    let destination = Destination::parse("orders")?;
    let unknown = Destination::parse("unknown")?;
    transport.declare_destination(destination.clone())?;

    assert!(matches!(
        transport.send_to(&unknown, envelope(99)).await,
        Err(error) if error.code() == ErrorCode::NotFound
    ));

    transport.send_to(&destination, envelope(1)).await?;
    let delivery = transport.receive_from(&destination).await?;
    assert_eq!(delivery.envelope().id(), 1);
    transport.ack(delivery).await?;

    transport.stop_accepting();
    assert!(matches!(
        transport.send_to(&destination, envelope(2)).await,
        Err(error) if error.code() == ErrorCode::Unavailable
    ));
    Ok(())
}
