use catga_core::{CatgaResult, Envelope, MessageMetadata, MessageTransport};
use catga_memory::MemoryTransport;

#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    let transport = MemoryTransport::new(16)?;
    transport
        .publish(Envelope::new(
            1,
            "order.created",
            vec![1],
            MessageMetadata::new(1, None),
        ))
        .await?;
    let delivery = transport.receive().await?;
    assert_eq!(delivery.envelope().message_type(), "order.created");
    transport.ack(delivery).await
}
