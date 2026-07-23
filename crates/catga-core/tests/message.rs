//! Message contract tests.

use catga_core::{Message, MessageMetadata, Request};

#[derive(Debug)]
struct CreateOrder {
    id: u64,
}

impl Message for CreateOrder {}

impl Request for CreateOrder {
    type Response = u64;
}

#[test]
fn request_metadata_preserves_message_and_correlation_ids() {
    let metadata = MessageMetadata::new(11, Some(3));

    assert_eq!(metadata.message_id(), 11);
    assert_eq!(metadata.correlation_id(), Some(3));
    let request = CreateOrder { id: 1 };
    assert_eq!(request.id, 1);
    assert!(request.message_type().ends_with("CreateOrder"));
}
