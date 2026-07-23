//! Envelope codec tests.

use catga_codec_postcard::PostcardCodec;
use catga_core::{Envelope, EnvelopeCodec, MessageMetadata};

#[test]
fn postcard_codec_round_trips_envelope_metadata_and_payload() {
    let envelope = Envelope::new(
        42,
        "order.created",
        vec![1, 2, 3],
        MessageMetadata::new(42, Some(9)),
    );
    let codec = PostcardCodec;

    let decoded = codec.decode(&codec.encode(&envelope).unwrap()).unwrap();

    assert_eq!(decoded.id(), 42);
    assert_eq!(decoded.message_type(), "order.created");
    assert_eq!(decoded.payload(), [1, 2, 3]);
    assert_eq!(decoded.metadata(), MessageMetadata::new(42, Some(9)));
}
