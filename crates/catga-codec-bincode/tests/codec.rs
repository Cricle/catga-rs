#![allow(missing_docs)]

use bincode_next::{Decode, Encode};
use catga_codec_bincode::{BincodeCodec, MAX_BINCODE_FRAME_BYTES};
use catga_core::{PayloadDecoder, PayloadEncoder};

#[derive(Debug, Decode, Encode, Eq, PartialEq)]
struct Payment {
    id: u64,
    amount_cents: u32,
}

#[test]
fn round_trips_native_bincode_payloads_and_rejects_trailing_bytes() -> catga_core::CatgaResult<()> {
    let codec = BincodeCodec;
    let payment = Payment {
        id: 42,
        amount_cents: 1_250,
    };
    let mut encoded = codec.encode_payload(&payment)?;

    assert_eq!(
        <BincodeCodec as PayloadDecoder<Payment>>::decode_payload(&codec, &encoded)?,
        payment
    );
    encoded.push(0);
    assert!(<BincodeCodec as PayloadDecoder<Payment>>::decode_payload(&codec, &encoded).is_err());
    Ok(())
}

#[test]
fn rejects_oversized_frames_before_decode() {
    let codec = BincodeCodec;
    let oversized = vec![0_u8; MAX_BINCODE_FRAME_BYTES + 1];

    assert!(<BincodeCodec as PayloadDecoder<Payment>>::decode_payload(&codec, &oversized).is_err());
}
