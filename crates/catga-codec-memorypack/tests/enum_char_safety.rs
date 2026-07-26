use catga_codec_memorypack::{MemoryPackError, MemoryPackSerializer, MemoryPackable};

#[derive(Clone, Copy, Debug, Eq, PartialEq, MemoryPackable)]
#[repr(i32)]
enum DeliveryState {
    Pending,
    Delivered,
    Returned,
}

#[test]
fn repr_i32_enum_rejects_unknown_wire_discriminants() {
    let error = MemoryPackSerializer::deserialize::<DeliveryState>(&99_i32.to_le_bytes())
        .expect_err("unknown enum discriminants must be rejected");

    assert!(matches!(error, MemoryPackError::DeserializationError(_)));
}

#[test]
fn repr_i32_enum_round_trips_known_discriminants() {
    for state in [
        DeliveryState::Pending,
        DeliveryState::Delivered,
        DeliveryState::Returned,
    ] {
        let encoded = MemoryPackSerializer::serialize(&state).expect("enum serializes");
        let decoded = MemoryPackSerializer::deserialize::<DeliveryState>(&encoded)
            .expect("known enum deserializes");

        assert_eq!(decoded, state);
    }
}

#[test]
fn astral_char_round_trips_through_the_serializer() {
    let encoded = MemoryPackSerializer::serialize(&'😀').expect("char serializes");
    let decoded = MemoryPackSerializer::deserialize::<char>(&encoded).expect("char deserializes");

    assert_eq!(decoded, '😀');
}

#[test]
fn unpaired_utf16_surrogates_are_rejected() {
    for bytes in [[0x00, 0xD8], [0x00, 0xDC]] {
        assert!(MemoryPackSerializer::deserialize::<char>(&bytes).is_err());
    }
}
