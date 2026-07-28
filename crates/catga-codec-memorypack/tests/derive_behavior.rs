//! Runtime coverage for the supported `MemoryPackable` schema forms.

use catga_codec_memorypack::{MemoryPackError, MemoryPackSerializer, MemoryPackable};

#[derive(Debug, Eq, PartialEq, MemoryPackable)]
struct OrderedRecord {
    #[memorypack(order = 1)]
    second: u8,
    #[memorypack(order = 0)]
    first: u8,
    #[memorypack(skip)]
    cached: u8,
}

#[derive(Debug, Eq, PartialEq, MemoryPackable)]
struct TupleRecord(u16, String);

#[derive(Debug, Eq, PartialEq, MemoryPackable)]
struct UnitRecord;

#[derive(Clone, Copy, Debug, Eq, PartialEq, MemoryPackable)]
#[repr(transparent)]
#[memorypack(flags)]
struct Permissions(i32);

#[derive(Debug, Eq, PartialEq, MemoryPackable)]
#[repr(i32)]
enum Kind {
    First = -7,
    Second = 42,
}

#[derive(Debug, Eq, PartialEq, MemoryPackable)]
#[memorypack(union)]
enum DefaultTaggedUnion {
    Text(String),
    Number(u16),
}

#[derive(Debug, Eq, PartialEq, MemoryPackable)]
#[memorypack(zero_copy)]
struct BorrowedText<'a> {
    text: &'a str,
}

#[test]
fn derived_named_fields_follow_wire_order_and_default_skipped_fields() {
    let value = OrderedRecord {
        second: 9,
        first: 4,
        cached: 99,
    };
    let frame = MemoryPackSerializer::serialize(&value).expect("record serializes");

    assert_eq!(frame, [2, 4, 9]);
    assert_eq!(
        MemoryPackSerializer::deserialize::<OrderedRecord>(&frame).expect("record deserializes"),
        OrderedRecord {
            second: 9,
            first: 4,
            cached: 0,
        }
    );
}

#[test]
fn derived_tuple_and_unit_structs_round_trip() {
    let tuple = TupleRecord(513, String::from("tuple"));
    assert_eq!(
        MemoryPackSerializer::deserialize::<TupleRecord>(
            &MemoryPackSerializer::serialize(&tuple).expect("tuple serializes"),
        )
        .expect("tuple deserializes"),
        tuple
    );
    assert_eq!(
        MemoryPackSerializer::deserialize::<UnitRecord>(&[]).expect("unit deserializes"),
        UnitRecord
    );
}

#[test]
fn transparent_flag_newtypes_round_trip_and_expose_bit_operations() {
    let read = Permissions(0b001);
    let write = Permissions(0b010);
    let combined = read | write;

    assert!(combined.contains(read));
    assert_eq!(combined & write, write);
    assert_eq!(combined ^ write, read);
    assert!(Permissions(0).is_empty());
    assert_eq!(!Permissions(0), Permissions(-1));
    assert_eq!(
        MemoryPackSerializer::deserialize::<Permissions>(
            &MemoryPackSerializer::serialize(&combined).expect("flags serialize"),
        )
        .expect("flags deserialize"),
        combined
    );
}

#[test]
fn repr_i32_enums_reject_unknown_discriminants() {
    for (value, expected) in [(&Kind::First, -7_i32), (&Kind::Second, 42_i32)] {
        let frame = MemoryPackSerializer::serialize(value).expect("enum serializes");
        assert_eq!(frame, expected.to_le_bytes());
        let decoded =
            MemoryPackSerializer::deserialize::<Kind>(&frame).expect("known enum deserializes");
        assert!(matches!(
            (expected, decoded),
            (-7, Kind::First) | (42, Kind::Second)
        ));
    }

    let error = MemoryPackSerializer::deserialize::<Kind>(&0_i32.to_le_bytes())
        .expect_err("unknown discriminant is not accepted");
    assert!(matches!(error, MemoryPackError::DeserializationError(_)));
}

#[test]
fn default_tag_unions_round_trip_and_reject_unknown_tags() {
    let text = DefaultTaggedUnion::Text(String::from("union"));
    let number = DefaultTaggedUnion::Number(42);

    for value in [text, number] {
        assert_eq!(
            MemoryPackSerializer::deserialize::<DefaultTaggedUnion>(
                &MemoryPackSerializer::serialize(&value).expect("union serializes"),
            )
            .expect("union deserializes"),
            value
        );
    }

    let error = MemoryPackSerializer::deserialize::<DefaultTaggedUnion>(&[9])
        .expect_err("unknown tag is rejected before decoding a payload");
    assert!(matches!(error, MemoryPackError::DeserializationError(_)));
}

#[test]
fn zero_copy_derived_strings_borrow_the_received_frame() {
    let value = BorrowedText { text: "borrowed" };
    let frame = MemoryPackSerializer::serialize(&value).expect("zero-copy text serializes");
    let decoded = MemoryPackSerializer::deserialize_zero_copy::<BorrowedText<'_>>(&frame)
        .expect("zero-copy text deserializes");

    assert_eq!(decoded, value);
    assert_eq!(decoded.text.as_ptr(), frame[9..].as_ptr());
}
