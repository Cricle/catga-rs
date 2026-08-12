//! Compile-pass and behavioral tests for `#[memorypack(union)]` tagged unions.
//!
//! The wire frame is a `u8` tag (explicit `#[tag = N]` or the declaration ordinal) followed
//! by the serialized payload; decoding rejects unknown tags.

use catga_core::MemoryPackable;
use catga_core::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    MemoryPackSerializer, MemoryPackWriter,
};

#[derive(Clone, Debug, PartialEq, MemoryPackable)]
struct Leg {
    dx: i32,
    dy: i32,
}

#[derive(Clone, Debug, PartialEq, MemoryPackable)]
#[memorypack(union)]
enum Command {
    #[tag = 3]
    Ping(u32),
    Pong(String),
    Step(Leg),
    #[tag = 7]
    Halt(bool),
}

#[test]
fn union_variants_round_trip_with_their_tags() -> Result<(), MemoryPackError> {
    let cases = [
        Command::Ping(0x0102_0304),
        Command::Pong("hi".to_owned()),
        Command::Step(Leg { dx: -1, dy: 2 }),
        Command::Halt(true),
    ];
    for command in &cases {
        let bytes = MemoryPackSerializer::serialize(command)?;
        let decoded: Command = MemoryPackSerializer::deserialize(&bytes)?;
        assert_eq!(&decoded, command);
    }
    Ok(())
}

#[test]
fn union_frame_is_a_tag_byte_followed_by_the_payload() -> Result<(), MemoryPackError> {
    // Explicit `#[tag = 3]`: tag byte, then the little-endian `u32` payload.
    let ping = MemoryPackSerializer::serialize(&Command::Ping(0x0102_0304))?;
    assert_eq!(ping, [3, 4, 3, 2, 1]);

    // No explicit tag: the declaration ordinal (1) identifies the variant.
    let pong = MemoryPackSerializer::serialize(&Command::Pong(String::new()))?;
    assert_eq!(pong, [1, 0, 0, 0, 0]);

    // Struct payloads use the struct frame after the tag.
    let step = MemoryPackSerializer::serialize(&Command::Step(Leg { dx: 1, dy: 2 }))?;
    assert_eq!(step, [2, 2, 1, 0, 0, 0, 2, 0, 0, 0]);

    let halt = MemoryPackSerializer::serialize(&Command::Halt(true))?;
    assert_eq!(halt, [7, 1]);
    Ok(())
}

#[test]
fn union_deserialize_rejects_an_unknown_tag() {
    let mut writer = MemoryPackWriter::new();
    writer.write_u8(9).expect("write tag");
    writer.write_u8(0).expect("write payload");

    let result = MemoryPackSerializer::deserialize::<Command>(&writer.into_bytes());
    assert!(matches!(
        result,
        Err(MemoryPackError::DeserializationError(message))
            if message.contains("Unknown union tag 9") && message.contains("Command")
    ));
}
