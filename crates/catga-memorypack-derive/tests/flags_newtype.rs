//! Compile-pass and behavioral tests for `#[memorypack(flags)]` transparent newtypes.
//!
//! The flags expansion must emit a valid `std::ops::Not` impl for the newtype itself; a
//! regression emitted `impl std::ops::Not for Self`, which is invalid Rust.

use catga_core::MemoryPackable;
use catga_core::codec::memorypack::{
    MemoryPackDeserialize, MemoryPackError, MemoryPackReader, MemoryPackSerialize,
    MemoryPackSerializer, MemoryPackWriter,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, MemoryPackable)]
#[repr(transparent)]
#[memorypack(flags)]
struct Permission(i32);

const READ: Permission = Permission(0b001);
const WRITE: Permission = Permission(0b010);

#[test]
fn flags_newtype_supports_all_bitwise_operators() {
    let both = READ | WRITE;
    assert_eq!(both, Permission(0b011));
    assert_eq!(both & READ, READ);
    assert_eq!(both ^ READ, WRITE);
    assert_eq!(!Permission(0), Permission(-1));
    assert_eq!(!both, Permission(!0b011));
}

#[test]
fn flags_newtype_contains_and_is_empty() {
    let both = READ | WRITE;
    assert!(both.contains(READ));
    assert!(both.contains(WRITE));
    assert!(!READ.contains(WRITE));
    assert!(Permission(0).is_empty());
    assert!(!both.is_empty());
}

#[test]
fn flags_newtype_round_trips_through_memorypack() -> Result<(), MemoryPackError> {
    let flags = READ | WRITE;
    let bytes = MemoryPackSerializer::serialize(&flags)?;
    let decoded: Permission = MemoryPackSerializer::deserialize(&bytes)?;
    assert_eq!(decoded, flags);
    Ok(())
}
