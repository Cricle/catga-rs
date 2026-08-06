//! Tests for MemoryPack nullable types (NullableString, NullableVec).

use catga_core::{MemoryPackDeserialize, MemoryPackSerialize, NullableString, NullableVec};

fn round_trip<T>(value: &T) -> T
where
    T: MemoryPackSerialize + MemoryPackDeserialize,
{
    let bytes = MemoryPackSerializer::serialize(value).expect("encode");
    MemoryPackSerializer::deserialize(&bytes).expect("decode")
}

#[test]
fn nullable_string_some() {
    let original = NullableString(Some("hello".to_string()));
    let round_tripped = round_trip(&original);
    assert_eq!(round_tripped, original);
}

#[test]
fn nullable_string_none() {
    let original = NullableString(None);
    let round_tripped = round_trip(&original);
    assert_eq!(round_tripped, original);
}

#[test]
fn nullable_string_empty() {
    let original = NullableString(Some(String::new()));
    let round_tripped = round_trip(&original);
    assert_eq!(round_tripped, original);
}

#[test]
fn nullable_string_unicode() {
    let original = NullableString(Some("你好世界".to_string()));
    let round_tripped = round_trip(&original);
    assert_eq!(round_tripped, original);
}

#[test]
fn nullable_string_long() {
    let original = NullableString(Some("a".repeat(1000)));
    let round_tripped = round_trip(&original);
    assert_eq!(round_tripped, original);
}

#[test]
fn nullable_vec_some() {
    let original = NullableVec(Some(vec![1i32, 2, 3]));
    let round_tripped = round_trip(&original);
    assert_eq!(round_tripped, original);
}

#[test]
fn nullable_vec_none() {
    let original: NullableVec<i32> = NullableVec(None);
    let round_tripped = round_trip(&original);
    assert_eq!(round_tripped, original);
}

#[test]
fn nullable_vec_empty() {
    let original: NullableVec<i32> = NullableVec(Some(vec![]));
    let round_tripped = round_trip(&original);
    assert_eq!(round_tripped, original);
}

#[test]
fn nullable_vec_large() {
    let original: NullableVec<i32> = NullableVec(Some((0..1000).collect()));
    let round_tripped = round_trip(&original);
    assert_eq!(round_tripped, original);
}

#[test]
fn nullable_vec_nested() {
    let original = NullableVec(Some(vec![
        NullableVec(Some(vec![1i32, 2])),
        NullableVec(None),
        NullableVec(Some(vec![])),
    ]));
    let round_tripped = round_trip(&original);
    assert_eq!(round_tripped, original);
}

#[test]
fn nullable_string_debug() {
    let ns = NullableString(Some("test".to_string()));
    let debug_str = format!("{:?}", ns);
    assert!(debug_str.contains("NullableString"));
}

#[test]
fn nullable_vec_debug() {
    let nv: NullableVec<i32> = NullableVec(Some(vec![1, 2]));
    let debug_str = format!("{:?}", nv);
    assert!(debug_str.contains("NullableVec"));
}

#[test]
fn nullable_string_clone() {
    let original = NullableString(Some("clone me".to_string()));
    let cloned = original.clone();
    assert_eq!(original, cloned);
}

#[test]
fn nullable_vec_clone() {
    let original: NullableVec<i32> = NullableVec(Some(vec![1, 2, 3]));
    let cloned = original.clone();
    assert_eq!(original, cloned);
}

#[test]
fn nullable_vec_with_strings() {
    let original = NullableVec(Some(vec!["a".to_string(), "b".to_string(), "c".to_string()]));
    let round_tripped = round_trip(&original);
    assert_eq!(round_tripped, original);
}

#[test]
fn nullable_vec_with_option() {
    let original = NullableVec(Some(vec![
        Some(1i32),
        None,
        Some(2i32),
    ]));
    let round_tripped = round_trip(&original);
    assert_eq!(round_tripped, original);
}
