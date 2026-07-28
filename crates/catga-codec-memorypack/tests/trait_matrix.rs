//! End-to-end trait coverage for the public MemoryPack value matrix.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, LinkedList, VecDeque},
    fmt::Debug,
    rc::Rc,
    sync::Arc,
};

use catga_codec_memorypack::{
    MemoryPackDeserialize, MemoryPackSerialize, MemoryPackSerializer, NullableString, NullableVec,
};

fn assert_round_trip<T>(value: T)
where
    T: Debug + PartialEq + MemoryPackSerialize + MemoryPackDeserialize,
{
    let frame = MemoryPackSerializer::serialize(&value).expect("value serializes");
    let decoded: T = MemoryPackSerializer::deserialize(&frame).expect("frame deserializes exactly");
    assert_eq!(decoded, value);
}

#[test]
fn primitive_and_tuple_values_round_trip_at_wire_boundaries() {
    assert_round_trip((
        (),
        true,
        -128_i8,
        u8::MAX,
        i16::MIN,
        u16::MAX,
        i32::MIN,
        u32::MAX,
        i64::MIN,
        u64::MAX,
        i128::MIN,
        u128::MAX,
    ));
    assert_round_trip((f32::MIN_POSITIVE, -f64::MAX));
    assert_round_trip(('A', '🦀', '中'));
}

#[test]
fn standard_collections_preserve_values_and_ordering_contracts() {
    let mut hash_map = HashMap::new();
    hash_map.insert(String::from("first"), 1_u32);
    hash_map.insert(String::from("second"), 2_u32);

    let mut btree_map = BTreeMap::new();
    btree_map.insert(-2_i16, String::from("negative"));
    btree_map.insert(7_i16, String::from("positive"));

    assert_round_trip(vec![1_u16, 2, 3]);
    assert_round_trip([4_u8, 5, 6]);
    assert_round_trip(VecDeque::from([7_u8, 8, 9]));
    assert_round_trip(LinkedList::from([10_u8, 11, 12]));
    assert_round_trip(HashSet::from([13_u16, 14, 15]));
    assert_round_trip(BTreeSet::from([16_u16, 17, 18]));
    assert_round_trip(hash_map);
    assert_round_trip(btree_map);
}

#[test]
fn nullable_and_generic_options_preserve_absence_without_trailing_bytes() {
    assert_round_trip(Some(42_u64));
    assert_round_trip(Option::<u64>::None);
    assert_round_trip(NullableString(Some(String::from("optional text"))));
    assert_round_trip(NullableString(None));
    assert_round_trip(NullableVec(Some(vec![1_u8, 2, 3])));
    assert_round_trip(NullableVec::<u8>(None));
}

#[test]
fn smart_pointers_decode_to_owned_equivalent_values() {
    assert_round_trip(Box::new(42_u32));
    assert_round_trip(String::from("owned text").into_boxed_str());
    assert_round_trip(Rc::new(String::from("reference counted")));
    assert_round_trip(Arc::new(vec![1_u8, 3, 3, 7]));
}

#[cfg(feature = "hashbrown")]
#[test]
fn hashbrown_collections_round_trip() {
    let mut map = hashbrown::HashMap::new();
    map.insert(1_i32, String::from("one"));
    let set = hashbrown::HashSet::from([2_i32, 3]);

    assert_round_trip(map);
    assert_round_trip(set);
}

#[cfg(feature = "ahash")]
#[test]
fn ahash_collections_round_trip() {
    let mut map = ahash::AHashMap::new();
    map.insert('x', 9_u16);
    let set = ahash::AHashSet::from([4_i32, 5]);

    assert_round_trip(map);
    assert_round_trip(set);
}

#[cfg(feature = "chrono")]
#[test]
fn chrono_values_round_trip_with_memorypack_tick_precision() {
    use chrono::{FixedOffset, NaiveDate, NaiveTime, TimeDelta, TimeZone, Utc};

    let utc = Utc
        .timestamp_opt(1_717_171_717, 123_456_700)
        .single()
        .expect("timestamp is valid");
    let fixed = FixedOffset::east_opt(5 * 60 * 60 + 30 * 60)
        .expect("offset is valid")
        .from_utc_datetime(&utc.naive_utc());
    let local = utc.with_timezone(&chrono::Local);
    let time = NaiveTime::from_hms_nano_opt(23, 59, 58, 123_456_700).expect("time is valid");
    let date = NaiveDate::from_ymd_opt(2024, 2, 29).expect("date is valid");

    assert_round_trip(TimeDelta::seconds(-42) + TimeDelta::nanoseconds(700));
    assert_round_trip(utc);
    assert_round_trip(fixed);
    assert_round_trip(local);
    assert_round_trip(time);
    assert_round_trip(date);
}

#[cfg(feature = "glam")]
#[test]
fn glam_vectors_quaternions_and_affine_matrices_round_trip() {
    use glam::{Mat3A, Mat4, Quat, Vec2, Vec3, Vec3A, Vec4};

    assert_round_trip(Vec2::new(1.5, -2.5));
    assert_round_trip(Vec3::new(3.5, -4.5, 5.5));
    assert_round_trip(Vec4::new(6.5, -7.5, 8.5, -9.5));
    assert_round_trip(Quat::from_xyzw(0.1, -0.2, 0.3, 0.9));
    assert_round_trip(Mat3A::from_cols(
        Vec3A::new(1.0, 2.0, 0.0),
        Vec3A::new(3.0, 4.0, 0.0),
        Vec3A::new(5.0, 6.0, 1.0),
    ));
    assert_round_trip(Mat4::from_cols_array(&[
        1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0,
    ]));
}

#[cfg(feature = "num-complex")]
#[test]
fn complex_numbers_round_trip() {
    assert_round_trip(num_complex::Complex::new(-12.5_f64, 42.25));
}

#[cfg(feature = "uuid")]
#[test]
fn uuid_round_trips_in_its_fixed_width_wire_form() {
    assert_round_trip(
        uuid::Uuid::parse_str("12345678-1234-5678-9abc-def012345678").expect("UUID is valid"),
    );
}

#[cfg(feature = "rust_decimal")]
#[test]
fn decimal_round_trips_sign_scale_and_high_bits() {
    assert_round_trip(rust_decimal::Decimal::from_parts(u32::MAX, 1, 2, true, 28));
}

#[cfg(feature = "half")]
#[test]
fn half_precision_values_preserve_their_raw_bits() {
    let value = half::f16::from_bits(0x7e01);
    let frame = MemoryPackSerializer::serialize(&value).expect("half value serializes");
    let decoded =
        MemoryPackSerializer::deserialize::<half::f16>(&frame).expect("half value deserializes");

    assert_eq!(decoded.to_bits(), value.to_bits());
}

#[cfg(feature = "num-bigint")]
#[test]
fn arbitrary_precision_integers_round_trip_sign_and_magnitude() {
    use num_bigint::{BigInt, BigUint};

    for value in [
        BigInt::from(-32_769_i32),
        BigInt::from(-129_i32),
        BigInt::from(-128_i32),
        BigInt::from(0_i32),
        BigInt::from(127_i32),
        BigInt::from(128_i32),
        BigInt::from(1_i32) << 160,
    ] {
        assert_round_trip(value);
    }
    assert_round_trip((BigUint::from(1_u8) << 192) + BigUint::from(7_u8));
}

#[cfg(feature = "url")]
#[test]
fn urls_round_trip_with_their_canonical_representation() {
    assert_round_trip(
        url::Url::parse("https://example.com:8443/path?q=catga#memorypack").expect("URL is valid"),
    );
}
