//! Tests for MemoryPack extended type traits

use catga_core::{MemoryPackDeserialize, MemoryPackSerialize, MemoryPackSerializer};

fn round_trip<T>(value: &T) -> T
where
    T: MemoryPackSerialize + MemoryPackDeserialize,
{
    let bytes = MemoryPackSerializer::serialize(value).expect("encode extended value");
    MemoryPackSerializer::deserialize(&bytes).expect("decode extended value")
}

#[cfg(feature = "uuid")]
#[test]
fn uuid_round_trips_and_rejects_short_frames() {
    let value = uuid::Uuid::from_u128(0x1234);
    assert_eq!(round_trip(&value), value);
    assert!(MemoryPackSerializer::deserialize::<uuid::Uuid>(&[0; 15]).is_err());
}

#[cfg(feature = "rust_decimal")]
#[test]
fn decimal_values_round_trip_sign_and_scale() {
    for value in [
        rust_decimal::Decimal::new(12345, 2),
        rust_decimal::Decimal::new(-987, 1),
        rust_decimal::Decimal::ZERO,
    ] {
        assert_eq!(round_trip(&value), value);
    }
}

#[cfg(feature = "half")]
#[test]
fn half_precision_values_round_trip_special_values() {
    for value in [
        half::f16::from_f32(1.5),
        half::f16::NEG_INFINITY,
        half::f16::NAN,
    ] {
        let decoded = round_trip(&value);
        assert_eq!(decoded.to_bits(), value.to_bits());
    }
}

#[cfg(feature = "num-bigint")]
#[test]
fn big_integer_values_round_trip_and_reject_negative_lengths() {
    for value in [
        num_bigint::BigInt::from(-123_456_i64),
        num_bigint::BigInt::from(0_i64),
        num_bigint::BigInt::from(987_654_u64),
    ] {
        assert_eq!(round_trip(&value), value);
    }
    for value in [
        num_bigint::BigUint::from(0_u64),
        num_bigint::BigUint::from(987_654_u64),
    ] {
        assert_eq!(round_trip(&value), value);
    }
    assert!(
        MemoryPackSerializer::deserialize::<num_bigint::BigInt>(&(-1_i32).to_le_bytes()).is_err()
    );
    assert!(
        MemoryPackSerializer::deserialize::<num_bigint::BigUint>(&(-1_i32).to_le_bytes()).is_err()
    );
}

#[cfg(feature = "url")]
#[test]
fn urls_round_trip_and_invalid_text_is_rejected() {
    let value = url::Url::parse("https://example.com/orders?id=7").expect("valid URL");
    assert_eq!(round_trip(&value), value);
    let encoded = MemoryPackSerializer::serialize(&String::from("not a URL"))
        .expect("encode invalid URL text");
    assert!(MemoryPackSerializer::deserialize::<url::Url>(&encoded).is_err());
}
