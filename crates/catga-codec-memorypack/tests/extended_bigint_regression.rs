//! Regression coverage for signed arbitrary-precision MemoryPack values.

#[cfg(feature = "num-bigint")]
use catga_codec_memorypack::MemoryPackSerializer;
#[cfg(feature = "num-bigint")]
use num_bigint::BigInt;

#[cfg(feature = "num-bigint")]
#[test]
fn bigint_round_trips_positive_values_whose_high_bit_is_set() {
    for value in [
        BigInt::from(128_i32),
        BigInt::from(255_i32),
        BigInt::from(32_768_i32),
    ] {
        let frame = MemoryPackSerializer::serialize(&value)
            .expect("a positive arbitrary-precision integer serializes");
        let decoded = MemoryPackSerializer::deserialize::<BigInt>(&frame)
            .expect("the serialized arbitrary-precision integer deserializes");

        assert_eq!(decoded, value, "the sign bit must not change the value");
    }
}
