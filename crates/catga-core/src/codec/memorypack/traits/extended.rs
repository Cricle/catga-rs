use super::super::error::MemoryPackError;
use super::super::reader::MemoryPackReader;
use super::super::traits::{MemoryPackDeserialize, MemoryPackSerialize};
use super::super::writer::MemoryPackWriter;

#[cfg(feature = "uuid")]
impl MemoryPackSerialize for uuid::Uuid {
    #[inline(always)]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        writer.buffer.extend_from_slice(self.as_bytes());
        Ok(())
    }
}

#[cfg(feature = "uuid")]
impl MemoryPackDeserialize for uuid::Uuid {
    #[inline(always)]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        Ok(uuid::Uuid::from_bytes(reader.read_fixed_bytes::<16>()?))
    }
}

#[cfg(feature = "rust_decimal")]
impl MemoryPackSerialize for rust_decimal::Decimal {
    #[inline(always)]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        let unpacked = self.unpack();

        let flags: u32 = ((unpacked.negative as u32) << 31) | (unpacked.scale << 16);
        let lo64: u64 = (unpacked.lo as u64) | ((unpacked.mid as u64) << 32);

        writer.write_u32(flags)?;
        writer.write_u32(unpacked.hi)?;
        writer.write_u64(lo64)
    }
}

#[cfg(feature = "rust_decimal")]
impl MemoryPackDeserialize for rust_decimal::Decimal {
    #[inline(always)]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        let flags = reader.read_u32()?;
        let hi = reader.read_u32()?;
        let lo64 = reader.read_u64()?;

        let negative = (flags & 0x8000_0000) != 0;
        let scale = (flags >> 16) & 0xFF;
        let lo = lo64 as u32;
        let mid = (lo64 >> 32) as u32;

        Ok(rust_decimal::Decimal::from_parts(
            lo, mid, hi, negative, scale,
        ))
    }
}

#[cfg(feature = "half")]
impl MemoryPackSerialize for half::f16 {
    #[inline(always)]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        writer.write_u16(self.to_bits())
    }
}

#[cfg(feature = "half")]
impl MemoryPackDeserialize for half::f16 {
    #[inline(always)]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        Ok(half::f16::from_bits(reader.read_u16()?))
    }
}

#[cfg(feature = "num-bigint")]
impl MemoryPackSerialize for num_bigint::BigInt {
    #[inline]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        let bytes = self.to_signed_bytes_le();

        writer.write_i32(MemoryPackWriter::checked_i32_length(bytes.len())?)?;
        writer.buffer.extend_from_slice(&bytes);
        Ok(())
    }
}

#[cfg(feature = "num-bigint")]
impl MemoryPackDeserialize for num_bigint::BigInt {
    #[inline]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        let len = reader.read_i32()?;
        if len < 0 {
            return Err(MemoryPackError::DeserializationError(
                "Negative length in BigInteger".into(),
            ));
        }

        Ok(num_bigint::BigInt::from_signed_bytes_le(
            &reader.read_bytes_vec(len as usize)?,
        ))
    }
}

#[cfg(feature = "num-bigint")]
impl MemoryPackSerialize for num_bigint::BigUint {
    #[inline]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        let bytes = self.to_bytes_le();
        writer.write_i32(MemoryPackWriter::checked_i32_length(bytes.len())?)?;
        writer.buffer.extend_from_slice(&bytes);
        Ok(())
    }
}

#[cfg(feature = "num-bigint")]
impl MemoryPackDeserialize for num_bigint::BigUint {
    #[inline]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        let len = reader.read_i32()?;
        if len < 0 {
            return Err(MemoryPackError::DeserializationError(
                "Negative length in BigUint".into(),
            ));
        }

        Ok(num_bigint::BigUint::from_bytes_le(
            &reader.read_bytes_vec(len as usize)?,
        ))
    }
}

#[cfg(feature = "url")]
impl MemoryPackSerialize for url::Url {
    #[inline(always)]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        self.as_str().serialize(writer)
    }
}

#[cfg(feature = "url")]
impl MemoryPackDeserialize for url::Url {
    #[inline(always)]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        let s = String::deserialize(reader)?;
        url::Url::parse(&s).map_err(|e| MemoryPackError::DeserializationError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryPackSerializer;

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
            MemoryPackSerializer::deserialize::<num_bigint::BigInt>(&(-1_i32).to_le_bytes())
                .is_err()
        );
        assert!(
            MemoryPackSerializer::deserialize::<num_bigint::BigUint>(&(-1_i32).to_le_bytes())
                .is_err()
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
}
