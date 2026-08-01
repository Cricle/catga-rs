use crate::error::MemoryPackError;
use crate::reader::MemoryPackReader;
use crate::traits::{MemoryPackDeserialize, MemoryPackSerialize};
use crate::writer::MemoryPackWriter;

#[cfg(feature = "chrono")]
use chrono::Timelike;

const TICKS_PER_SECOND: i64 = 10_000_000;
const TICKS_PER_NANOSECOND: i64 = 100;
const DOTNET_EPOCH_TICKS: i64 = 621355968000000000;
const TICKS_MASK: i64 = 0x3FFFFFFFFFFFFFFF;
const UTC_KIND_FLAG: i64 = 1i64 << 62;

#[cfg(feature = "chrono")]
impl MemoryPackSerialize for chrono::TimeDelta {
    #[inline(always)]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        let ticks = self
            .num_nanoseconds()
            .ok_or_else(|| MemoryPackError::SerializationError("Duration out of range".into()))?
            / TICKS_PER_NANOSECOND;
        writer.write_i64(ticks)
    }
}

#[cfg(feature = "chrono")]
impl MemoryPackDeserialize for chrono::TimeDelta {
    #[inline(always)]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        let ticks = reader.read_i64()?;
        Ok(chrono::TimeDelta::nanoseconds(ticks * TICKS_PER_NANOSECOND))
    }
}

#[cfg(feature = "chrono")]
impl MemoryPackSerialize for chrono::DateTime<chrono::Utc> {
    #[inline(always)]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        let unix_nanos = self
            .timestamp_nanos_opt()
            .ok_or_else(|| MemoryPackError::SerializationError("DateTime out of range".into()))?;
        let ticks = (unix_nanos / TICKS_PER_NANOSECOND) + DOTNET_EPOCH_TICKS;
        writer.write_i64(ticks | UTC_KIND_FLAG)
    }
}

#[cfg(feature = "chrono")]
impl MemoryPackDeserialize for chrono::DateTime<chrono::Utc> {
    #[inline(always)]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        let ticks_with_kind = reader.read_i64()?;
        let ticks = ticks_with_kind & TICKS_MASK;
        let unix_nanos = (ticks - DOTNET_EPOCH_TICKS).saturating_mul(TICKS_PER_NANOSECOND);
        Ok(chrono::DateTime::from_timestamp_nanos(unix_nanos))
    }
}

#[cfg(feature = "chrono")]
impl MemoryPackSerialize for chrono::DateTime<chrono::Local> {
    #[inline(always)]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        self.with_timezone(&chrono::Utc).serialize(writer)
    }
}

#[cfg(feature = "chrono")]
impl MemoryPackDeserialize for chrono::DateTime<chrono::Local> {
    #[inline(always)]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        let utc = chrono::DateTime::<chrono::Utc>::deserialize(reader)?;
        Ok(utc.with_timezone(&chrono::Local))
    }
}

#[cfg(feature = "chrono")]
impl MemoryPackSerialize for chrono::DateTime<chrono::FixedOffset> {
    #[inline(always)]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        let offset_minutes = (self.offset().local_minus_utc() / 60) as i16;
        let utc = self.with_timezone(&chrono::Utc);
        let unix_nanos = utc
            .timestamp_nanos_opt()
            .ok_or_else(|| MemoryPackError::SerializationError("DateTime out of range".into()))?;
        let ticks = (unix_nanos / TICKS_PER_NANOSECOND) + DOTNET_EPOCH_TICKS;

        writer.write_i16(offset_minutes)?;
        writer
            .buffer
            .extend_from_slice(&[0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00]);
        writer.write_i64(ticks)
    }
}

#[cfg(feature = "chrono")]
impl MemoryPackDeserialize for chrono::DateTime<chrono::FixedOffset> {
    #[inline(always)]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        let offset_minutes = reader.read_i16()?;
        reader.read_fixed_bytes::<6>()?;
        let ticks = reader.read_i64()?;

        let unix_nanos = (ticks - DOTNET_EPOCH_TICKS).saturating_mul(TICKS_PER_NANOSECOND);
        let offset_seconds = (offset_minutes as i32) * 60;

        let utc = chrono::DateTime::from_timestamp_nanos(unix_nanos);
        let offset = chrono::FixedOffset::east_opt(offset_seconds)
            .ok_or_else(|| MemoryPackError::DeserializationError("Invalid offset".into()))?;

        Ok(utc.with_timezone(&offset))
    }
}

#[cfg(feature = "chrono")]
impl MemoryPackSerialize for chrono::NaiveTime {
    #[inline(always)]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        let ticks = (self.num_seconds_from_midnight() as i64 * TICKS_PER_SECOND)
            + (self.nanosecond() as i64 / TICKS_PER_NANOSECOND);
        writer.write_i64(ticks)
    }
}

#[cfg(feature = "chrono")]
impl MemoryPackDeserialize for chrono::NaiveTime {
    #[inline(always)]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        let ticks = reader.read_i64()?;
        let total_nanos = ticks
            .checked_mul(TICKS_PER_NANOSECOND)
            .ok_or_else(|| MemoryPackError::DeserializationError("Invalid time ticks".into()))?;
        let secs = (total_nanos / 1_000_000_000) as u32;
        let nanos = (total_nanos % 1_000_000_000) as u32;

        chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
            .ok_or_else(|| MemoryPackError::DeserializationError("Invalid time ticks".into()))
    }
}

#[cfg(feature = "chrono")]
impl MemoryPackSerialize for chrono::NaiveDate {
    #[inline(always)]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        let epoch = chrono::NaiveDate::from_ymd_opt(1, 1, 1).ok_or_else(|| {
            MemoryPackError::SerializationError("MemoryPack date epoch is invalid".into())
        })?;
        let days = self.signed_duration_since(epoch).num_days() as i32;
        writer.write_i32(days)
    }
}

#[cfg(feature = "chrono")]
impl MemoryPackDeserialize for chrono::NaiveDate {
    #[inline(always)]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        let days = reader.read_i32()?;
        chrono::NaiveDate::from_ymd_opt(1, 1, 1)
            .and_then(|base| base.checked_add_days(chrono::Days::new(days as u64)))
            .ok_or_else(|| MemoryPackError::DeserializationError("Invalid date".into()))
    }
}

#[cfg(all(test, feature = "chrono"))]
mod tests {
    use super::*;
    use crate::MemoryPackSerializer;
    use chrono::{Datelike, NaiveDate, NaiveTime, TimeDelta, Timelike, Utc};

    fn round_trip<T>(value: &T) -> T
    where
        T: MemoryPackSerialize + MemoryPackDeserialize,
    {
        let bytes = MemoryPackSerializer::serialize(value).expect("encode datetime value");
        MemoryPackSerializer::deserialize(&bytes).expect("decode datetime value")
    }

    #[test]
    fn chrono_values_round_trip_across_time_zones_and_date_shapes() {
        let delta = TimeDelta::new(12, 345_678_900).expect("valid duration");
        assert_eq!(round_trip(&delta), delta);

        let utc = chrono::DateTime::<Utc>::from_timestamp(1_700_000_000, 123_456_700)
            .expect("valid UTC timestamp");
        assert_eq!(round_trip(&utc), utc);
        let local = utc.with_timezone(&chrono::Local);
        assert_eq!(round_trip(&local), local);
        let offset = chrono::FixedOffset::east_opt(5 * 3600 + 30 * 60).expect("valid offset");
        let fixed = utc.with_timezone(&offset);
        assert_eq!(round_trip(&fixed), fixed);

        let time = NaiveTime::from_hms_nano_opt(12, 34, 56, 123_456_700).expect("valid time");
        assert_eq!(round_trip(&time), time);
        assert_eq!(time.hour(), 12);
        assert_eq!(time.minute(), 34);
        let date = NaiveDate::from_ymd_opt(2026, 8, 1).expect("valid date");
        assert_eq!(round_trip(&date), date);
        assert_eq!(date.year(), 2026);
        assert_eq!(date.month(), 8);
    }

    #[test]
    fn chrono_decoders_reject_truncated_or_invalid_wire_values() {
        assert!(MemoryPackSerializer::deserialize::<TimeDelta>(&[]).is_err());
        assert!(MemoryPackSerializer::deserialize::<chrono::DateTime<Utc>>(&[0; 4]).is_err());
        assert!(MemoryPackSerializer::deserialize::<NaiveTime>(&i64::MAX.to_le_bytes()).is_err());
        assert!(MemoryPackSerializer::deserialize::<NaiveDate>(&i32::MAX.to_le_bytes()).is_err());
        let mut invalid_offset = Vec::new();
        invalid_offset.extend_from_slice(&i16::MAX.to_le_bytes());
        invalid_offset.extend_from_slice(&[0; 6]);
        invalid_offset.extend_from_slice(&0_i64.to_le_bytes());
        assert!(
            MemoryPackSerializer::deserialize::<chrono::DateTime<chrono::FixedOffset>>(
                &invalid_offset
            )
            .is_err()
        );
    }
}
