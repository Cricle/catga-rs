use super::super::error::MemoryPackError;
use super::super::reader::MemoryPackReader;
use super::super::traits::{MemoryPackDeserialize, MemoryPackSerialize};
use super::super::writer::MemoryPackWriter;

#[cfg(feature = "num-complex")]
impl MemoryPackSerialize for num_complex::Complex<f64> {
    #[inline(always)]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        writer.write_f64(self.re)?;
        writer.write_f64(self.im)
    }
}

#[cfg(feature = "num-complex")]
impl MemoryPackDeserialize for num_complex::Complex<f64> {
    #[inline(always)]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        let re = reader.read_f64()?;
        let im = reader.read_f64()?;
        Ok(num_complex::Complex::new(re, im))
    }
}

#[cfg(feature = "glam")]
impl MemoryPackSerialize for glam::Vec2 {
    #[inline(always)]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        let arr = self.to_array();
        writer.write_f32(arr[0])?;
        writer.write_f32(arr[1])
    }
}

#[cfg(feature = "glam")]
impl MemoryPackDeserialize for glam::Vec2 {
    #[inline(always)]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        Ok(glam::Vec2::from_array([
            reader.read_f32()?,
            reader.read_f32()?,
        ]))
    }
}

#[cfg(feature = "glam")]
impl MemoryPackSerialize for glam::Vec3 {
    #[inline(always)]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        let arr = self.to_array();
        writer.write_f32(arr[0])?;
        writer.write_f32(arr[1])?;
        writer.write_f32(arr[2])
    }
}

#[cfg(feature = "glam")]
impl MemoryPackDeserialize for glam::Vec3 {
    #[inline(always)]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        Ok(glam::Vec3::from_array([
            reader.read_f32()?,
            reader.read_f32()?,
            reader.read_f32()?,
        ]))
    }
}

#[cfg(feature = "glam")]
impl MemoryPackSerialize for glam::Vec4 {
    #[inline(always)]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        let arr = self.to_array();
        writer.write_f32(arr[0])?;
        writer.write_f32(arr[1])?;
        writer.write_f32(arr[2])?;
        writer.write_f32(arr[3])
    }
}

#[cfg(feature = "glam")]
impl MemoryPackDeserialize for glam::Vec4 {
    #[inline(always)]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        Ok(glam::Vec4::from_array([
            reader.read_f32()?,
            reader.read_f32()?,
            reader.read_f32()?,
            reader.read_f32()?,
        ]))
    }
}

#[cfg(feature = "glam")]
impl MemoryPackSerialize for glam::Quat {
    #[inline(always)]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        let arr = self.to_array();
        writer.write_f32(arr[0])?;
        writer.write_f32(arr[1])?;
        writer.write_f32(arr[2])?;
        writer.write_f32(arr[3])
    }
}

#[cfg(feature = "glam")]
impl MemoryPackDeserialize for glam::Quat {
    #[inline(always)]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        Ok(glam::Quat::from_array([
            reader.read_f32()?,
            reader.read_f32()?,
            reader.read_f32()?,
            reader.read_f32()?,
        ]))
    }
}

#[cfg(feature = "glam")]
impl MemoryPackSerialize for glam::Mat3A {
    #[inline(always)]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        let cols = self.to_cols_array();
        writer.write_f32(cols[0])?;
        writer.write_f32(cols[1])?;
        writer.write_f32(cols[3])?;
        writer.write_f32(cols[4])?;
        writer.write_f32(cols[6])?;
        writer.write_f32(cols[7])
    }
}

#[cfg(feature = "glam")]
impl MemoryPackDeserialize for glam::Mat3A {
    #[inline(always)]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        let m11 = reader.read_f32()?;
        let m12 = reader.read_f32()?;
        let m21 = reader.read_f32()?;
        let m22 = reader.read_f32()?;
        let m31 = reader.read_f32()?;
        let m32 = reader.read_f32()?;
        Ok(glam::Mat3A::from_cols(
            glam::Vec3A::new(m11, m12, 0.0),
            glam::Vec3A::new(m21, m22, 0.0),
            glam::Vec3A::new(m31, m32, 1.0),
        ))
    }
}

#[cfg(feature = "glam")]
impl MemoryPackSerialize for glam::Mat4 {
    #[inline(always)]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        let arr = self.to_cols_array();
        for &val in &arr {
            writer.write_f32(val)?;
        }
        Ok(())
    }
}

#[cfg(feature = "glam")]
impl MemoryPackDeserialize for glam::Mat4 {
    #[inline(always)]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        let mut arr = [0.0f32; 16];
        for val in &mut arr {
            *val = reader.read_f32()?;
        }
        Ok(glam::Mat4::from_cols_array(&arr))
    }
}

#[cfg(test)]
mod tests {
    use crate::MemoryPackSerializer;

    #[cfg(feature = "num-complex")]
    #[test]
    fn complex_values_round_trip_through_the_wire() {
        let value = num_complex::Complex::new(1.25, -2.5);
        let encoded = MemoryPackSerializer::serialize(&value).expect("encode complex");
        assert_eq!(
            MemoryPackSerializer::deserialize::<num_complex::Complex<f64>>(&encoded)
                .expect("decode complex"),
            value
        );
    }

    #[cfg(feature = "glam")]
    #[test]
    fn glam_vectors_quaternions_and_matrices_round_trip() {
        for value in [glam::Vec2::new(1.0, 2.0), glam::Vec2::new(-3.5, 4.25)] {
            let encoded = MemoryPackSerializer::serialize(&value).expect("encode Vec2");
            assert_eq!(
                MemoryPackSerializer::deserialize::<glam::Vec2>(&encoded).expect("decode Vec2"),
                value
            );
        }
        for value in [glam::Vec3::new(1.0, 2.0, 3.0), glam::Vec3::splat(-4.0)] {
            let encoded = MemoryPackSerializer::serialize(&value).expect("encode Vec3");
            assert_eq!(
                MemoryPackSerializer::deserialize::<glam::Vec3>(&encoded).expect("decode Vec3"),
                value
            );
        }
        for value in [glam::Vec4::new(1.0, 2.0, 3.0, 4.0), glam::Vec4::ZERO] {
            let encoded = MemoryPackSerializer::serialize(&value).expect("encode Vec4");
            assert_eq!(
                MemoryPackSerializer::deserialize::<glam::Vec4>(&encoded).expect("decode Vec4"),
                value
            );
        }
        for value in [
            glam::Quat::from_xyzw(1.0, 2.0, 3.0, 4.0),
            glam::Quat::IDENTITY,
        ] {
            let encoded = MemoryPackSerializer::serialize(&value).expect("encode Quat");
            assert_eq!(
                MemoryPackSerializer::deserialize::<glam::Quat>(&encoded).expect("decode Quat"),
                value
            );
        }

        let mat3 = glam::Mat3A::from_cols(
            glam::Vec3A::new(1.0, 2.0, 0.0),
            glam::Vec3A::new(3.0, 4.0, 0.0),
            glam::Vec3A::new(5.0, 6.0, 1.0),
        );
        let encoded = MemoryPackSerializer::serialize(&mat3).expect("encode Mat3A");
        assert_eq!(
            MemoryPackSerializer::deserialize::<glam::Mat3A>(&encoded).expect("decode Mat3A"),
            mat3
        );

        let mat4 = glam::Mat4::from_cols_array(&[
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0,
        ]);
        let encoded = MemoryPackSerializer::serialize(&mat4).expect("encode Mat4");
        assert_eq!(
            MemoryPackSerializer::deserialize::<glam::Mat4>(&encoded).expect("decode Mat4"),
            mat4
        );
    }

    #[cfg(feature = "glam")]
    #[test]
    fn math_decoders_report_truncated_frames() {
        assert!(MemoryPackSerializer::deserialize::<glam::Vec4>(&[0; 4]).is_err());
        assert!(MemoryPackSerializer::deserialize::<glam::Mat4>(&[0; 15 * 4]).is_err());
    }
}
