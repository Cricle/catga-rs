//! Tests for MemoryPack math type traits

#[cfg(any(feature = "num-complex", feature = "glam"))]
use catga_core::MemoryPackSerializer;

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
