use crate::error::MemoryPackError;
use crate::reader::MemoryPackReader;
use crate::traits::{MemoryPackDeserialize, MemoryPackSerialize};
use crate::writer::MemoryPackWriter;

use std::rc::Rc;
use std::sync::Arc;

impl<T: MemoryPackSerialize> MemoryPackSerialize for Box<T> {
    #[inline]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        (**self).serialize(writer)
    }
}

impl<T: MemoryPackDeserialize> MemoryPackDeserialize for Box<T> {
    #[inline]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        Ok(Box::new(T::deserialize(reader)?))
    }
}

impl MemoryPackSerialize for Box<str> {
    #[inline]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        writer.write_string(self)
    }
}

impl MemoryPackDeserialize for Box<str> {
    #[inline]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        Ok(reader.read_string()?.into_boxed_str())
    }
}

impl<T: MemoryPackSerialize> MemoryPackSerialize for Rc<T> {
    #[inline]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        (**self).serialize(writer)
    }
}

impl<T: MemoryPackDeserialize> MemoryPackDeserialize for Rc<T> {
    #[inline]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        Ok(Rc::new(T::deserialize(reader)?))
    }
}

impl<T: MemoryPackSerialize> MemoryPackSerialize for Arc<T> {
    #[inline]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        (**self).serialize(writer)
    }
}

impl<T: MemoryPackDeserialize> MemoryPackDeserialize for Arc<T> {
    #[inline]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        Ok(Arc::new(T::deserialize(reader)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryPackSerializer;
    use std::sync::Arc;

    #[test]
    fn boxed_rc_arc_and_boxed_str_values_round_trip() {
        let boxed = Box::new(7_u32);
        let bytes = MemoryPackSerializer::serialize(&boxed).unwrap();
        assert_eq!(
            MemoryPackSerializer::deserialize::<Box<u32>>(&bytes).unwrap(),
            boxed
        );

        let rc = Rc::new(String::from("rc"));
        let bytes = MemoryPackSerializer::serialize(&rc).unwrap();
        assert_eq!(
            &*MemoryPackSerializer::deserialize::<Rc<String>>(&bytes).unwrap(),
            "rc"
        );

        let arc = Arc::new(String::from("arc"));
        let bytes = MemoryPackSerializer::serialize(&arc).unwrap();
        assert_eq!(
            &*MemoryPackSerializer::deserialize::<Arc<String>>(&bytes).unwrap(),
            "arc"
        );

        let boxed_str: Box<str> = "boxed".into();
        let bytes = MemoryPackSerializer::serialize(&boxed_str).unwrap();
        assert_eq!(
            &*MemoryPackSerializer::deserialize::<Box<str>>(&bytes).unwrap(),
            "boxed"
        );
    }
}
