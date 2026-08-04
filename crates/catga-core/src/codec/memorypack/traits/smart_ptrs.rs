use super::super::error::MemoryPackError;
use super::super::reader::MemoryPackReader;
use super::super::traits::{MemoryPackDeserialize, MemoryPackSerialize};
use super::super::writer::MemoryPackWriter;

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
