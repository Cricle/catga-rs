use crate::error::MemoryPackError;
use crate::reader::MemoryPackReader;
use crate::traits::{MemoryPackDeserialize, MemoryPackSerialize};
use crate::writer::MemoryPackWriter;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, LinkedList, VecDeque};
use std::mem::size_of;

#[cfg(feature = "hashbrown")]
use hashbrown::HashMap as HashbrownHashMap;
#[cfg(feature = "hashbrown")]
use hashbrown::HashSet as HashbrownHashSet;

#[cfg(feature = "ahash")]
use ahash::{AHashMap, AHashSet};

#[inline(always)]
fn validate_size(
    reader: &mut MemoryPackReader,
    size: i32,
) -> Result<Option<usize>, MemoryPackError> {
    reader.validate_collection_len(size)
}

#[inline(always)]
fn reserve_collection<T>(
    reader: &mut MemoryPackReader,
    capacity: usize,
) -> Result<(), MemoryPackError> {
    let bytes = capacity
        .checked_mul(size_of::<T>())
        .ok_or(MemoryPackError::LimitExceeded {
            resource: "cumulative allocation bytes",
            limit: usize::MAX,
        })?;
    reader.reserve_allocation(bytes)
}

#[inline(always)]
fn write_collection_header(
    writer: &mut MemoryPackWriter,
    len: usize,
) -> Result<(), MemoryPackError> {
    writer.write_i32(MemoryPackWriter::checked_i32_length(len)?)
}

impl<T: MemoryPackSerialize> MemoryPackSerialize for Vec<T> {
    #[inline(always)]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        write_collection_header(writer, self.len())?;
        for item in self.iter() {
            item.serialize(writer)?;
        }
        Ok(())
    }
}

impl<T: MemoryPackDeserialize> MemoryPackDeserialize for Vec<T> {
    #[inline(always)]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        let size = reader.read_i32()?;
        match validate_size(reader, size)? {
            None => Ok(Vec::new()),
            Some(capacity) => {
                reserve_collection::<T>(reader, capacity)?;
                let mut result = Vec::with_capacity(capacity);
                for _ in 0..capacity {
                    result.push(T::deserialize(reader)?);
                }
                Ok(result)
            }
        }
    }
}

impl<T: MemoryPackSerialize, const N: usize> MemoryPackSerialize for [T; N] {
    #[inline(always)]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        let length = i32::try_from(N).map_err(|_| {
            MemoryPackError::SerializationError("fixed array length exceeds i32::MAX".into())
        })?;
        writer.write_i32(length)?;
        for item in self {
            item.serialize(writer)?;
        }
        Ok(())
    }
}

impl<T: MemoryPackDeserialize, const N: usize> MemoryPackDeserialize for [T; N] {
    #[inline(always)]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        let length = reader.read_i32()?;
        let Some(length) = validate_size(reader, length)? else {
            return Err(MemoryPackError::DeserializationError(
                "fixed array wire length does not match the target length".into(),
            ));
        };
        if length != N {
            return Err(MemoryPackError::DeserializationError(
                "fixed array wire length does not match the target length".into(),
            ));
        }

        reserve_collection::<T>(reader, N)?;
        let mut values = Vec::with_capacity(N);
        for _ in 0..N {
            values.push(T::deserialize(reader)?);
        }
        values.try_into().map_err(|_| {
            MemoryPackError::DeserializationError(
                "fixed array wire length does not match the target length".into(),
            )
        })
    }
}

impl<T: MemoryPackSerialize> MemoryPackSerialize for VecDeque<T> {
    #[inline(always)]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        write_collection_header(writer, self.len())?;
        for item in self.iter() {
            item.serialize(writer)?;
        }
        Ok(())
    }
}

impl<T: MemoryPackDeserialize> MemoryPackDeserialize for VecDeque<T> {
    #[inline(always)]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        let size = reader.read_i32()?;
        match validate_size(reader, size)? {
            None => Ok(VecDeque::new()),
            Some(capacity) => {
                reserve_collection::<T>(reader, capacity)?;
                let mut result = VecDeque::with_capacity(capacity);
                for _ in 0..capacity {
                    result.push_back(T::deserialize(reader)?);
                }
                Ok(result)
            }
        }
    }
}

impl<T: MemoryPackSerialize> MemoryPackSerialize for LinkedList<T> {
    #[inline(always)]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        write_collection_header(writer, self.len())?;
        for item in self.iter() {
            item.serialize(writer)?;
        }
        Ok(())
    }
}

impl<T: MemoryPackDeserialize> MemoryPackDeserialize for LinkedList<T> {
    #[inline(always)]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        let size = reader.read_i32()?;
        match validate_size(reader, size)? {
            None => Ok(LinkedList::new()),
            Some(capacity) => {
                let mut result = LinkedList::new();
                for _ in 0..capacity {
                    result.push_back(T::deserialize(reader)?);
                }
                Ok(result)
            }
        }
    }
}

impl<T: MemoryPackSerialize + Eq + std::hash::Hash> MemoryPackSerialize for HashSet<T> {
    #[inline(always)]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        write_collection_header(writer, self.len())?;
        for item in self.iter() {
            item.serialize(writer)?;
        }
        Ok(())
    }
}

impl<T: MemoryPackDeserialize + Eq + std::hash::Hash> MemoryPackDeserialize for HashSet<T> {
    #[inline(always)]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        let size = reader.read_i32()?;
        match validate_size(reader, size)? {
            None => Ok(HashSet::new()),
            Some(capacity) => {
                reserve_collection::<T>(reader, capacity)?;
                let mut result = HashSet::with_capacity(capacity);
                for _ in 0..capacity {
                    result.insert(T::deserialize(reader)?);
                }
                Ok(result)
            }
        }
    }
}

impl<T: MemoryPackSerialize + Ord> MemoryPackSerialize for BTreeSet<T> {
    #[inline(always)]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        write_collection_header(writer, self.len())?;
        for item in self.iter() {
            item.serialize(writer)?;
        }
        Ok(())
    }
}

impl<T: MemoryPackDeserialize + Ord> MemoryPackDeserialize for BTreeSet<T> {
    #[inline(always)]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        let size = reader.read_i32()?;
        match validate_size(reader, size)? {
            None => Ok(BTreeSet::new()),
            Some(capacity) => {
                let mut result = BTreeSet::new();
                for _ in 0..capacity {
                    result.insert(T::deserialize(reader)?);
                }
                Ok(result)
            }
        }
    }
}

macro_rules! impl_std_hashmap {
    ($key_type:ty) => {
        impl<V: MemoryPackDeserialize + Default> MemoryPackDeserialize for HashMap<$key_type, V> {
            #[inline(always)]
            fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
                let count = reader.read_i32()?;
                match validate_size(reader, count)? {
                    None => Ok(HashMap::new()),
                    Some(capacity) => {
                        reserve_collection::<($key_type, V)>(reader, capacity)?;
                        let mut map = HashMap::with_capacity(capacity);
                        for _ in 0..capacity {
                            map.insert(<$key_type>::deserialize(reader)?, V::deserialize(reader)?);
                        }
                        Ok(map)
                    }
                }
            }
        }

        impl<V: MemoryPackSerialize> MemoryPackSerialize for HashMap<$key_type, V> {
            #[inline(always)]
            fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
                write_collection_header(writer, self.len())?;
                for (key, value) in self.iter() {
                    key.serialize(writer)?;
                    value.serialize(writer)?;
                }
                Ok(())
            }
        }
    };
}

impl_std_hashmap!(String);
impl_std_hashmap!(i8);
impl_std_hashmap!(u8);
impl_std_hashmap!(i16);
impl_std_hashmap!(u16);
impl_std_hashmap!(i32);
impl_std_hashmap!(u32);
impl_std_hashmap!(i64);
impl_std_hashmap!(u64);
impl_std_hashmap!(i128);
impl_std_hashmap!(u128);
impl_std_hashmap!(char);

macro_rules! impl_btreemap {
    ($key_type:ty) => {
        impl<V: MemoryPackDeserialize + Default> MemoryPackDeserialize for BTreeMap<$key_type, V> {
            #[inline(always)]
            fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
                let count = reader.read_i32()?;
                match validate_size(reader, count)? {
                    None => Ok(BTreeMap::new()),
                    Some(capacity) => {
                        reserve_collection::<($key_type, V)>(reader, capacity)?;
                        let mut map = BTreeMap::new();
                        for _ in 0..capacity {
                            map.insert(<$key_type>::deserialize(reader)?, V::deserialize(reader)?);
                        }
                        Ok(map)
                    }
                }
            }
        }

        impl<V: MemoryPackSerialize> MemoryPackSerialize for BTreeMap<$key_type, V> {
            #[inline(always)]
            fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
                write_collection_header(writer, self.len())?;
                for (key, value) in self.iter() {
                    key.serialize(writer)?;
                    value.serialize(writer)?;
                }
                Ok(())
            }
        }
    };
}

impl_btreemap!(String);
impl_btreemap!(i8);
impl_btreemap!(u8);
impl_btreemap!(i16);
impl_btreemap!(u16);
impl_btreemap!(i32);
impl_btreemap!(u32);
impl_btreemap!(i64);
impl_btreemap!(u64);
impl_btreemap!(i128);
impl_btreemap!(u128);
impl_btreemap!(char);

#[cfg(feature = "hashbrown")]
impl<T: MemoryPackSerialize + Eq + std::hash::Hash> MemoryPackSerialize for HashbrownHashSet<T> {
    #[inline(always)]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        write_collection_header(writer, self.len())?;
        for item in self.iter() {
            item.serialize(writer)?;
        }
        Ok(())
    }
}

#[cfg(feature = "hashbrown")]
impl<T: MemoryPackDeserialize + Eq + std::hash::Hash> MemoryPackDeserialize
    for HashbrownHashSet<T>
{
    #[inline(always)]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        let size = reader.read_i32()?;
        match validate_size(reader, size)? {
            None => Ok(HashbrownHashSet::new()),
            Some(capacity) => {
                reserve_collection::<T>(reader, capacity)?;
                let mut result = HashbrownHashSet::with_capacity(capacity);
                for _ in 0..capacity {
                    result.insert(T::deserialize(reader)?);
                }
                Ok(result)
            }
        }
    }
}

#[cfg(feature = "hashbrown")]
macro_rules! impl_hashbrown_hashmap {
    ($key_type:ty) => {
        impl<V: MemoryPackDeserialize + Default> MemoryPackDeserialize
            for HashbrownHashMap<$key_type, V>
        {
            #[inline(always)]
            fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
                let count = reader.read_i32()?;
                match validate_size(reader, count)? {
                    None => Ok(HashbrownHashMap::new()),
                    Some(capacity) => {
                        reserve_collection::<($key_type, V)>(reader, capacity)?;
                        let mut map = HashbrownHashMap::with_capacity(capacity);
                        for _ in 0..capacity {
                            map.insert(<$key_type>::deserialize(reader)?, V::deserialize(reader)?);
                        }
                        Ok(map)
                    }
                }
            }
        }

        impl<V: MemoryPackSerialize> MemoryPackSerialize for HashbrownHashMap<$key_type, V> {
            #[inline(always)]
            fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
                write_collection_header(writer, self.len())?;
                for (key, value) in self.iter() {
                    key.serialize(writer)?;
                    value.serialize(writer)?;
                }
                Ok(())
            }
        }
    };
}

#[cfg(feature = "hashbrown")]
impl_hashbrown_hashmap!(String);
#[cfg(feature = "hashbrown")]
impl_hashbrown_hashmap!(i8);
#[cfg(feature = "hashbrown")]
impl_hashbrown_hashmap!(u8);
#[cfg(feature = "hashbrown")]
impl_hashbrown_hashmap!(i16);
#[cfg(feature = "hashbrown")]
impl_hashbrown_hashmap!(u16);
#[cfg(feature = "hashbrown")]
impl_hashbrown_hashmap!(i32);
#[cfg(feature = "hashbrown")]
impl_hashbrown_hashmap!(u32);
#[cfg(feature = "hashbrown")]
impl_hashbrown_hashmap!(i64);
#[cfg(feature = "hashbrown")]
impl_hashbrown_hashmap!(u64);
#[cfg(feature = "hashbrown")]
impl_hashbrown_hashmap!(i128);
#[cfg(feature = "hashbrown")]
impl_hashbrown_hashmap!(u128);
#[cfg(feature = "hashbrown")]
impl_hashbrown_hashmap!(char);

#[cfg(feature = "ahash")]
macro_rules! impl_ahash_hashmap {
    ($key_type:ty) => {
        impl<V: MemoryPackDeserialize + Default> MemoryPackDeserialize for AHashMap<$key_type, V> {
            #[inline(always)]
            fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
                let count = reader.read_i32()?;
                match validate_size(reader, count)? {
                    None => Ok(AHashMap::new()),
                    Some(capacity) => {
                        reserve_collection::<($key_type, V)>(reader, capacity)?;
                        let mut map = AHashMap::with_capacity(capacity);
                        for _ in 0..capacity {
                            map.insert(<$key_type>::deserialize(reader)?, V::deserialize(reader)?);
                        }
                        Ok(map)
                    }
                }
            }
        }

        impl<V: MemoryPackSerialize> MemoryPackSerialize for AHashMap<$key_type, V> {
            #[inline(always)]
            fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
                write_collection_header(writer, self.len())?;
                for (key, value) in self.iter() {
                    key.serialize(writer)?;
                    value.serialize(writer)?;
                }
                Ok(())
            }
        }
    };
}

#[cfg(feature = "ahash")]
impl_ahash_hashmap!(String);
#[cfg(feature = "ahash")]
impl_ahash_hashmap!(i8);
#[cfg(feature = "ahash")]
impl_ahash_hashmap!(u8);
#[cfg(feature = "ahash")]
impl_ahash_hashmap!(i16);
#[cfg(feature = "ahash")]
impl_ahash_hashmap!(u16);
#[cfg(feature = "ahash")]
impl_ahash_hashmap!(i32);
#[cfg(feature = "ahash")]
impl_ahash_hashmap!(u32);
#[cfg(feature = "ahash")]
impl_ahash_hashmap!(i64);
#[cfg(feature = "ahash")]
impl_ahash_hashmap!(u64);
#[cfg(feature = "ahash")]
impl_ahash_hashmap!(i128);
#[cfg(feature = "ahash")]
impl_ahash_hashmap!(u128);
#[cfg(feature = "ahash")]
impl_ahash_hashmap!(char);

#[cfg(feature = "ahash")]
impl<T: MemoryPackSerialize + Eq + std::hash::Hash> MemoryPackSerialize for AHashSet<T> {
    #[inline(always)]
    fn serialize(&self, writer: &mut MemoryPackWriter) -> Result<(), MemoryPackError> {
        write_collection_header(writer, self.len())?;
        for item in self.iter() {
            item.serialize(writer)?;
        }
        Ok(())
    }
}

#[cfg(feature = "ahash")]
impl<T: MemoryPackDeserialize + Eq + std::hash::Hash> MemoryPackDeserialize for AHashSet<T> {
    #[inline(always)]
    fn deserialize(reader: &mut MemoryPackReader) -> Result<Self, MemoryPackError> {
        let size = reader.read_i32()?;
        match validate_size(reader, size)? {
            None => Ok(AHashSet::new()),
            Some(capacity) => {
                reserve_collection::<T>(reader, capacity)?;
                let mut result = AHashSet::with_capacity(capacity);
                for _ in 0..capacity {
                    result.insert(T::deserialize(reader)?);
                }
                Ok(result)
            }
        }
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
        let bytes = MemoryPackSerializer::serialize(value).expect("encode collection");
        MemoryPackSerializer::deserialize(&bytes).expect("decode collection")
    }

    #[test]
    fn standard_collections_round_trip_empty_and_populated_values() {
        assert_eq!(round_trip(&vec![1_i32, 2, 3]), vec![1, 2, 3]);
        assert_eq!(round_trip(&Vec::<i32>::new()), Vec::<i32>::new());
        assert_eq!(
            round_trip(&VecDeque::from([1_i32, 2])),
            VecDeque::from([1, 2])
        );
        assert_eq!(
            round_trip(&LinkedList::from([1_i32, 2])),
            LinkedList::from([1, 2])
        );
        assert_eq!(round_trip(&[1_i32, 2, 3]), [1, 2, 3]);
        assert_eq!(
            round_trip(&HashSet::from([1_i32, 2])),
            HashSet::from([1, 2])
        );
        assert_eq!(
            round_trip(&BTreeSet::from([1_i32, 2])),
            BTreeSet::from([1, 2])
        );

        macro_rules! map_round_trip {
            ($key:ty, $value:expr) => {{
                let mut hash = HashMap::<$key, i32>::new();
                hash.insert($value, 9);
                assert_eq!(round_trip(&hash).len(), 1);
                let mut tree = BTreeMap::<$key, i32>::new();
                tree.insert($value, 9);
                assert_eq!(round_trip(&tree).len(), 1);
            }};
        }
        map_round_trip!(String, String::from("key"));
        map_round_trip!(i8, 1_i8);
        map_round_trip!(u8, 2_u8);
        map_round_trip!(i16, 3_i16);
        map_round_trip!(u16, 4_u16);
        map_round_trip!(i32, 5_i32);
        map_round_trip!(u32, 6_u32);
        map_round_trip!(i64, 7_i64);
        map_round_trip!(u64, 8_u64);
        map_round_trip!(i128, 9_i128);
        map_round_trip!(u128, 10_u128);
        map_round_trip!(char, 'x');

        #[cfg(feature = "hashbrown")]
        {
            let mut set = HashbrownHashSet::new();
            set.insert(1_i32);
            assert_eq!(round_trip(&set).len(), 1);
            let mut map = HashbrownHashMap::new();
            map.insert(1_i32, 2_i32);
            assert_eq!(round_trip(&map).len(), 1);
        }
        #[cfg(feature = "ahash")]
        {
            let mut set = AHashSet::new();
            set.insert(1_i32);
            assert_eq!(round_trip(&set).len(), 1);
            let mut map = AHashMap::new();
            map.insert(1_i32, 2_i32);
            assert_eq!(round_trip(&map).len(), 1);
        }
    }

    #[test]
    fn collection_decoders_reject_negative_and_mismatched_lengths() {
        assert!(MemoryPackSerializer::deserialize::<Vec<i32>>(&(-2_i32).to_le_bytes()).is_err());
        assert!(MemoryPackSerializer::deserialize::<[i32; 2]>(&1_i32.to_le_bytes()).is_err());
        assert!(MemoryPackSerializer::deserialize::<[i32; 2]>(&(-1_i32).to_le_bytes()).is_err());
    }
}
