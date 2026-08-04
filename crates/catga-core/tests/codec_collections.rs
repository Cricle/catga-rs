//! Tests for MemoryPack collection traits

use catga_core::{MemoryPackDeserialize, MemoryPackSerialize, MemoryPackSerializer};

fn round_trip<T>(value: &T) -> T
where
    T: MemoryPackSerialize + MemoryPackDeserialize,
{
    let bytes = MemoryPackSerializer::serialize(value).expect("encode collection");
    MemoryPackSerializer::deserialize(&bytes).expect("decode collection")
}

#[test]
fn standard_collections_round_trip_empty_and_populated_values() {
    use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, LinkedList, VecDeque};

    #[cfg(feature = "ahash")]
    use ahash::{AHashMap, AHashSet};
    #[cfg(feature = "hashbrown")]
    use hashbrown::{HashMap as HashbrownHashMap, HashSet as HashbrownHashSet};

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
