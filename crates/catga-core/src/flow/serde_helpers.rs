//! Serialization helpers for types not directly supported by serde.
//!
//! This module provides custom serde serialization/deserialization functions
//! for types like `Arc<[T]>` that don't have built-in serde support.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::sync::Arc;

/// Serializes an `Arc<[T]>` as a sequence, converting to Vec during serialization.
pub fn serialize_arc_slice<S, T>(arc: &Arc<[T]>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: Serialize,
{
    arc.as_ref().serialize(serializer)
}

/// Deserializes an `Arc<[T]>` from a sequence.
pub fn deserialize_arc_slice<'de, D, T>(deserializer: D) -> Result<Arc<[T]>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    let vec = Vec::<T>::deserialize(deserializer)?;
    Ok(vec.into())
}

/// Serializes an `Option<Arc<[T]>>` as an optional sequence.
pub fn serialize_optional_arc_slice<S, T>(
    opt: &Option<Arc<[T]>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: Serialize,
{
    match opt {
        Some(arc) => serializer.serialize_some(arc.as_ref()),
        None => serializer.serialize_none(),
    }
}

/// Deserializes an `Option<Arc<[T]>>` from an optional sequence.
pub fn deserialize_optional_arc_slice<'de, D, T>(
    deserializer: D,
) -> Result<Option<Arc<[T]>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<Vec<T>>::deserialize(deserializer).map(|opt| opt.map(Vec::into))
}
