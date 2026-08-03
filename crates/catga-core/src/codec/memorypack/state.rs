use super::error::MemoryPackError;
use std::any::Any;
use std::collections::HashMap;

/// Object-identity table used while encoding reference-preserving object graphs.
pub struct MemoryPackWriterOptionalState {
    next_id: u32,
    object_to_ref: HashMap<usize, u32>,
}

impl MemoryPackWriterOptionalState {
    /// Creates an empty object-identity table.
    pub fn new() -> Self {
        Self {
            next_id: 0,
            object_to_ref: HashMap::new(),
        }
    }

    /// Clears every recorded object identity for the next frame.
    pub fn reset(&mut self) {
        self.object_to_ref.clear();
        self.next_id = 0;
    }

    /// Returns whether `value` was known and its stable per-frame reference ID.
    pub fn get_or_add_reference<T: ?Sized>(&mut self, value: &T) -> (bool, u32) {
        let ptr = value as *const T as *const () as usize;

        if let Some(&id) = self.object_to_ref.get(&ptr) {
            (true, id)
        } else {
            let id = self.next_id;
            self.next_id += 1;
            self.object_to_ref.insert(ptr, id);
            (false, id)
        }
    }
}

impl Default for MemoryPackWriterOptionalState {
    fn default() -> Self {
        Self::new()
    }
}

/// Object-reference table used while decoding reference-preserving object graphs.
pub struct MemoryPackReaderOptionalState {
    ref_to_object: HashMap<u32, Box<dyn Any>>,
}

impl MemoryPackReaderOptionalState {
    /// Creates an empty object-reference table.
    pub fn new() -> Self {
        Self {
            ref_to_object: HashMap::new(),
        }
    }

    /// Clears every decoded object reference for the next frame.
    pub fn reset(&mut self) {
        self.ref_to_object.clear();
    }

    /// Clones the previously decoded object stored under `id` when its type matches `T`.
    pub fn get_object_reference<T: 'static + Clone>(&self, id: u32) -> Result<T, MemoryPackError> {
        self.ref_to_object
            .get(&id)
            .and_then(|boxed: &Box<dyn Any>| boxed.downcast_ref::<T>())
            .cloned()
            .ok_or_else(|| {
                MemoryPackError::DeserializationError(format!(
                    "Object is not found in this reference id: {}",
                    id
                ))
            })
    }

    /// Registers a newly decoded object under an unused reference ID.
    pub fn add_object_reference<T: 'static>(
        &mut self,
        id: u32,
        value: T,
    ) -> Result<(), MemoryPackError> {
        if self.ref_to_object.contains_key(&id) {
            return Err(MemoryPackError::DeserializationError(format!(
                "Object is already added, id: {}",
                id
            )));
        }
        self.ref_to_object.insert(id, Box::new(value));
        Ok(())
    }

    /// Replaces the object stored under an existing reference ID.
    pub fn update_object_reference<T: 'static>(
        &mut self,
        id: u32,
        value: T,
    ) -> Result<(), MemoryPackError> {
        if let Some(entry) = self.ref_to_object.get_mut(&id) {
            *entry = Box::new(value);
            Ok(())
        } else {
            Err(MemoryPackError::DeserializationError(format!(
                "Object not found for update, id: {}",
                id
            )))
        }
    }
}

impl Default for MemoryPackReaderOptionalState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writer_state_assigns_ids_and_reader_state_handles_replacement() {
        let value = String::from("value");
        let mut writer = MemoryPackWriterOptionalState::default();
        assert_eq!(writer.get_or_add_reference(&value), (false, 0));
        assert_eq!(writer.get_or_add_reference(&value), (true, 0));
        writer.reset();
        assert_eq!(writer.get_or_add_reference(&value), (false, 0));

        let mut reader = MemoryPackReaderOptionalState::default();
        reader
            .add_object_reference(1, value)
            .expect("reference adds");
        assert_eq!(
            reader
                .get_object_reference::<String>(1)
                .expect("stored string reference"),
            "value"
        );
        reader
            .update_object_reference(1, String::from("updated"))
            .expect("existing reference updates");
        assert_eq!(
            reader
                .get_object_reference::<String>(1)
                .expect("updated string reference"),
            "updated"
        );
        assert!(reader.update_object_reference(2, 1_u8).is_err());
        assert!(reader.get_object_reference::<u8>(1).is_err());
        reader.reset();
        assert!(reader.get_object_reference::<String>(1).is_err());
    }
}
