//! Tests for MemoryPack state module

use catga_core::codec::memorypack::state::{
    MemoryPackReaderOptionalState, MemoryPackWriterOptionalState,
};

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
