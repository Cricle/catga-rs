//! Tests for MemoryPack smart pointer traits

use std::rc::Rc;
use std::sync::Arc;

use catga_core::MemoryPackSerializer;

#[test]
fn boxed_rc_arc_and_boxed_str_values_round_trip() {
    let boxed = Box::new(7_u32);
    let bytes = MemoryPackSerializer::serialize(&boxed).expect("box serializes");
    assert_eq!(
        MemoryPackSerializer::deserialize::<Box<u32>>(&bytes).expect("box deserializes"),
        boxed
    );

    let rc = Rc::new(String::from("rc"));
    let bytes = MemoryPackSerializer::serialize(&rc).expect("rc serializes");
    assert_eq!(
        &*MemoryPackSerializer::deserialize::<Rc<String>>(&bytes).expect("rc deserializes"),
        "rc"
    );

    let arc = Arc::new(String::from("arc"));
    let bytes = MemoryPackSerializer::serialize(&arc).expect("arc serializes");
    assert_eq!(
        &*MemoryPackSerializer::deserialize::<Arc<String>>(&bytes).expect("arc deserializes"),
        "arc"
    );

    let boxed_str: Box<str> = "boxed".into();
    let bytes = MemoryPackSerializer::serialize(&boxed_str).expect("boxed string serializes");
    assert_eq!(
        &*MemoryPackSerializer::deserialize::<Box<str>>(&bytes)
            .expect("boxed string deserializes"),
        "boxed"
    );
}
