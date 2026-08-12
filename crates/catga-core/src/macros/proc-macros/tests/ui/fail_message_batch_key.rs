//! `#[derive(Message)]` rejects invalid `batch_key` declarations at expansion time.

#[derive(catga_core_macros::Message)]
#[catga(batch_key = "a", batch_key = "b")]
struct BatchKeyTwice {
    a: u64,
    b: u64,
}

#[derive(catga_core_macros::Message)]
#[catga(batch_key = 1)]
struct BatchKeyNotAString {
    a: u64,
}

#[derive(catga_core_macros::Message)]
#[catga(batch_key = "")]
struct BatchKeyEmpty {
    a: u64,
}

#[derive(catga_core_macros::Message)]
#[catga(batch_key = "a")]
enum BatchKeyOnEnum {
    A,
}

#[derive(catga_core_macros::Message)]
#[catga(batch_key = "field")]
struct BatchKeyOnTupleStruct(u64);

#[derive(catga_core_macros::Message)]
#[catga(batch_key = "missing")]
struct BatchKeyMissingField {
    a: u64,
}

fn main() {}
