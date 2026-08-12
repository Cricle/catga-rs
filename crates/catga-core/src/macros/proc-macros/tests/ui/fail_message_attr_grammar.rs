//! `#[derive(Message)]` rejects malformed `#[catga(...)]` attribute grammar at expansion time.

#[derive(catga_core_macros::Message)]
#[catga(version =)]
struct BrokenAttribute;

fn main() {}
