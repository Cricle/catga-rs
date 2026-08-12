//! `#[derive(Message)]` rejects invalid `priority` arguments at expansion time.

#[derive(catga_core_macros::Message)]
#[catga(priority = 3)]
struct PriorityNotAPath;

#[derive(catga_core_macros::Message)]
#[catga(priority = a::b)]
struct PriorityNotAnIdentifier;

#[derive(catga_core_macros::Message)]
#[catga(priority = urgent)]
struct PriorityUnknown;

#[derive(catga_core_macros::Message)]
#[catga(priority = high, priority = low)]
struct PriorityTwice;

fn main() {}
