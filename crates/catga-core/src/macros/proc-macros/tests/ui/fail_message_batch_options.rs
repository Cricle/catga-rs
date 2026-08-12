//! `#[derive(Message)]` rejects invalid `batch(...)` options at expansion time.

#[derive(catga_core_macros::Message)]
#[catga(batch(max_batch_size = 2), batch(max_batch_size = 3))]
struct BatchTwice;

#[derive(catga_core_macros::Message)]
#[catga(batch(max_batch_size))]
struct BatchOptionNotNamedValue;

#[derive(catga_core_macros::Message)]
#[catga(batch(a::b = 3))]
struct BatchOptionNotAnIdentifier;

#[derive(catga_core_macros::Message)]
#[catga(batch(max_batch_size = "x"))]
struct BatchOptionNotAnInteger;

#[derive(catga_core_macros::Message)]
#[catga(batch(max_batch_size = 0))]
struct BatchOptionZero;

#[derive(catga_core_macros::Message)]
#[catga(batch(max_batch_size = 99999999999999999999))]
struct BatchOptionTooLarge;

#[derive(catga_core_macros::Message)]
#[catga(batch(bogus = 1))]
struct BatchOptionUnknown;

fn main() {}
