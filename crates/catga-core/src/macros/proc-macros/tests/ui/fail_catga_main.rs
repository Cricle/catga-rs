//! `#[catga_main]` rejection contracts.

#[catga_core_macros::catga_main(transport = ())]
async fn transport_argument() {}

#[catga_core_macros::catga_main(bogus)]
async fn unknown_argument() {}

#[catga_core_macros::catga_main]
pub struct NotAFunction;

fn main() {}
