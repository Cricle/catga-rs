//! `#[catga_service]` rejection contracts.

pub struct Service;

#[catga_core_macros::catga_service(1)]
impl Service {}

#[catga_core_macros::catga_service]
pub struct NotAnImplBlock;

fn main() {}
