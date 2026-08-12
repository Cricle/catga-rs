//! `#[catga_request]` rejection contracts.

#[catga_core_macros::catga_request]
pub struct MissingResponse;

#[catga_core_macros::catga_request(response =)]
pub struct EmptyResponse;

#[catga_core_macros::catga_request(response = 7)]
pub struct ResponseNotAType;

#[catga_core_macros::catga_request(response = u64)]
pub fn not_a_message() {}

fn main() {}
