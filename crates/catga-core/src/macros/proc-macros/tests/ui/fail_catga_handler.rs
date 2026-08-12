//! `#[catga_handler]` rejection contracts.

pub struct NotATraitImpl;

#[catga_core_macros::catga_handler]
impl NotATraitImpl {}

pub struct WrongTrait;

#[catga_core_macros::catga_handler]
impl PartialEq<u8> for WrongTrait {}

pub struct UntypedTraitImpl;

#[catga_core_macros::catga_handler]
impl Handler for UntypedTraitImpl {}

#[catga_core_macros::catga_handler]
pub struct NotAnImplBlock;

fn main() {}
