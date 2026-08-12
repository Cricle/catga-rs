//! `#[catga_auto]` rejection contracts.

#[catga_core_macros::catga_auto]
mod no_handlers {
    pub struct NotAHandler;
}

#[catga_core_macros::catga_auto]
mod untyped_handler {
    pub struct UntypedHandler;
    impl Handler for UntypedHandler {}
}

#[catga_core_macros::catga_auto]
pub struct NotAModule;

fn main() {}
