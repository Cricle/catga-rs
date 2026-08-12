//! `catga_typed_mediator!` rejection contracts.

catga_core_macros::catga_typed_mediator! {
    pub struct UnknownKind;
    bogus Msg => handler;
}

catga_core_macros::catga_typed_mediator! {
    pub struct DuplicateRequest;
    request Dup => handler_a;
    request Dup => handler_b;
}

catga_core_macros::catga_typed_mediator! {
    pub struct DuplicateCommand;
    command DupCmd => handler_a;
    command DupCmd => handler_b;
}

catga_core_macros::catga_typed_mediator! {
    pub struct EmptyEvent;
    event Empty => [];
}

catga_core_macros::catga_typed_mediator! {
    pub struct BrokenHeader
    request Msg => handler;
}

fn main() {}
