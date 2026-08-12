//! `catga_handlers!` rejection contracts.

catga_core_macros::catga_handlers! {
    bogus Msg => handler;
}

catga_core_macros::catga_handlers! {
    request Dup => handler_a;
    request Dup => handler_b;
}

catga_core_macros::catga_handlers! {
    command DupCmd => handler_a;
    command DupCmd => handler_b;
}

catga_core_macros::catga_handlers! {
    event Empty => [];
}

catga_core_macros::catga_handlers! {
    request Broken Grammar
}

fn main() {}
