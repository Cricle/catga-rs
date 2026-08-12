//! `#[derive(Message)]` rejects invalid authorization options at expansion time.

#[derive(catga_core_macros::Message)]
#[catga(bogus)]
struct UnknownOption;

#[derive(catga_core_macros::Message)]
#[catga(policy("a"), policy("b"))]
struct PolicyTwice;

#[derive(catga_core_macros::Message)]
#[catga(roles(1))]
struct RoleNotAString;

#[derive(catga_core_macros::Message)]
#[catga(policy(admin))]
struct PolicyNotAString;

fn main() {}
