//! Additional valid `#[derive(Message)]` shapes: enums, tuple structs, generics, lone
//! authorization forms, and foreign-attribute pass-through.

use catga_core::{AuthorizedRequest, Message, MessagePriority, Request};

// Enums and tuple structs derive `Message` (trace tag scanning is struct-only).
#[derive(catga_core_macros::Message)]
pub enum State {
    Active,
    Retired,
}

#[derive(catga_core_macros::Message)]
pub struct Envelope(pub u64);

// Generic parameters gain `Send + Sync + 'static` bounds; const parameters pass through.
#[derive(catga_core_macros::Message)]
pub struct GenericNamed<T> {
    pub value: T,
}

#[derive(catga_core_macros::Message)]
pub struct WithConst<T, const N: usize> {
    pub values: [T; N],
}

// Each lone authorization form maps to its own requirements constructor.
#[derive(catga_core_macros::Message)]
#[catga(authorize)]
pub struct LoneAuthorize;

impl Request for LoneAuthorize {
    type Response = ();
}

#[derive(catga_core_macros::Message)]
#[catga(roles("ops"))]
pub struct LoneRoles;

impl Request for LoneRoles {
    type Response = ();
}

#[derive(catga_core_macros::Message)]
#[catga(policy("billing"))]
pub struct LonePolicy;

impl Request for LonePolicy {
    type Response = ();
}

// Non-`catga` attributes are ignored by every option parser.
#[derive(catga_core_macros::Message)]
#[allow(dead_code)]
#[catga(version = 2)]
pub struct ForeignAttributes {
    #[allow(dead_code)]
    value: u64,
}

fn main() {
    assert_eq!(State::Active.schema_version(), 1);
    assert_eq!(Envelope(1).priority(), MessagePriority::Normal);
    assert!(
        GenericNamed { value: 1_u8 }
            .message_type()
            .contains("GenericNamed")
    );
    let _with_const = WithConst::<u8, 2> { values: [0, 1] };

    assert_eq!(
        LoneAuthorize::authorization(),
        catga_core::AuthorizationRequirements::authenticated()
    );
    assert_eq!(LoneRoles::authorization().roles(), &["ops"]);
    assert_eq!(LonePolicy::authorization().policy(), Some("billing"));

    assert_eq!(ForeignAttributes { value: 1 }.schema_version(), 2);
}
