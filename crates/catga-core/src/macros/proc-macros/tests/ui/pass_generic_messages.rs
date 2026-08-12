//! Message derives add `Clone + Send + Sync + 'static` bounds to generic parameters.

use catga_core::Message;

#[catga_core_macros::catga_request(response = u64)]
pub struct GenericAsk<T> {
    pub payload: T,
}

// Const parameters are skipped by the bound generation; the scanner stops collecting the
// response type at the next `=`, so the trailing segment is ignored.
#[catga_core_macros::catga_request(response = u64 = u8)]
pub struct ConstAsk<T, const N: usize> {
    pub payload: [T; N],
}

#[derive(catga_core::catga_command)]
pub struct GenericCommand<T> {
    pub payload: T,
}

#[derive(catga_core::catga_command)]
pub struct ConstCommand<const N: usize>;

#[derive(Clone, catga_core::catga_event)]
pub struct GenericEvent<T> {
    pub payload: T,
}

#[derive(Clone, catga_core::catga_event)]
pub struct ConstEvent<const N: usize>;

fn main() {
    let ask = GenericAsk { payload: 1_u64 };
    assert!(ask.message_type().contains("GenericAsk"));
    let const_ask = ConstAsk::<u8, 2> { payload: [0, 1] };
    assert!(const_ask.message_type().contains("ConstAsk"));

    let command = GenericCommand {
        payload: String::new(),
    };
    assert!(command.message_type().contains("GenericCommand"));
    assert!(ConstCommand::<4>.message_type().contains("ConstCommand"));

    let event = GenericEvent { payload: 1_u8 };
    assert!(event.message_type().contains("GenericEvent"));
    assert!(ConstEvent::<4>.message_type().contains("ConstEvent"));
}
