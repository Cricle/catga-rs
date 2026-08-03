#![forbid(unsafe_code)]
//! Re-exports procedural macros from the internal catga-core-macros crate.

pub use catga_core_macros::{
    Message, catga_auto, catga_command, catga_event, catga_handler, catga_handlers, catga_main,
    catga_request, catga_typed_mediator,
};
