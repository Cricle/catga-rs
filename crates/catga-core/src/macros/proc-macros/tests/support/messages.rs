//! Shared message fixtures included via `#[path]` from each test target.
//!
//! Every including target exercises all three message kinds, so nothing in this module is
//! dead code in any of them.

#[catga_core::catga_request(response = u64)]
pub struct Ask {
    pub value: u64,
}

#[derive(catga_core::catga_command)]
pub struct Log;

#[derive(Clone, catga_core::catga_event)]
pub struct Bell;
