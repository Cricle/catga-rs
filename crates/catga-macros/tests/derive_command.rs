//! Tests for the catga_command derive macro.

use catga_core::{Command, Message};
use catga_macros::catga_command;

#[derive(catga_command)]
struct CreateUser {
    name: String,
}

#[derive(catga_command)]
struct DeleteUser(u64);

#[test]
fn implements_message() {
    let cmd = CreateUser { name: "Alice".into() };
    assert!(cmd.message_type().ends_with("CreateUser"));
}

#[test]
fn implements_command() {
    fn assert_command<T: Command>() {}
    assert_command::<CreateUser>();
    assert_command::<DeleteUser>();
}
