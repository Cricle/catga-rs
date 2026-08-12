//! `#[derive(Message)]` rejects invalid `version` arguments at expansion time.

#[derive(catga_core_macros::Message)]
#[catga(version = "one")]
struct VersionNotAnInteger;

#[derive(catga_core_macros::Message)]
#[catga(version = 0)]
struct VersionZero;

#[derive(catga_core_macros::Message)]
#[catga(version = 2, version = 3)]
struct VersionTwice;

#[derive(catga_core_macros::Message)]
#[catga(version = 99999999999999999999999999)]
struct VersionTooLarge;

fn main() {}
