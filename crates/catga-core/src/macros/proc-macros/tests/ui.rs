//! Compile-pass and compile-fail contract tests for every macro entry point.
//!
//! The `.stderr` snapshots pin the rejection messages emitted by the expansion-time
//! validation (`syn::Error` arms); the pass cases pin that valid inputs expand against the
//! real `catga_core` API.

#[test]
fn macro_contracts() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/ui/pass_*.rs");
    cases.compile_fail("tests/ui/fail_*.rs");
}
