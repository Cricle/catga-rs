#[test]
fn macro_validation_rejects_invalid_inputs() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/*.rs");
}
