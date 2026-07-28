use catga_core::{
    DeadLetter, DeadLetterDiagnostics, Envelope, ErrorCode, MAX_DEAD_LETTER_DESCRIPTION_BYTES,
    MessageMetadata,
};

fn envelope() -> Envelope {
    Envelope::new(
        7,
        "tests.dead-letter",
        vec![1, 2, 3],
        MessageMetadata::new(7, None),
    )
}

#[test]
fn diagnostics_retain_bounded_failure_context() -> catga_core::CatgaResult<()> {
    let diagnostics = DeadLetterDiagnostics::new(ErrorCode::Timeout, "consumer.handle")?;
    let letter = DeadLetter::try_with_diagnostics(envelope(), "timed out", 3, diagnostics)?;
    assert_eq!(letter.diagnostics().error_code(), ErrorCode::Timeout);
    assert_eq!(letter.diagnostics().stage(), "consumer.handle");
    assert_eq!(letter.reason(), "timed out");
    assert!(letter.diagnostics().failed_at_unix_ms() > 0);
    Ok(())
}

#[test]
fn diagnostics_reject_an_unbounded_error_description() -> catga_core::CatgaResult<()> {
    let diagnostics = DeadLetterDiagnostics::new(ErrorCode::Internal, "pipeline")?;
    let description = "x".repeat(MAX_DEAD_LETTER_DESCRIPTION_BYTES + 1);
    let result = DeadLetter::try_with_diagnostics(envelope(), description, 1, diagnostics);
    assert_eq!(
        result.err().map(|error| error.code()),
        Some(ErrorCode::Validation)
    );
    Ok(())
}
