//! Reliability diagnostic bounds and failure-context contract tests.

use std::time::Duration;

use catga_core::{
    DeadLetter, DeadLetterDiagnostics, Envelope, ErrorCode, InboxClaim,
    MAX_DEAD_LETTER_DESCRIPTION_BYTES, MAX_DEAD_LETTER_STAGE_BYTES, MAX_RETENTION_CLEANUP_LIMIT,
    MessageMetadata, inbox_claim_expires_at, validate_completed_retention,
    validate_inbox_claim_lease, validate_retention_cleanup_limit,
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

#[test]
fn diagnostic_and_reliability_bounds_reject_invalid_values_without_retaining_them() {
    for stage in ["", "has space", "contains/slash"] {
        assert!(matches!(
            DeadLetterDiagnostics::try_at(7, ErrorCode::Internal, stage),
            Err(error) if error.code() == ErrorCode::Validation
        ));
    }
    assert!(matches!(
        DeadLetterDiagnostics::try_at(
            7,
            ErrorCode::Internal,
            "x".repeat(MAX_DEAD_LETTER_STAGE_BYTES + 1),
        ),
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert!(matches!(
        validate_completed_retention(Duration::ZERO),
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert!(matches!(
        validate_retention_cleanup_limit(MAX_RETENTION_CLEANUP_LIMIT + 1),
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert!(matches!(
        validate_inbox_claim_lease(Duration::ZERO),
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert!(matches!(
        inbox_claim_expires_at(Duration::ZERO),
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert_eq!(InboxClaim::new(42, 0), None);
    let claim = InboxClaim::new(42, 7).expect("nonzero inbox generation is valid");
    assert_eq!(claim.message_id(), 42);
    assert_eq!(claim.generation(), 7);
}

#[test]
fn legacy_and_framework_dead_letters_truncate_utf8_without_losing_the_failure_category()
-> catga_core::CatgaResult<()> {
    let oversized = "🚀".repeat(MAX_DEAD_LETTER_DESCRIPTION_BYTES);
    let legacy = DeadLetter::new(envelope(), oversized.clone(), 2);
    assert!(legacy.reason().len() <= MAX_DEAD_LETTER_DESCRIPTION_BYTES);
    assert!(legacy.reason().is_char_boundary(legacy.reason().len()));
    assert_eq!(legacy.diagnostics().error_code(), ErrorCode::Internal);
    assert_eq!(legacy.diagnostics().stage(), "legacy");

    let failure = catga_core::CatgaError::new(ErrorCode::Timeout, oversized);
    let captured = DeadLetter::from_failure(envelope(), &failure, 3, "worker.handle")?;
    assert!(captured.reason().len() <= MAX_DEAD_LETTER_DESCRIPTION_BYTES);
    assert_eq!(captured.diagnostics().error_code(), ErrorCode::Timeout);
    assert_eq!(captured.diagnostics().stage(), "worker.handle");
    Ok(())
}
