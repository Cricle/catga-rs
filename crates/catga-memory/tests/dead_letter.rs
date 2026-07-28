use catga_core::{
    DeadLetter, DeadLetterDiagnostics, DeadLetterStore, Envelope, ErrorCode, MessageMetadata,
};
use catga_memory::MemoryDeadLetters;

#[tokio::test]
async fn memory_dead_letters_preserve_failure_diagnostics() -> catga_core::CatgaResult<()> {
    let store = MemoryDeadLetters::new(1)?;
    let envelope = Envelope::new(
        3,
        "tests.dead-letter",
        vec![],
        MessageMetadata::new(3, None),
    );
    let diagnostics = DeadLetterDiagnostics::try_at(123, ErrorCode::Timeout, "consumer.handle")?;
    let letter = DeadLetter::try_with_diagnostics(envelope, "expired", 2, diagnostics)?;

    store.enqueue(letter).await?;
    let retained = store.list(1).await?;
    assert_eq!(retained[0].diagnostics().failed_at_unix_ms(), 123);
    assert_eq!(retained[0].diagnostics().error_code(), ErrorCode::Timeout);
    assert_eq!(retained[0].diagnostics().stage(), "consumer.handle");
    Ok(())
}
