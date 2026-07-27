//! Regression coverage for feature-gated SQL migration statements.

#[cfg(feature = "mysql")]
#[test]
fn mysql_continuation_migration_backfills_the_due_index() {
    let migration = include_str!("../src/mysql_suspended.rs");

    assert!(migration.contains(
        "CREATE INDEX IF NOT EXISTS catga_flow_continuations_due_idx \\
         ON catga_flow_continuations(deadline_ms, lease_until_ms, flow_key)"
    ));
}

#[cfg(feature = "mssql")]
#[test]
fn mssql_continuation_migration_backfills_the_due_index_before_timeout_polling() {
    let migration = include_str!("../src/mssql_suspended.rs");
    let timeout_poll = include_str!("../src/mssql_timeout.rs");

    assert!(migration.contains("name = N'catga_flow_continuations_due_idx'"));
    assert!(timeout_poll.contains("INDEX(catga_flow_continuations_due_idx)"));
}

#[test]
fn mssql_scheduler_keeps_idempotent_creation_atomic_and_claims_compatible_with_rcsi() {
    let scheduler = include_str!("../src/mssql_scheduler.rs");
    assert!(scheduler.contains("BEGIN TRANSACTION"));
    assert!(scheduler.contains("READCOMMITTEDLOCK"));
}
