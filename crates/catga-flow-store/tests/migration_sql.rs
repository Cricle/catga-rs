//! Regression coverage for feature-gated SQL migration statements.

#[cfg(feature = "mysql")]
#[test]
fn mysql_continuation_schema_declares_the_due_index() {
    let migration = include_str!("../src/mysql_suspended.rs");

    assert!(
        migration.contains(
            "INDEX catga_flow_continuations_due_idx(deadline_ms, lease_until_ms, flow_key)"
        )
    );
}

#[cfg(feature = "mssql")]
#[test]
fn mssql_continuation_migration_backfills_the_due_index_before_timeout_polling() {
    let migration = include_str!("../src/mssql_suspended.rs");
    let timeout_poll = include_str!("../src/mssql_timeout.rs");

    assert!(migration.contains("name = N'catga_flow_continuations_due_idx'"));
    assert!(timeout_poll.contains("INDEX(catga_flow_continuations_due_idx)"));
}

#[cfg(feature = "mssql")]
#[test]
fn mssql_continuation_migration_serializes_concurrent_schema_changes() {
    let migration = include_str!("../src/mssql_suspended.rs");

    assert!(migration.contains("sp_getapplock"));
    assert!(migration.contains("catga_flow_continuations_schema"));
    assert!(migration.contains("@LockOwner = N'Transaction'"));
    assert!(!migration.contains("SET XACT_ABORT ON"));
}

#[test]
fn mssql_scheduler_keeps_idempotent_creation_atomic_and_claims_compatible_with_rcsi() {
    let scheduler = include_str!("../src/mssql_scheduler.rs");
    assert!(scheduler.contains("BEGIN TRANSACTION"));
    assert!(scheduler.contains("READCOMMITTEDLOCK"));
}

#[test]
fn mssql_scheduler_claim_cte_projects_the_lease_columns_it_updates() {
    let scheduler = include_str!("../src/mssql_scheduler.rs");

    assert!(scheduler.contains(
        "schedule_id, flow_id, state_id, due_at_ms, due_at_subsec_ns, lease_owner, lease_until_ms"
    ));
}
