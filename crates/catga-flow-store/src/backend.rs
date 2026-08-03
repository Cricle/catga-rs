//! Private feature-gated database pool selection.

/// One owned connection pool selected at construction time.
pub(crate) enum Backend {
    /// SQLite uses the SQLx native asynchronous pool.
    #[cfg(feature = "sqlite")]
    Sqlite(sqlx::SqlitePool),
    /// MySQL support is enabled by the `mysql` feature.
    #[cfg(feature = "mysql")]
    MySql(sqlx::MySqlPool),
    /// PostgreSQL support is enabled by the `postgres` feature.
    #[cfg(feature = "postgres")]
    Postgres(sqlx::PgPool),
    /// SQL Server support is enabled by the `mssql` feature.
    #[cfg(feature = "mssql")]
    Mssql(crate::MssqlPool),
}
