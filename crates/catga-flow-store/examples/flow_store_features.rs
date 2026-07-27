//! Selects only the FlowStore backends deployed by the application.
//!
//! Run this local SQLite example with:
//! `cargo run -p catga-flow-store --example flow_store_features --features sqlite`.
//! Network backends stay explicit: enable their feature, then call the corresponding helper with
//! an application-owned connection URL during controlled startup.

use catga_core::CatgaResult;
use catga_flow_store::SqlFlowStore;

#[cfg(feature = "sqlite")]
async fn sqlite_store() -> CatgaResult<SqlFlowStore> {
    SqlFlowStore::connect_sqlite("sqlite::memory:").await
}

#[cfg(feature = "mysql")]
async fn mysql_store(url: &str) -> CatgaResult<SqlFlowStore> {
    SqlFlowStore::connect_mysql(url).await
}

#[cfg(feature = "postgres")]
async fn postgres_store(url: &str) -> CatgaResult<SqlFlowStore> {
    SqlFlowStore::connect_postgres(url).await
}

#[cfg(feature = "mssql")]
async fn mssql_store(url: &str) -> CatgaResult<SqlFlowStore> {
    SqlFlowStore::connect_mssql(url).await
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> CatgaResult<()> {
    let store = sqlite_store().await?;
    store.migrate().await?;

    #[cfg(feature = "mysql")]
    let _ = mysql_store;
    #[cfg(feature = "postgres")]
    let _ = postgres_store;
    #[cfg(feature = "mssql")]
    let _ = mssql_store;

    Ok(())
}
