//! Manual FlowStore lifecycle measurements for every supported durable storage backend.
//!
//! Run through `scripts/performance.sh --profile full`. SQLite uses a temporary local database;
//! MySQL, PostgreSQL, SQL Server, and Redis use the Docker E2E service URLs supplied by the
//! runner. Every timed operation creates, reads, and version-updates one unique flow record.

use std::{
    env,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use catga_flow::{FlowState, FlowStore};
use catga_flow_store::SqlFlowStore;
use catga_redis::RedisFlows;

#[path = "support/performance_report.rs"]
mod performance_report;

const OPERATION_COUNT: u64 = 256;
const PAYLOAD_BYTES: usize = 256;

/// Measures a comparable create/read/update lifecycle for SQLite and all configured services.
#[tokio::test]
#[ignore = "manual Docker E2E storage benchmark; run scripts/performance.sh --profile full"]
async fn flow_store_lifecycle_reports_every_supported_backend() -> Result<(), String> {
    let temporary_directory = tempfile::tempdir().map_err(performance_report::debug_error)?;
    let sqlite_url = format!(
        "sqlite:{}",
        temporary_directory.path().join("performance.db").display()
    );
    let sqlite = SqlFlowStore::connect_sqlite(&sqlite_url)
        .await
        .map_err(performance_report::debug_error)?;
    sqlite
        .migrate()
        .await
        .map_err(performance_report::debug_error)?;

    let mut results = vec![measure_store("sqlite_flow_store_lifecycle", &sqlite, "sqlite").await?];

    let mysql_url = required_service_url("CATGA_MYSQL_URL")?;
    let mysql = SqlFlowStore::connect_mysql(&mysql_url)
        .await
        .map_err(performance_report::debug_error)?;
    mysql
        .migrate()
        .await
        .map_err(performance_report::debug_error)?;
    results.push(measure_store("mysql_flow_store_lifecycle", &mysql, "mysql").await?);

    let postgres_url = required_service_url("CATGA_POSTGRES_URL")?;
    let postgres = SqlFlowStore::connect_postgres(&postgres_url)
        .await
        .map_err(performance_report::debug_error)?;
    postgres
        .migrate()
        .await
        .map_err(performance_report::debug_error)?;
    results.push(measure_store("postgres_flow_store_lifecycle", &postgres, "postgres").await?);

    let mssql_url = required_service_url("CATGA_MSSQL_URL")?;
    let mssql = SqlFlowStore::connect_mssql(&mssql_url)
        .await
        .map_err(performance_report::debug_error)?;
    mssql
        .migrate()
        .await
        .map_err(performance_report::debug_error)?;
    results.push(measure_store("mssql_flow_store_lifecycle", &mssql, "mssql").await?);

    let redis_url = required_service_url("CATGA_REDIS_URL")?;
    let redis = RedisFlows::connect(&redis_url, format!("catga:performance:{}", suffix("redis")))
        .await
        .map_err(performance_report::debug_error)?;
    results.push(measure_store("redis_flow_store_lifecycle", &redis, "redis").await?);

    let report = performance_report::PerformanceReport {
        schema_version: 1,
        source: "Storage backends: SQLite, MySQL, PostgreSQL, SQL Server, Redis",
        environment: performance_report::environment(),
        results,
    };
    performance_report::write_report_if_configured(&report)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report).map_err(performance_report::debug_error)?
    );
    Ok(())
}

async fn measure_store<S>(
    name: &'static str,
    store: &S,
    backend: &str,
) -> Result<performance_report::BenchmarkResult, String>
where
    S: FlowStore + Sync,
{
    let warmup_id = format!("performance-{}-warmup", suffix(backend));
    let warmup = FlowState::new(
        warmup_id.as_str(),
        "performance",
        vec![0xA5; PAYLOAD_BYTES],
        "benchmark",
    );
    if !store
        .create(warmup.clone())
        .await
        .map_err(performance_report::debug_error)?
    {
        return Err(format!("{backend} did not create warm-up flow"));
    }
    if store
        .get(&warmup_id)
        .await
        .map_err(performance_report::debug_error)?
        .is_none()
    {
        return Err(format!("{backend} did not read warm-up flow"));
    }

    let rss_before_bytes = performance_report::current_rss_bytes();
    let started = Instant::now();
    let mut latencies = Vec::with_capacity(OPERATION_COUNT as usize);
    for sequence in 0..OPERATION_COUNT {
        let operation_started = Instant::now();
        let id = format!("performance-{backend}-{}-{sequence}", suffix("flow"));
        let state = FlowState::new(
            id.as_str(),
            "performance",
            vec![0xA5; PAYLOAD_BYTES],
            "benchmark",
        );
        if !store
            .create(state.clone())
            .await
            .map_err(performance_report::debug_error)?
        {
            return Err(format!("{backend} did not create {id}"));
        }
        let persisted = store
            .get(&id)
            .await
            .map_err(performance_report::debug_error)?
            .ok_or_else(|| format!("{backend} did not read {id}"))?;
        if persisted.data() != state.data() || persisted.version() != 0 {
            return Err(format!("{backend} changed {id} during create/read"));
        }
        let next = persisted
            .next_version()
            .map_err(performance_report::debug_error)?;
        if !store
            .update(0, next)
            .await
            .map_err(performance_report::debug_error)?
        {
            return Err(format!("{backend} did not update {id}"));
        }
        latencies.push(operation_started.elapsed());
    }
    Ok(performance_report::measured(
        name,
        Some(PAYLOAD_BYTES),
        started.elapsed(),
        latencies,
        "create + read + optimistic update",
        rss_before_bytes,
    ))
}

fn required_service_url(name: &str) -> Result<String, String> {
    match env::var(name) {
        Ok(url) if !url.trim().is_empty() => Ok(url),
        _ if env::var_os("CATGA_REQUIRE_EXTERNAL_SERVICES").is_some() => Err(format!(
            "{name} must be configured when CI executes storage performance tests"
        )),
        _ => Err(format!(
            "{name} is required for the manual full performance profile"
        )),
    }
}

fn suffix(label: &str) -> String {
    let nanoseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("{label}-{}-{nanoseconds}", std::process::id())
}
