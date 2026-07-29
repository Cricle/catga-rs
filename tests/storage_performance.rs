//! Manual FlowStore lifecycle measurements for every supported durable storage backend.
//!
//! Run through `scripts/performance.sh --profile full`. SQLite uses a temporary local database;
//! MySQL, PostgreSQL, SQL Server, and Redis use the Docker E2E service URLs supplied by the
//! runner. Every timed operation creates, reads, and version-updates one unique flow record.

use std::{
    env,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use catga_flow::{FlowState, FlowStore};
use catga_flow_store::SqlFlowStore;
use catga_redis::RedisFlows;
use futures::{StreamExt, TryStreamExt, stream};

#[path = "support/performance_report.rs"]
mod performance_report;

const OPERATION_COUNT: u64 = 256;
const PAYLOAD_BYTES: usize = 256;
const CONCURRENCY_LEVELS: [usize; 2] = [4, 8];
static UNIQUE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

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

    let mut results = measure_store_variants(
        &sqlite,
        "sqlite",
        [
            "sqlite_flow_store_lifecycle",
            "sqlite_flow_store_lifecycle_bounded_concurrency_4",
            "sqlite_flow_store_lifecycle_bounded_concurrency_8",
        ],
    )
    .await?;

    let mysql_url = required_service_url("CATGA_MYSQL_URL")?;
    let mysql = SqlFlowStore::connect_mysql(&mysql_url)
        .await
        .map_err(performance_report::debug_error)?;
    mysql
        .migrate()
        .await
        .map_err(performance_report::debug_error)?;
    results.extend(
        measure_store_variants(
            &mysql,
            "mysql",
            [
                "mysql_flow_store_lifecycle",
                "mysql_flow_store_lifecycle_bounded_concurrency_4",
                "mysql_flow_store_lifecycle_bounded_concurrency_8",
            ],
        )
        .await?,
    );

    let postgres_url = required_service_url("CATGA_POSTGRES_URL")?;
    let postgres = SqlFlowStore::connect_postgres(&postgres_url)
        .await
        .map_err(performance_report::debug_error)?;
    postgres
        .migrate()
        .await
        .map_err(performance_report::debug_error)?;
    results.extend(
        measure_store_variants(
            &postgres,
            "postgres",
            [
                "postgres_flow_store_lifecycle",
                "postgres_flow_store_lifecycle_bounded_concurrency_4",
                "postgres_flow_store_lifecycle_bounded_concurrency_8",
            ],
        )
        .await?,
    );

    let mssql_url = required_service_url("CATGA_MSSQL_URL")?;
    let mssql = SqlFlowStore::connect_mssql(&mssql_url)
        .await
        .map_err(performance_report::debug_error)?;
    mssql
        .migrate()
        .await
        .map_err(performance_report::debug_error)?;
    results.extend(
        measure_store_variants(
            &mssql,
            "mssql",
            [
                "mssql_flow_store_lifecycle",
                "mssql_flow_store_lifecycle_bounded_concurrency_4",
                "mssql_flow_store_lifecycle_bounded_concurrency_8",
            ],
        )
        .await?,
    );

    let redis_url = required_service_url("CATGA_REDIS_URL")?;
    let redis = RedisFlows::connect(&redis_url, format!("catga:performance:{}", suffix("redis")))
        .await
        .map_err(performance_report::debug_error)?;
    results.extend(
        measure_store_variants(
            &redis,
            "redis",
            [
                "redis_flow_store_lifecycle",
                "redis_flow_store_lifecycle_bounded_concurrency_4",
                "redis_flow_store_lifecycle_bounded_concurrency_8",
            ],
        )
        .await?,
    );

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

async fn measure_store_variants<S>(
    store: &S,
    backend: &str,
    names: [&'static str; 3],
) -> Result<Vec<performance_report::BenchmarkResult>, String>
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

    let mut results = vec![
        measure_store(
            names[0],
            store,
            backend,
            "create + read + optimistic update; bounded_concurrency=1",
        )
        .await?,
    ];
    for (&name, concurrency) in names[1..].iter().zip(CONCURRENCY_LEVELS) {
        results.push(
            measure_store_bounded(
                name,
                store,
                backend,
                concurrency,
                match concurrency {
                    4 => "create + read + optimistic update; bounded_concurrency=4",
                    8 => "create + read + optimistic update; bounded_concurrency=8",
                    _ => unreachable!("only declared concurrency levels are benchmarked"),
                },
            )
            .await?,
        );
    }
    Ok(results)
}

async fn measure_store<S>(
    name: &'static str,
    store: &S,
    backend: &str,
    latency_scope: &'static str,
) -> Result<performance_report::BenchmarkResult, String>
where
    S: FlowStore + Sync,
{
    let rss_before_bytes = performance_report::current_rss_bytes();
    let started = Instant::now();
    let mut latencies = Vec::with_capacity(OPERATION_COUNT as usize);
    for sequence in 0..OPERATION_COUNT {
        latencies.push(execute_lifecycle(store, backend, sequence).await?);
    }
    Ok(performance_report::measured(
        name,
        Some(PAYLOAD_BYTES),
        started.elapsed(),
        latencies,
        latency_scope,
        rss_before_bytes,
    ))
}

async fn measure_store_bounded<S>(
    name: &'static str,
    store: &S,
    backend: &str,
    concurrency: usize,
    latency_scope: &'static str,
) -> Result<performance_report::BenchmarkResult, String>
where
    S: FlowStore + Sync,
{
    let rss_before_bytes = performance_report::current_rss_bytes();
    let started = Instant::now();
    let latencies = stream::iter(0..OPERATION_COUNT)
        .map(|sequence| execute_lifecycle(store, backend, sequence))
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(performance_report::measured(
        name,
        Some(PAYLOAD_BYTES),
        started.elapsed(),
        latencies,
        latency_scope,
        rss_before_bytes,
    ))
}

async fn execute_lifecycle<S>(store: &S, backend: &str, sequence: u64) -> Result<Duration, String>
where
    S: FlowStore + Sync,
{
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
    Ok(operation_started.elapsed())
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
    let sequence = UNIQUE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{label}-{}-{nanoseconds}-{sequence}", std::process::id())
}
