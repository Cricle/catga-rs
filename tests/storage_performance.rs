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

use catga_core::flow::{FlowState, FlowStore};
use catga_flow_store::{SqlFlowStore, SqlFlowStoreOptions};
use catga_redis::RedisFlows;
use futures::{StreamExt, TryStreamExt, stream};
use sqlx::{Row, mysql::MySqlPoolOptions, postgres::PgPoolOptions};

#[path = "support/performance_report.rs"]
mod performance_report;

const OPERATION_COUNT: u64 = 256;
const PAYLOAD_BYTES: usize = 256;
const CONCURRENCY_LEVELS: [usize; 3] = [4, 8, 16];
const SERVER_BENCHMARK_CONNECTIONS: u32 = 16;
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
            "sqlite_flow_store_lifecycle_bounded_concurrency_16",
        ],
    )
    .await?;
    results.extend(measure_sqlite_batch_comparison(&sqlite).await?);

    let mut metric_deltas = Vec::new();

    if let Some(mysql_url) = optional_service_url("CATGA_MYSQL_URL")? {
        let mysql = SqlFlowStore::connect_mysql_with_options(
            &mysql_url,
            SqlFlowStoreOptions::new().max_connections(SERVER_BENCHMARK_CONNECTIONS),
        )
        .await
        .map_err(performance_report::debug_error)?;
        mysql
            .migrate()
            .await
            .map_err(performance_report::debug_error)?;
        let mysql_metrics_before = capture_mysql_metrics(&mysql_url).await?;
        results.extend(
            measure_store_variants(
                &mysql,
                "mysql",
                [
                    "mysql_flow_store_lifecycle",
                    "mysql_flow_store_lifecycle_bounded_concurrency_4",
                    "mysql_flow_store_lifecycle_bounded_concurrency_8",
                    "mysql_flow_store_lifecycle_bounded_concurrency_16",
                ],
            )
            .await?,
        );
        let mysql_metrics_after = capture_mysql_metrics(&mysql_url).await?;
        metric_deltas.extend(database_metric_deltas(
            mysql_metrics_before,
            mysql_metrics_after,
        )?);
    }

    if let Some(postgres_url) = optional_service_url("CATGA_POSTGRES_URL")? {
        let postgres = SqlFlowStore::connect_postgres_with_options(
            &postgres_url,
            SqlFlowStoreOptions::new().max_connections(SERVER_BENCHMARK_CONNECTIONS),
        )
        .await
        .map_err(performance_report::debug_error)?;
        postgres
            .migrate()
            .await
            .map_err(performance_report::debug_error)?;
        let postgres_metrics_before = capture_postgres_metrics(&postgres_url).await?;
        results.extend(
            measure_store_variants(
                &postgres,
                "postgres",
                [
                    "postgres_flow_store_lifecycle",
                    "postgres_flow_store_lifecycle_bounded_concurrency_4",
                    "postgres_flow_store_lifecycle_bounded_concurrency_8",
                    "postgres_flow_store_lifecycle_bounded_concurrency_16",
                ],
            )
            .await?,
        );
        let postgres_metrics_after = capture_postgres_metrics(&postgres_url).await?;
        metric_deltas.extend(database_metric_deltas(
            postgres_metrics_before,
            postgres_metrics_after,
        )?);
    }

    if let Some(mssql_url) = optional_service_url("CATGA_MSSQL_URL")? {
        let mssql = SqlFlowStore::connect_mssql_with_options(
            &mssql_url,
            SqlFlowStoreOptions::new().max_connections(SERVER_BENCHMARK_CONNECTIONS),
        )
        .await
        .map_err(performance_report::debug_error)?;
        mssql
            .migrate()
            .await
            .map_err(performance_report::debug_error)?;
        let mssql_metrics_before = capture_mssql_metrics(&mssql_url).await?;
        results.extend(
            measure_store_variants(
                &mssql,
                "mssql",
                [
                    "mssql_flow_store_lifecycle",
                    "mssql_flow_store_lifecycle_bounded_concurrency_4",
                    "mssql_flow_store_lifecycle_bounded_concurrency_8",
                    "mssql_flow_store_lifecycle_bounded_concurrency_16",
                ],
            )
            .await?,
        );
        let mssql_metrics_after = capture_mssql_metrics(&mssql_url).await?;
        metric_deltas.extend(database_metric_deltas(
            mssql_metrics_before,
            mssql_metrics_after,
        )?);
    }

    if let Some(redis_url) = optional_service_url("CATGA_REDIS_URL")? {
        let redis =
            RedisFlows::connect(&redis_url, format!("catga:performance:{}", suffix("redis")))
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
                    "redis_flow_store_lifecycle_bounded_concurrency_16",
                ],
            )
            .await?,
        );
    }

    let report = performance_report::PerformanceReport {
        schema_version: 2,
        source: "Storage backends: SQLite, MySQL, PostgreSQL, SQL Server, Redis",
        environment: performance_report::environment(),
        results,
        database_metric_deltas: metric_deltas,
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
    names: [&'static str; 4],
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
                    16 => "create + read + optimistic update; bounded_concurrency=16",
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
    let mut result = performance_report::measured(
        name,
        Some(PAYLOAD_BYTES),
        started.elapsed(),
        latencies.iter().map(|latency| latency.total).collect(),
        latency_scope,
        rss_before_bytes,
    );
    result.phase_latencies = lifecycle_phase_latencies(&latencies);
    Ok(result)
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
    let mut result = performance_report::measured(
        name,
        Some(PAYLOAD_BYTES),
        started.elapsed(),
        latencies.iter().map(|latency| latency.total).collect(),
        latency_scope,
        rss_before_bytes,
    );
    result.phase_latencies = lifecycle_phase_latencies(&latencies);
    Ok(result)
}

/// Compares creating the same number of flows one transaction at a time versus one batched
/// transaction, isolating how much the per-commit durability flush amortizes across a batch.
async fn measure_sqlite_batch_comparison(
    store: &SqlFlowStore,
) -> Result<Vec<performance_report::BenchmarkResult>, String> {
    const FLOW_COUNT: usize = 256;
    let mut results = Vec::new();

    let rss_before_bytes = performance_report::current_rss_bytes();
    let started = Instant::now();
    let mut latencies = Vec::with_capacity(FLOW_COUNT);
    for sequence in 0..FLOW_COUNT {
        let operation_started = Instant::now();
        store
            .create(batch_flow_state("sequential", sequence))
            .await
            .map_err(performance_report::debug_error)?;
        latencies.push(operation_started.elapsed());
    }
    results.push(performance_report::measured(
        "sqlite_flow_create_sequential",
        Some(PAYLOAD_BYTES),
        started.elapsed(),
        latencies,
        "one transaction per create",
        rss_before_bytes,
    ));

    let states: Vec<FlowState> = (0..FLOW_COUNT)
        .map(|sequence| batch_flow_state("batched", sequence))
        .collect();
    let rss_before_bytes = performance_report::current_rss_bytes();
    let started = Instant::now();
    let created = store
        .create_batch(states)
        .await
        .map_err(performance_report::debug_error)?;
    let batch_elapsed = started.elapsed();
    if created
        .into_iter()
        .filter(|was_created| *was_created)
        .count()
        != FLOW_COUNT
    {
        return Err("SQLite batch did not create every flow".to_owned());
    }
    let amortized = batch_elapsed / u32::try_from(FLOW_COUNT).unwrap_or(u32::MAX);
    results.push(performance_report::measured(
        "sqlite_flow_create_batched",
        Some(PAYLOAD_BYTES),
        batch_elapsed,
        vec![amortized; FLOW_COUNT],
        "one transaction for the whole batch (amortized per flow)",
        rss_before_bytes,
    ));

    Ok(results)
}

fn batch_flow_state(tag: &str, sequence: usize) -> FlowState {
    FlowState::new(
        format!("batch-{tag}-{}-{sequence}", suffix("flow")).as_str(),
        "performance",
        vec![0xA5; PAYLOAD_BYTES],
        "benchmark",
    )
}

struct LifecycleLatency {
    total: Duration,
    create: Duration,
    get: Duration,
    update: Duration,
}

fn lifecycle_phase_latencies(
    latencies: &[LifecycleLatency],
) -> Vec<performance_report::PhaseLatency> {
    [
        (
            "create",
            latencies.iter().map(|latency| latency.create).collect(),
        ),
        ("get", latencies.iter().map(|latency| latency.get).collect()),
        (
            "update",
            latencies.iter().map(|latency| latency.update).collect(),
        ),
    ]
    .into_iter()
    .map(
        |(phase, latencies): (&'static str, Vec<Duration>)| performance_report::PhaseLatency {
            phase,
            p50_ns: performance_report::percentile_nanoseconds(&latencies, 50),
            p95_ns: performance_report::percentile_nanoseconds(&latencies, 95),
            p99_ns: performance_report::percentile_nanoseconds(&latencies, 99),
        },
    )
    .collect()
}

async fn execute_lifecycle<S>(
    store: &S,
    backend: &str,
    sequence: u64,
) -> Result<LifecycleLatency, String>
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
    let create_started = Instant::now();
    if !store
        .create(state.clone())
        .await
        .map_err(performance_report::debug_error)?
    {
        return Err(format!("{backend} did not create {id}"));
    }
    let create = create_started.elapsed();
    let get_started = Instant::now();
    let persisted = store
        .get(&id)
        .await
        .map_err(performance_report::debug_error)?
        .ok_or_else(|| format!("{backend} did not read {id}"))?;
    if persisted.data() != state.data() || persisted.version() != 0 {
        return Err(format!("{backend} changed {id} during create/read"));
    }
    let get = get_started.elapsed();
    let next = persisted
        .next_version()
        .map_err(performance_report::debug_error)?;
    let update_started = Instant::now();
    if !store
        .update(0, next)
        .await
        .map_err(performance_report::debug_error)?
    {
        return Err(format!("{backend} did not update {id}"));
    }
    Ok(LifecycleLatency {
        total: operation_started.elapsed(),
        create,
        get,
        update: update_started.elapsed(),
    })
}

struct DatabaseMetricSnapshot {
    backend: &'static str,
    values: Vec<DatabaseMetricValue>,
}

struct DatabaseMetricValue {
    metric: &'static str,
    unit: &'static str,
    value: u64,
}

#[test]
fn database_metric_deltas_preserve_counter_resets() {
    let before = DatabaseMetricSnapshot::new(
        "mysql",
        vec![DatabaseMetricValue {
            metric: "innodb_data_writes",
            unit: "operations",
            value: 12,
        }],
    );
    let after = DatabaseMetricSnapshot::new(
        "mysql",
        vec![DatabaseMetricValue {
            metric: "innodb_data_writes",
            unit: "operations",
            value: 3,
        }],
    );

    let deltas = database_metric_deltas(before, after).expect("matching counter snapshots");

    assert_eq!(deltas.len(), 1);
    assert_eq!(deltas[0].delta, -9);
}

impl DatabaseMetricSnapshot {
    fn new(backend: &'static str, values: Vec<DatabaseMetricValue>) -> Self {
        Self { backend, values }
    }
}

fn database_metric_deltas(
    before: DatabaseMetricSnapshot,
    after: DatabaseMetricSnapshot,
) -> Result<Vec<performance_report::DatabaseMetricDelta>, String> {
    if before.backend != after.backend {
        return Err(format!(
            "cannot compare {} counters to {} counters",
            before.backend, after.backend
        ));
    }
    before
        .values
        .into_iter()
        .map(|before_value| {
            let after_value = after
                .values
                .iter()
                .find(|value| value.metric == before_value.metric)
                .ok_or_else(|| {
                    format!(
                        "{} did not return a second value for {}",
                        before.backend, before_value.metric
                    )
                })?;
            if before_value.unit != after_value.unit {
                return Err(format!(
                    "{} changed the unit of {} from {} to {}",
                    before.backend, before_value.metric, before_value.unit, after_value.unit
                ));
            }
            Ok(performance_report::DatabaseMetricDelta {
                backend: before.backend,
                metric: before_value.metric,
                unit: before_value.unit,
                before: before_value.value,
                after: after_value.value,
                delta: after_value.value as i128 - before_value.value as i128,
            })
        })
        .collect()
}

async fn capture_mysql_metrics(url: &str) -> Result<DatabaseMetricSnapshot, String> {
    let pool = MySqlPoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .map_err(performance_report::debug_error)?;
    let rows = sqlx::query(
        "SHOW GLOBAL STATUS WHERE Variable_name IN (\
         'Innodb_buffer_pool_reads', 'Innodb_data_reads', 'Innodb_data_writes', \
         'Innodb_os_log_written', 'Threads_connected', 'Threads_running')",
    )
    .fetch_all(&pool)
    .await
    .map_err(performance_report::debug_error)?;
    pool.close().await;

    let values = rows
        .into_iter()
        .map(|row| {
            let name = row
                .try_get::<String, _>("Variable_name")
                .map_err(performance_report::debug_error)?;
            let value = row
                .try_get::<String, _>("Value")
                .map_err(performance_report::debug_error)?
                .parse::<u64>()
                .map_err(performance_report::debug_error)?;
            Ok((name, value))
        })
        .collect::<Result<std::collections::BTreeMap<_, _>, String>>()?;
    mysql_snapshot(&values)
}

fn mysql_snapshot(
    values: &std::collections::BTreeMap<String, u64>,
) -> Result<DatabaseMetricSnapshot, String> {
    const METRICS: [(&str, &str, &str); 6] = [
        (
            "Innodb_buffer_pool_reads",
            "innodb_buffer_pool_reads",
            "operations",
        ),
        ("Innodb_data_reads", "innodb_data_reads", "operations"),
        ("Innodb_data_writes", "innodb_data_writes", "operations"),
        ("Innodb_os_log_written", "innodb_redo_bytes", "bytes"),
        ("Threads_connected", "threads_connected", "connections"),
        ("Threads_running", "threads_running", "connections"),
    ];
    METRICS
        .into_iter()
        .map(|(native_name, metric, unit)| {
            values
                .get(native_name)
                .copied()
                .map(|value| DatabaseMetricValue {
                    metric,
                    unit,
                    value,
                })
                .ok_or_else(|| format!("MySQL did not return global status {native_name}"))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|values| DatabaseMetricSnapshot::new("mysql", values))
}

async fn capture_postgres_metrics(url: &str) -> Result<DatabaseMetricSnapshot, String> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .map_err(performance_report::debug_error)?;
    let row = sqlx::query(
        "SELECT database_stats.xact_commit, database_stats.tup_returned, \
         database_stats.tup_fetched, database_stats.tup_inserted, database_stats.tup_updated, \
         database_stats.blks_read, database_stats.blks_hit, database_stats.temp_bytes, \
         wal_stats.wal_bytes::BIGINT AS wal_bytes \
         FROM pg_stat_database AS database_stats CROSS JOIN pg_stat_wal AS wal_stats \
         WHERE database_stats.datname = current_database()",
    )
    .fetch_one(&pool)
    .await
    .map_err(performance_report::debug_error)?;
    pool.close().await;
    const METRICS: [(&str, &str); 9] = [
        ("xact_commit", "operations"),
        ("tup_returned", "rows"),
        ("tup_fetched", "rows"),
        ("tup_inserted", "rows"),
        ("tup_updated", "rows"),
        ("blks_read", "blocks"),
        ("blks_hit", "blocks"),
        ("temp_bytes", "bytes"),
        ("wal_bytes", "bytes"),
    ];
    let values = METRICS
        .into_iter()
        .map(|(metric, unit)| {
            row.try_get::<i64, _>(metric)
                .map_err(performance_report::debug_error)
                .and_then(|value| u64::try_from(value).map_err(performance_report::debug_error))
                .map(|value| DatabaseMetricValue {
                    metric,
                    unit,
                    value,
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(DatabaseMetricSnapshot::new("postgres", values))
}

async fn capture_mssql_metrics(url: &str) -> Result<DatabaseMetricSnapshot, String> {
    let manager =
        bb8_tiberius::ConnectionManager::build(url).map_err(performance_report::debug_error)?;
    let pool = bb8::Pool::builder()
        .max_size(1)
        .build(manager)
        .await
        .map_err(performance_report::debug_error)?;
    let mut connection = pool.get().await.map_err(performance_report::debug_error)?;
    let row = connection
        .simple_query(
            "SELECT SUM(num_of_reads) AS read_operations, \
                    SUM(num_of_bytes_read) AS read_bytes, \
                    SUM(io_stall_read_ms) AS read_stall_ms, \
                    SUM(num_of_writes) AS write_operations, \
                    SUM(num_of_bytes_written) AS write_bytes, \
                    SUM(io_stall_write_ms) AS write_stall_ms \
             FROM sys.dm_io_virtual_file_stats(DB_ID(), NULL)",
        )
        .await
        .map_err(performance_report::debug_error)?
        .into_row()
        .await
        .map_err(performance_report::debug_error)?
        .ok_or_else(|| "SQL Server did not return virtual file statistics".to_owned())?;
    const METRICS: [(&str, &str); 6] = [
        ("read_operations", "operations"),
        ("read_bytes", "bytes"),
        ("read_stall_ms", "milliseconds"),
        ("write_operations", "operations"),
        ("write_bytes", "bytes"),
        ("write_stall_ms", "milliseconds"),
    ];
    let values = METRICS
        .into_iter()
        .map(|(metric, unit)| {
            row.try_get::<i64, _>(metric)
                .map_err(performance_report::debug_error)
                .and_then(|value| {
                    value.ok_or_else(|| {
                        format!("SQL Server returned NULL for virtual file metric {metric}")
                    })
                })
                .and_then(|value| u64::try_from(value).map_err(performance_report::debug_error))
                .map(|value| DatabaseMetricValue {
                    metric,
                    unit,
                    value,
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(DatabaseMetricSnapshot::new("mssql", values))
}

/// Returns a service URL when configured, or `None` to let a local run skip that backend.
///
/// CI sets `CATGA_REQUIRE_EXTERNAL_SERVICES`, which turns an absent URL into an error so the
/// full profile never silently drops a backend. Local runs without that variable measure only
/// the backends whose URLs are present (SQLite always runs because it needs no service).
fn optional_service_url(name: &str) -> Result<Option<String>, String> {
    match env::var(name) {
        Ok(url) if !url.trim().is_empty() => Ok(Some(url)),
        _ if env::var_os("CATGA_REQUIRE_EXTERNAL_SERVICES").is_some() => Err(format!(
            "{name} must be configured when CI executes storage performance tests"
        )),
        _ => Ok(None),
    }
}

fn suffix(label: &str) -> String {
    let nanoseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let sequence = UNIQUE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{label}-{}-{nanoseconds}-{sequence}", std::process::id())
}
