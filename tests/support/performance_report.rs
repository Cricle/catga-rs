//! Shared structured reporting helpers for manual performance integration tests.

use std::{path::PathBuf, time::Duration};

use serde::Serialize;

/// One reproducible performance measurement.
#[derive(Serialize)]
pub struct BenchmarkResult {
    /// Stable benchmark identifier.
    pub name: &'static str,
    /// Number of completed operations in the timed interval.
    pub operations: u64,
    /// Size of the logical payload, when the benchmark has one.
    pub payload_bytes: Option<usize>,
    /// Wall-clock time for the complete timed interval.
    pub elapsed_nanoseconds: u128,
    /// Completed operations per second.
    pub operations_per_second: f64,
    /// Nearest-rank operation latency percentile.
    pub p50_ns: u64,
    /// Nearest-rank operation latency percentile.
    pub p95_ns: u64,
    /// Nearest-rank operation latency percentile.
    pub p99_ns: u64,
    /// Meaning of a latency sample, such as `operation` or `batch`.
    pub latency_scope: &'static str,
    /// Process resident set size before the interval, when Linux exposes it.
    pub rss_before_bytes: Option<u64>,
    /// Process resident set size after the interval, when Linux exposes it.
    pub rss_after_bytes: Option<u64>,
    /// Process peak resident set size, when Linux exposes it.
    pub rss_peak_bytes: Option<u64>,
}

/// Machine-readable report emitted by one benchmark executable.
#[derive(Serialize)]
pub struct PerformanceReport {
    /// Format revision for artifact consumers.
    pub schema_version: u8,
    /// Human-readable benchmark source, including the storage backend when applicable.
    pub source: &'static str,
    /// Runtime environment and RSS provenance.
    pub environment: Environment,
    /// All measurements produced by this executable.
    pub results: Vec<BenchmarkResult>,
}

/// Environment metadata that makes host-only memory data unambiguous.
#[derive(Serialize)]
pub struct Environment {
    /// Operating system that ran this test executable.
    pub operating_system: &'static str,
    /// Source of the process RSS fields, or why they are absent.
    pub rss_source: &'static str,
}

/// Creates a report result from one set of operation latency samples.
pub fn measured(
    name: &'static str,
    payload_bytes: Option<usize>,
    elapsed: Duration,
    latencies: Vec<Duration>,
    latency_scope: &'static str,
    rss_before_bytes: Option<u64>,
) -> BenchmarkResult {
    let operations = u64::try_from(latencies.len()).expect("benchmark operations fit in u64");
    BenchmarkResult {
        name,
        operations,
        payload_bytes,
        elapsed_nanoseconds: elapsed.as_nanos(),
        operations_per_second: operations as f64 / elapsed.as_secs_f64(),
        p50_ns: percentile_nanoseconds(&latencies, 50),
        p95_ns: percentile_nanoseconds(&latencies, 95),
        p99_ns: percentile_nanoseconds(&latencies, 99),
        latency_scope,
        rss_before_bytes,
        rss_after_bytes: current_rss_bytes(),
        rss_peak_bytes: peak_rss_bytes(),
    }
}

/// Returns a Linux process RSS reading; unsupported platforms intentionally report no value.
pub fn current_rss_bytes() -> Option<u64> {
    proc_status_bytes("VmRSS:")
}

/// Returns a Linux process RSS high-water mark; unsupported platforms intentionally report no value.
pub fn peak_rss_bytes() -> Option<u64> {
    proc_status_bytes("VmHWM:")
}

/// Returns the metadata common to all reports produced by this helper.
pub const fn environment() -> Environment {
    Environment {
        operating_system: std::env::consts::OS,
        rss_source: "Linux /proc/self/status (VmRSS and VmHWM); null when unavailable",
    }
}

/// Writes the report requested by `CATGA_PERFORMANCE_RESULTS`, if configured.
pub fn write_report_if_configured(report: &PerformanceReport) -> Result<(), String> {
    let Some(path) = report_path() else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(debug_error)?;
    }
    let mut value = serde_json::to_value(report).map_err(debug_error)?;
    if path.exists() {
        let mut existing: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).map_err(debug_error)?)
                .map_err(debug_error)?;
        let existing_results = existing
            .get_mut("results")
            .and_then(serde_json::Value::as_array_mut)
            .ok_or_else(|| "existing performance report does not contain results".to_owned())?;
        let new_results = value
            .get_mut("results")
            .and_then(serde_json::Value::as_array_mut)
            .ok_or_else(|| "new performance report does not contain results".to_owned())?;
        existing_results.append(new_results);
        value = existing;
    }
    let serialized = serde_json::to_vec_pretty(&value).map_err(debug_error)?;
    std::fs::write(path, serialized).map_err(debug_error)
}

/// Uses nearest-rank percentile selection so a 100-sample p99 is sample 99.
pub fn percentile_nanoseconds(latencies: &[Duration], percentile: usize) -> u64 {
    assert!(!latencies.is_empty());
    assert!((1..=100).contains(&percentile));
    let mut nanoseconds = latencies
        .iter()
        .map(|latency| u64::try_from(latency.as_nanos()).unwrap_or(u64::MAX))
        .collect::<Vec<_>>();
    nanoseconds.sort_unstable();
    nanoseconds[(nanoseconds.len() * percentile)
        .div_ceil(100)
        .saturating_sub(1)]
}

/// Renders a debug error without making benchmark APIs depend on a concrete error type.
pub fn debug_error(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
}

fn proc_status_bytes(field: &str) -> Option<u64> {
    if !cfg!(target_os = "linux") {
        return None;
    }
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix(field))?
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()?
        .checked_mul(1024)
}

fn report_path() -> Option<PathBuf> {
    Some(PathBuf::from(std::env::var_os(
        "CATGA_PERFORMANCE_RESULTS",
    )?))
}
