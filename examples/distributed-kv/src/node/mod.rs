//! Node wiring: raft runtime bootstrap, mediator wiring, and shared types.
//!
//! The KV application (state machine, routes, bench) depends only on the
//! `catga-core` consensus traits. This module wires
//! [`CatgaRaftRuntimeBuilder`](catga_raft::CatgaRaftRuntimeBuilder) (catga-raft
//! multi-Raft over gRPC, forwarding inside its transport). The client-facing
//! surface has two flavors with identical semantics: the HTTP/JSON routes and
//! the gRPC fast path (`kv_grpc`).

pub(crate) mod kv_grpc;
mod raft_backend;

use std::sync::Arc;

use catga_axum::axum::{Json, Router, extract::{Path, State}, http::StatusCode, routing::get};
use catga_core::{CatgaError, CatgaResult, ErrorCode, Mediator};
use catga_raft::CatgaRaftRuntime;
use serde::Serialize;

use crate::messages::KvService;
use crate::state::SharedState;
use crate::{Args, K8sTopology};

/// Shared HTTP handler state.
#[derive(Clone)]
pub(crate) struct ApiState {
    pub service: Arc<KvService>,
    pub state: Arc<SharedState>,
}

#[derive(Serialize)]
struct KvEntry { key: String, value: String }

/// Maps a Catga failure to the KV API's HTTP error response.
///
/// Status selection delegates to the canonical `ErrorCode::http_status_u16`
/// table — the same one `catga_axum::CatgaHttpError` uses — instead of
/// re-matching variants here. The body stays plain text (the error message):
/// `CatgaHttpError` responds with JSON, which would change what this API's
/// clients see.
pub(crate) fn error_response(error: CatgaError) -> (StatusCode, String) {
    let status = StatusCode::from_u16(error.code().http_status_u16())
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (status, error.to_string())
}

/// How long a linearizable read waits for the state machine to catch up to
/// the ReadIndex commit point.
const READ_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

async fn get_kv(State(app): State<ApiState>, Path(key): Path<String>) -> Result<Json<KvEntry>, StatusCode> {
    // Linearizable read (TiKV-style ReadIndex) via the runtime barrier:
    // quorum-checked commit index, then wait for local apply, then read.
    app.service.read_barrier(READ_WAIT_TIMEOUT).await.map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    app.state.get(&key).map(|value| Json(KvEntry { key, value })).ok_or(StatusCode::NOT_FOUND)
}

pub(crate) fn read_route() -> Router<ApiState> { Router::new().route("/kv/{key}", get(get_kv)) }

/// Which client stack the bench drives against the node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BenchMode {
    /// HTTP/JSON API (`POST /kv`, `POST /kv/batch`).
    Http,
    /// gRPC fast path (`Kv.Put`, `Kv.PutBatch`).
    Grpc,
}

/// Measures write throughput against one node with a keep-alive client.
///
/// With `concurrency > 1`, that many tasks each post their sequential share
/// of the writes (one connection per task, reused across its requests) and
/// one summary covers the run. With `batch > 1`, each request posts `batch`
/// keys (the last request carries the remainder), so recorded latencies are
/// per request while throughput is reported in keys/s. `mode` selects the
/// transport; both modes use the same key scheme and summary format.
pub(crate) async fn bench(addr: &str, writes: u32, concurrency: usize, batch: usize, mode: BenchMode) -> CatgaResult<()> {
    let started = std::time::Instant::now();
    let mut latencies = match mode {
        BenchMode::Http => bench_http(addr, writes, concurrency, batch).await?,
        BenchMode::Grpc => bench_grpc(addr, writes, concurrency, batch).await?,
    };
    let total_time = started.elapsed();
    latencies.sort_unstable();
    let pick = |p: f64| latencies.get((latencies.len().saturating_sub(1) as f64 * p) as usize).copied().unwrap_or_default();
    let mean = total_time / latencies.len().max(1) as u32;
    let throughput = f64::from(writes) / total_time.as_secs_f64();
    if batch == 1 {
        println!("writes={writes} total={total_time:.2?} throughput={throughput:.0}/s mean={mean:.2?} p50={:.2?} p99={:.2?}", pick(0.50), pick(0.99));
    } else {
        println!("writes={writes} keys_per_req={batch} total={total_time:.2?} throughput={throughput:.0} keys/s mean={mean:.2?} p50={:.2?} p99={:.2?}", pick(0.50), pick(0.99));
    }
    Ok(())
}

/// HTTP/JSON bench worker.
async fn bench_http(addr: &str, writes: u32, concurrency: usize, batch: usize) -> CatgaResult<Vec<std::time::Duration>> {
    let client = reqwest::Client::new();
    let single_url = format!("http://{addr}/kv");
    let batch_url = format!("http://{addr}/kv/batch");
    let total = writes as usize;
    let mut latencies = Vec::with_capacity(total.div_ceil(batch));
    if concurrency <= 1 {
        for request in 0..total.div_ceil(batch) {
            let start = request * batch;
            let end = (start + batch).min(total);
            let (url, body) = if batch == 1 {
                (single_url.as_str(), format!(r#"{{"key":"bench-{start}","value":"v{start}"}}"#))
            } else {
                let items: Vec<String> = (start..end)
                    .map(|index| format!(r#"{{"key":"bench-{request}-{}","value":"v{index}"}}"#, index - start))
                    .collect();
                (batch_url.as_str(), format!(r#"{{"items":[{}]}}"#, items.join(",")))
            };
            let one = std::time::Instant::now();
            let response = client.post(url).header(reqwest::header::CONTENT_TYPE, "application/json").body(body).send().await.map_err(map_err)?;
            if !response.status().is_success() {
                return Err(CatgaError::new(ErrorCode::Unavailable, format!("bench request {request} failed with HTTP {}", response.status())));
            }
            latencies.push(one.elapsed());
        }
    } else {
        let tasks = concurrency.min(total.max(1));
        let share = total / tasks;
        let remainder = total % tasks;
        let mut handles = Vec::with_capacity(tasks);
        let mut next_index = 0usize;
        for task in 0..tasks {
            let count = share + usize::from(task < remainder);
            let start_index = next_index;
            next_index += count;
            // Clone shares the same connection pool, keeping keep-alive across tasks.
            let client = client.clone();
            let single_url = single_url.clone();
            let batch_url = batch_url.clone();
            handles.push(tokio::spawn(async move {
                let mut task_latencies = Vec::with_capacity(count.div_ceil(batch));
                let mut index = start_index;
                let mut request = 0usize;
                while index < start_index + count {
                    let end = (index + batch).min(start_index + count);
                    let (url, body) = if batch == 1 {
                        let offset = index - start_index;
                        (single_url.as_str(), format!(r#"{{"key":"bench-{task}-{offset}","value":"v{index}"}}"#))
                    } else {
                        let items: Vec<String> = (index..end)
                            .map(|key_index| format!(r#"{{"key":"bench-{task}-{request}-{}","value":"v{key_index}"}}"#, key_index - index))
                            .collect();
                        (batch_url.as_str(), format!(r#"{{"items":[{}]}}"#, items.join(",")))
                    };
                    let one = std::time::Instant::now();
                    let response = client.post(url).header(reqwest::header::CONTENT_TYPE, "application/json").body(body).send().await.map_err(map_err)?;
                    if !response.status().is_success() {
                        return Err(CatgaError::new(ErrorCode::Unavailable, format!("bench request {request} (keys {index}..{end}) failed with HTTP {}", response.status())));
                    }
                    task_latencies.push(one.elapsed());
                    index = end;
                    request += 1;
                }
                Ok::<_, CatgaError>(task_latencies)
            }));
        }
        let mut first_error = None;
        for handle in handles {
            match handle.await {
                Ok(Ok(mut task_latencies)) => latencies.append(&mut task_latencies),
                Ok(Err(error)) => first_error = first_error.or(Some(error)),
                Err(join_error) => first_error = first_error.or(Some(map_err(join_error))),
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
    }
    Ok(latencies)
}

/// gRPC bench worker: one tonic channel per worker task (connected once,
/// reused for all of that task's RPCs), `PutBatch` when `batch > 1` else
/// `Put`, same key scheme as [`bench_http`].
async fn bench_grpc(addr: &str, writes: u32, concurrency: usize, batch: usize) -> CatgaResult<Vec<std::time::Duration>> {
    use kv_grpc::proto::kv_client::KvClient;
    use kv_grpc::proto::{PutBatchRequest, PutRequest};
    use tonic::Request;

    fn rpc_error(context: impl std::fmt::Display, error: impl std::fmt::Display) -> CatgaError {
        CatgaError::new(ErrorCode::Unavailable, format!("bench grpc {context}: {error}"))
    }

    // The gRPC path proposes faster than HTTP, so it can outrun the raft
    // pipeline's inflight window; the runtime answers with an explicitly
    // retryable backpressure rejection. Retry those (real clients back off
    // the same way); bench keys are idempotent overwrites, so retrying a
    // partially proposed batch cannot corrupt the dataset.
    const BACKPRESSURE_RETRIES: u32 = 200;
    const BACKPRESSURE_DELAY: std::time::Duration = std::time::Duration::from_millis(5);
    fn is_backpressure(status: &tonic::Status) -> bool {
        status.code() == tonic::Code::Unavailable && status.message().contains("backpressure")
    }
    async fn put_with_retry(client: &mut KvClient<tonic::transport::Channel>, item: PutRequest) -> Result<(), tonic::Status> {
        let mut attempt = 0u32;
        loop {
            match client.put(Request::new(item.clone())).await {
                Ok(_) => return Ok(()),
                Err(status) if is_backpressure(&status) && attempt < BACKPRESSURE_RETRIES => {
                    attempt += 1;
                    tokio::time::sleep(BACKPRESSURE_DELAY).await;
                }
                Err(status) => return Err(status),
            }
        }
    }
    async fn put_batch_with_retry(client: &mut KvClient<tonic::transport::Channel>, items: PutBatchRequest) -> Result<(), tonic::Status> {
        let mut attempt = 0u32;
        loop {
            match client.put_batch(Request::new(items.clone())).await {
                Ok(_) => return Ok(()),
                Err(status) if is_backpressure(&status) && attempt < BACKPRESSURE_RETRIES => {
                    attempt += 1;
                    tokio::time::sleep(BACKPRESSURE_DELAY).await;
                }
                Err(status) => return Err(status),
            }
        }
    }

    let endpoint = format!("http://{addr}");
    let total = writes as usize;
    let mut latencies = Vec::with_capacity(total.div_ceil(batch));
    if concurrency <= 1 {
        let mut client = KvClient::connect(endpoint).await.map_err(|error| rpc_error(format_args!("connect to {addr}"), error))?;
        for request in 0..total.div_ceil(batch) {
            let start = request * batch;
            let end = (start + batch).min(total);
            let one = std::time::Instant::now();
            if batch == 1 {
                put_with_retry(&mut client, PutRequest { key: format!("bench-{start}"), value: format!("v{start}") })
                    .await
                    .map_err(|error| rpc_error(format_args!("request {request}"), error))?;
            } else {
                let items: Vec<PutRequest> = (start..end)
                    .map(|index| PutRequest { key: format!("bench-{request}-{}", index - start), value: format!("v{index}") })
                    .collect();
                put_batch_with_retry(&mut client, PutBatchRequest { items })
                    .await
                    .map_err(|error| rpc_error(format_args!("request {request}"), error))?;
            }
            latencies.push(one.elapsed());
        }
    } else {
        let tasks = concurrency.min(total.max(1));
        let share = total / tasks;
        let remainder = total % tasks;
        let mut handles = Vec::with_capacity(tasks);
        let mut next_index = 0usize;
        for task in 0..tasks {
            let count = share + usize::from(task < remainder);
            let start_index = next_index;
            next_index += count;
            // One channel per worker task: connected once, reused for every RPC.
            let endpoint = endpoint.clone();
            handles.push(tokio::spawn(async move {
                let mut client = KvClient::connect(endpoint).await.map_err(|error| rpc_error("connect", error))?;
                let mut task_latencies = Vec::with_capacity(count.div_ceil(batch));
                let mut index = start_index;
                let mut request = 0usize;
                while index < start_index + count {
                    let end = (index + batch).min(start_index + count);
                    let one = std::time::Instant::now();
                    if batch == 1 {
                        let offset = index - start_index;
                        put_with_retry(&mut client, PutRequest { key: format!("bench-{task}-{offset}"), value: format!("v{index}") })
                            .await
                            .map_err(|error| rpc_error(format_args!("request {request} (keys {index}..{end})"), error))?;
                    } else {
                        let items: Vec<PutRequest> = (index..end)
                            .map(|key_index| PutRequest { key: format!("bench-{task}-{request}-{}", key_index - index), value: format!("v{key_index}") })
                            .collect();
                        put_batch_with_retry(&mut client, PutBatchRequest { items })
                            .await
                            .map_err(|error| rpc_error(format_args!("request {request} (keys {index}..{end})"), error))?;
                    }
                    task_latencies.push(one.elapsed());
                    index = end;
                    request += 1;
                }
                Ok::<_, CatgaError>(task_latencies)
            }));
        }
        let mut first_error = None;
        for handle in handles {
            match handle.await {
                Ok(Ok(mut task_latencies)) => latencies.append(&mut task_latencies),
                Ok(Err(error)) => first_error = first_error.or(Some(error)),
                Err(join_error) => first_error = first_error.or(Some(map_err(join_error))),
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
    }
    Ok(latencies)
}

/// Builds the HTTP handler state and the mediator that owns the registered
/// `PutKv` write handler; the caller keeps the mediator alive.
pub(crate) fn api_and_mediator(
    runtime: Arc<CatgaRaftRuntime<crate::state::KvMachine>>,
    state: Arc<SharedState>,
    node_id: u64,
) -> CatgaResult<(ApiState, Arc<Mediator>)> {
    let service = Arc::new(KvService::new(Arc::clone(&runtime), node_id, Arc::clone(&state)));
    let mediator = Arc::new(Mediator::new(service.registry()?));
    let api = ApiState { service, state };
    Ok((api, mediator))
}

/// Runs one KV node until SIGINT/SIGTERM: consensus, HTTP API, using catga-raft.
pub(crate) async fn run(args: Args, k8s: Option<K8sTopology>) -> CatgaResult<()> {
    let data_dir = std::env::var("KV_DATA_DIR").unwrap_or_else(|_| match &k8s {
        Some(_) => "/data".to_string(),
        None => format!("kv-data-{}", args.node),
    });
    let state = Arc::new(SharedState::new(std::path::Path::new(&data_dir).join("kv.redb"))?);
    let raft_dir = std::path::Path::new(&data_dir).join("raft");
    raft_backend::run_raft(&args, k8s, state, raft_dir).await
}

fn map_err(error: impl std::fmt::Display) -> CatgaError { CatgaError::new(ErrorCode::Internal, error.to_string()) }
