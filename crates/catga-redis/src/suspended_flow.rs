//! Redis binary continuation records with exact-value Lua CAS.

use std::time::SystemTime;

use async_trait::async_trait;
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use catga_flow::{
    FlowContinuation, FlowQuery, FlowState, FlowSummary, SuspendedFlowStore, TimedOutFlowPoll,
    TimedOutFlowReceipt, TimedOutFlowStore, decode_continuation, encode_continuation,
    flow_timeout_deadline_unix_ms,
};
use redis::{AsyncCommands, Script, aio::ConnectionManager};

use crate::{suspended_flow_timeout, transport::map_error};

const MAX_CAS_RETRIES: usize = 8;
const RECEIPT_LEASE_MILLIS: u64 = 30_000;

struct WaitCorrelationUpdate<'a> {
    previous: Option<&'a str>,
    next: Option<&'a str>,
}

/// Redis-backed suspended flow store using atomic exact-value compare-and-set.
pub struct RedisSuspendedFlows {
    connection: ConnectionManager,
    prefix: Box<str>,
}

impl RedisSuspendedFlows {
    /// Connects and stores each continuation as a compact binary Redis value beneath `prefix`.
    pub async fn connect(
        server: impl AsRef<str>,
        prefix: impl Into<Box<str>>,
    ) -> CatgaResult<Self> {
        let client = redis::Client::open(server.as_ref()).map_err(map_error)?;
        let connection = client
            .get_connection_manager_with_config(crate::config::command_connection_manager_config())
            .await
            .map_err(map_error)?;
        Ok(Self {
            connection,
            prefix: prefix.into(),
        })
    }

    fn key(&self, flow_id: &str) -> String {
        format!("{}:{flow_id}", self.prefix)
    }

    fn timeout_key(&self, suffix: &str) -> String {
        format!("{}.__timeout_{suffix}", self.prefix)
    }

    fn records_key(&self) -> String {
        format!("{}.__records", self.prefix)
    }

    fn wait_correlation_key(&self, correlation_id: &str) -> String {
        format!("{}.__wait_correlation:{correlation_id}", self.prefix)
    }

    async fn load_raw(&self, key: &str) -> CatgaResult<Option<Vec<u8>>> {
        let mut connection = self.connection.clone();
        connection.get(key).await.map_err(map_error)
    }

    async fn compare_and_set(
        &self,
        key: &str,
        expected: Vec<u8>,
        next: Vec<u8>,
        flow_id: &str,
        deadline: Option<u64>,
        correlations: WaitCorrelationUpdate<'_>,
    ) -> CatgaResult<bool> {
        let mut connection = self.connection.clone();
        let updated = Script::new(suspended_flow_timeout::COMPARE_AND_SET)
            .key(key)
            .key(self.timeout_key("due"))
            .key(self.timeout_key("inflight"))
            .key(self.timeout_key("receipts"))
            .key(self.wait_correlation_key(correlations.previous.unwrap_or_default()))
            .key(self.wait_correlation_key(correlations.next.unwrap_or_default()))
            .key(self.records_key())
            .arg(expected)
            .arg(next)
            .arg(flow_id)
            .arg(deadline.map_or_else(String::new, |value| value.to_string()))
            .arg(i64::from(correlations.previous.is_some()))
            .arg(i64::from(correlations.next.is_some()))
            .invoke_async::<i64>(&mut connection)
            .await
            .map_err(map_error)?;
        Ok(updated == 1)
    }

    async fn delete_if_equal(
        &self,
        key: &str,
        flow_id: &str,
        expected: Vec<u8>,
        correlation: Option<&str>,
    ) -> CatgaResult<bool> {
        let mut connection = self.connection.clone();
        let deleted = Script::new(suspended_flow_timeout::DELETE_IF_EQUAL)
            .key(key)
            .key(self.timeout_key("due"))
            .key(self.timeout_key("inflight"))
            .key(self.timeout_key("receipts"))
            .key(self.wait_correlation_key(correlation.unwrap_or_default()))
            .key(self.records_key())
            .arg(expected)
            .arg(flow_id)
            .arg(i64::from(correlation.is_some()))
            .invoke_async::<i64>(&mut connection)
            .await
            .map_err(map_error)?;
        Ok(deleted == 1)
    }

    async fn mutate<F>(&self, flow_id: &str, version: i64, transform: F) -> CatgaResult<bool>
    where
        F: Fn(&FlowContinuation) -> Option<FlowContinuation>,
    {
        let key = self.key(flow_id);
        for _ in 0..MAX_CAS_RETRIES {
            let Some(current_raw) = self.load_raw(&key).await? else {
                return Ok(false);
            };
            let current = decode_continuation(&current_raw)?;
            if current.state().version() != version {
                return Ok(false);
            }
            let Some(next) = transform(&current) else {
                return Ok(false);
            };
            if next == current {
                return Ok(true);
            }
            if self
                .compare_and_set(
                    &key,
                    current_raw,
                    encode_continuation(&next)?,
                    flow_id,
                    flow_timeout_deadline_unix_ms(&next)?,
                    WaitCorrelationUpdate {
                        previous: current.wait().map(|wait| wait.correlation_id()),
                        next: next.wait().map(|wait| wait.correlation_id()),
                    },
                )
                .await?
            {
                return Ok(true);
            }
        }
        Err(CatgaError::new(
            ErrorCode::Transient,
            "Redis suspended-flow compare-and-set did not stabilize",
        ))
    }
}

#[async_trait]
impl SuspendedFlowStore for RedisSuspendedFlows {
    async fn create(&self, continuation: FlowContinuation) -> CatgaResult<bool> {
        continuation.validate()?;
        let key = self.key(continuation.state().id());
        let mut connection = self.connection.clone();
        let inserted =
            Script::new(suspended_flow_timeout::CREATE)
                .key(key)
                .key(self.timeout_key("due"))
                .key(self.wait_correlation_key(
                    continuation.wait().map_or("", |wait| wait.correlation_id()),
                ))
                .key(self.records_key())
                .arg(encode_continuation(&continuation)?)
                .arg(continuation.state().id())
                .arg(
                    flow_timeout_deadline_unix_ms(&continuation)?
                        .map_or_else(String::new, |value| value.to_string()),
                )
                .arg(i64::from(continuation.wait().is_some()))
                .invoke_async::<i64>(&mut connection)
                .await
                .map_err(map_error)?;
        Ok(inserted == 1)
    }

    async fn get(&self, flow_id: &str) -> CatgaResult<Option<FlowContinuation>> {
        self.load_raw(&self.key(flow_id))
            .await?
            .map(|bytes| decode_continuation(&bytes))
            .transpose()
    }

    async fn get_by_wait_correlation(
        &self,
        correlation_id: &str,
    ) -> CatgaResult<Option<FlowContinuation>> {
        let mut connection = self.connection.clone();
        let flow_ids: Vec<String> = connection
            .zrange(self.wait_correlation_key(correlation_id), 0, 1)
            .await
            .map_err(map_error)?;
        if flow_ids.len() > 1 {
            return Err(CatgaError::new(
                ErrorCode::Conflict,
                "flow wait correlation identifies multiple active flows",
            ));
        }
        let Some(flow_id) = flow_ids.into_iter().next() else {
            return Ok(None);
        };
        Ok(self.get(&flow_id).await?.filter(|continuation| {
            continuation
                .wait()
                .is_some_and(|wait| wait.correlation_id() == correlation_id)
        }))
    }

    async fn query(&self, query: &FlowQuery) -> CatgaResult<Vec<FlowSummary>> {
        let mut connection = self.connection.clone();
        let stop = isize::try_from(query.max_scan().saturating_sub(1)).map_err(|_| {
            CatgaError::new(
                ErrorCode::Validation,
                "Redis continuation query scan limit exceeds isize",
            )
        })?;
        let flow_ids: Vec<String> = connection
            .zrange(self.records_key(), 0, stop)
            .await
            .map_err(map_error)?;
        if flow_ids.is_empty() {
            return Ok(Vec::new());
        }
        let keys = flow_ids
            .iter()
            .map(|flow_id| self.key(flow_id))
            .collect::<Vec<_>>();
        let raws: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
            .arg(keys)
            .query_async(&mut connection)
            .await
            .map_err(map_error)?;
        let mut summaries = Vec::with_capacity(query.max_results());
        for raw in raws {
            if summaries.len() == query.max_results() {
                break;
            }
            let Some(raw) = raw else {
                continue;
            };
            let continuation = decode_continuation(&raw)?;
            if query.matches(&continuation) {
                summaries.push(FlowSummary::from_continuation(&continuation));
            }
        }
        Ok(summaries)
    }

    async fn delete(&self, flow_id: &str, expected_version: i64) -> CatgaResult<bool> {
        let key = self.key(flow_id);
        for _ in 0..MAX_CAS_RETRIES {
            let Some(current_raw) = self.load_raw(&key).await? else {
                return Ok(false);
            };
            let current = decode_continuation(&current_raw)?;
            if current.state().version() != expected_version {
                return Ok(false);
            }
            if self
                .delete_if_equal(
                    &key,
                    flow_id,
                    current_raw,
                    current.wait().map(|wait| wait.correlation_id()),
                )
                .await?
            {
                return Ok(true);
            }
        }
        Err(CatgaError::new(
            ErrorCode::Transient,
            "Redis suspended-flow deletion compare-and-set did not stabilize",
        ))
    }

    async fn update(&self, expected_version: i64, next: FlowContinuation) -> CatgaResult<bool> {
        if !FlowState::is_next_version(expected_version, next.state().version()) {
            return Ok(false);
        }
        next.validate()?;
        self.mutate(next.state().id(), expected_version, |_| Some(next.clone()))
            .await
    }

    async fn claim(
        &self,
        expected: &FlowContinuation,
        next: FlowContinuation,
    ) -> CatgaResult<bool> {
        if next.state().id() != expected.state().id()
            || !FlowState::is_next_version(expected.state().version(), next.state().version())
        {
            return Ok(false);
        }
        next.validate()?;
        let key = self.key(expected.state().id());
        let expected_raw = encode_continuation(expected)?;
        let next_raw = encode_continuation(&next)?;
        for _ in 0..MAX_CAS_RETRIES {
            let Some(current_raw) = self.load_raw(&key).await? else {
                return Ok(false);
            };
            if current_raw != expected_raw {
                return Ok(false);
            }
            if self
                .compare_and_set(
                    &key,
                    current_raw,
                    next_raw.clone(),
                    next.state().id(),
                    flow_timeout_deadline_unix_ms(&next)?,
                    WaitCorrelationUpdate {
                        previous: expected.wait().map(|wait| wait.correlation_id()),
                        next: next.wait().map(|wait| wait.correlation_id()),
                    },
                )
                .await?
            {
                return Ok(true);
            }
        }
        Err(CatgaError::new(
            ErrorCode::Transient,
            "Redis suspended-flow claim compare-and-set did not stabilize",
        ))
    }

    async fn record_wait_success(
        &self,
        flow_id: &str,
        version: i64,
        child_id: &str,
        payload: Vec<u8>,
    ) -> CatgaResult<bool> {
        self.mutate(flow_id, version, |current| {
            current.wait().map(|wait| {
                current
                    .clone()
                    .with_wait(wait.record_success(child_id, payload.clone()))
            })
        })
        .await
    }

    async fn record_wait_failure(
        &self,
        flow_id: &str,
        version: i64,
        child_id: &str,
        error: CatgaError,
    ) -> CatgaResult<bool> {
        self.mutate(flow_id, version, |current| {
            current.wait().map(|wait| {
                current
                    .clone()
                    .with_wait(wait.record_failure(child_id, error.clone()))
            })
        })
        .await
    }

    async fn heartbeat(&self, flow_id: &str, owner: &str, version: i64) -> CatgaResult<bool> {
        self.mutate(flow_id, version, |current| {
            (current.state().owner() == Some(owner)).then(|| {
                current
                    .clone()
                    .with_state(current.state().clone().heartbeated_at(SystemTime::now()))
            })
        })
        .await
    }
}

#[async_trait]
impl TimedOutFlowStore for RedisSuspendedFlows {
    async fn poll_timed_out(
        &self,
        poll: &TimedOutFlowPoll,
    ) -> CatgaResult<Vec<TimedOutFlowReceipt>> {
        let mut connection = self.connection.clone();
        let now = system_time_unix_ms(poll.now())?;
        let receipt_expiry = now.checked_add(RECEIPT_LEASE_MILLIS).ok_or_else(|| {
            CatgaError::new(
                ErrorCode::Validation,
                "Redis timeout receipt deadline overflows",
            )
        })?;
        let values = Script::new(suspended_flow_timeout::POLL)
            .key(self.timeout_key("due"))
            .key(self.timeout_key("inflight"))
            .key(self.timeout_key("receipts"))
            .key(self.timeout_key("sequence"))
            .arg(&*self.prefix)
            .arg(now)
            .arg(poll.limit())
            .arg(poll.scan_limit())
            .arg(receipt_expiry)
            .invoke_async::<Vec<String>>(&mut connection)
            .await
            .map_err(map_error)?;
        if values.len() % 2 != 0 {
            return Err(CatgaError::new(
                ErrorCode::Internal,
                "Redis timeout poll returned a malformed receipt list",
            ));
        }
        Ok(values
            .chunks_exact(2)
            .map(|pair| TimedOutFlowReceipt::new(pair[0].as_str(), pair[1].as_bytes().to_vec()))
            .collect())
    }

    async fn ack_timed_out(&self, receipt: &TimedOutFlowReceipt) -> CatgaResult<()> {
        let token = std::str::from_utf8(receipt.token()).map_err(|_| {
            CatgaError::new(ErrorCode::Validation, "Redis timeout receipt is invalid")
        })?;
        let mut connection = self.connection.clone();
        Script::new(suspended_flow_timeout::ACK)
            .key(self.timeout_key("receipts"))
            .key(self.timeout_key("inflight"))
            .arg(receipt.flow_id())
            .arg(token)
            .invoke_async::<i64>(&mut connection)
            .await
            .map(|_| ())
            .map_err(map_error)
    }

    async fn release_timed_out(&self, receipt: &TimedOutFlowReceipt) -> CatgaResult<()> {
        let token = std::str::from_utf8(receipt.token()).map_err(|_| {
            CatgaError::new(ErrorCode::Validation, "Redis timeout receipt is invalid")
        })?;
        let mut connection = self.connection.clone();
        Script::new(suspended_flow_timeout::RELEASE)
            .key(self.timeout_key("receipts"))
            .key(self.timeout_key("inflight"))
            .key(self.timeout_key("due"))
            .arg(receipt.flow_id())
            .arg(token)
            .invoke_async::<i64>(&mut connection)
            .await
            .map(|_| ())
            .map_err(map_error)
    }
}

fn system_time_unix_ms(time: SystemTime) -> CatgaResult<u64> {
    let elapsed = time.duration_since(SystemTime::UNIX_EPOCH).map_err(|_| {
        CatgaError::new(
            ErrorCode::Validation,
            "Redis timeout poll precedes the Unix epoch",
        )
    })?;
    u64::try_from(elapsed.as_millis()).map_err(|_| {
        CatgaError::new(
            ErrorCode::Validation,
            "Redis timeout poll exceeds the supported millisecond range",
        )
    })
}
