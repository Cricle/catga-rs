//! Shared server-SQL continuation operations.

macro_rules! define_server_suspended {
    ($pool:ty, $postgres:expr, $label:literal) => {
        use std::time::SystemTime;
        use catga_core::{CatgaError, CatgaResult, ErrorCode};
        use catga_flow::{FlowContinuation, FlowQuery, FlowSummary, decode_continuation, encode_continuation};
        use sqlx::Row;
        use crate::{error::{database_error, is_mysql_duplicate_key}, key::flow_key, sql_backend::{cas_error, deadline_millis, statement, status_code, status_from_code, system_time_from_unix_millis_and_subsec_nanos, unix_millis_and_subsec_nanos, MAX_CAS_RETRIES}};

        struct StoredContinuation { continuation: FlowContinuation, revision: i64 }

        /// Inserts one continuation and collision-checks its raw identity.
        pub(crate) async fn create(pool: &$pool, continuation: FlowContinuation) -> CatgaResult<bool> {
            continuation.validate()?;
            let key = flow_key(continuation.state().id());
            let (created_at_ms, created_at_subsec_ns) = unix_millis_and_subsec_nanos(continuation.created_at())?; let (updated_at_ms, updated_at_subsec_ns) = unix_millis_and_subsec_nanos(continuation.updated_at())?;
            let insert = if $postgres {
                "INSERT INTO catga_flow_continuations (flow_key, flow_id, flow_type, flow_type_key, status, version, created_at_ms, created_at_subsec_ns, updated_at_ms, updated_at_subsec_ns, deadline_ms, wait_correlation, wait_correlation_key, revision, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?) ON CONFLICT DO NOTHING"
            } else {
                "INSERT INTO catga_flow_continuations (flow_key, flow_id, flow_type, flow_type_key, status, version, created_at_ms, created_at_subsec_ns, updated_at_ms, updated_at_subsec_ns, deadline_ms, wait_correlation, wait_correlation_key, revision, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)"
            };
            let flow_type_key = flow_key(continuation.state().flow_type());
            let result = sqlx::query(statement(insert, $postgres)).bind(key.as_slice()).bind(continuation.state().id()).bind(continuation.state().flow_type()).bind(flow_type_key.as_slice()).bind(status_code(continuation.state().status())).bind(continuation.state().version()).bind(created_at_ms).bind(created_at_subsec_ns).bind(updated_at_ms).bind(updated_at_subsec_ns).bind(deadline_millis(&continuation)?).bind(wait_correlation(&continuation)).bind(wait_correlation_key(&continuation).map(|key| key.to_vec())).bind(encode_continuation(&continuation)?)
                .execute(pool).await;
            let created = match result { Ok(result) => result.rows_affected() == 1, Err(error) if !$postgres && is_mysql_duplicate_key(&error) => false, Err(error) => return Err(database_error(concat!("create ", $label, " continuation"), error)), };
            if created { return Ok(true); }
            let row = sqlx::query(statement("SELECT flow_id FROM catga_flow_continuations WHERE flow_key = ?", $postgres)).bind(key.as_slice()).fetch_optional(pool).await.map_err(|error| database_error(concat!("read conflicting ", $label, " continuation"), error))?;
            let Some(row) = row else { return Err(CatgaError::new(ErrorCode::Transient, concat!($label, " continuation disappeared after a conflicting create"))); };
            let existing: String = row.try_get("flow_id").map_err(|error| database_error(concat!("decode ", $label, " continuation identity"), error))?;
            if existing == continuation.state().id() { Ok(false) } else { Err(CatgaError::new(ErrorCode::Internal, "SHA-256 collision between SQL continuation identities")) }
        }

        /// Loads one validated continuation.
        pub(crate) async fn get(pool: &$pool, flow_id: &str) -> CatgaResult<Option<FlowContinuation>> { load(pool, flow_id).await.map(|value| value.map(|value| value.continuation)) }

        /// Loads exactly one continuation by its indexed active wait correlation.
        pub(crate) async fn get_by_wait_correlation(pool: &$pool, correlation_id: &str) -> CatgaResult<Option<FlowContinuation>> {
            let correlation_key = flow_key(correlation_id); let rows = sqlx::query(statement("SELECT payload FROM catga_flow_continuations WHERE wait_correlation_key = ? AND wait_correlation = ? ORDER BY flow_key ASC LIMIT 2", $postgres)).bind(correlation_key.as_slice()).bind(correlation_id).fetch_all(pool).await.map_err(|error| database_error(concat!("read ", $label, " wait correlation"), error))?;
            if rows.len() > 1 { return Err(CatgaError::new(ErrorCode::Conflict, "flow wait correlation identifies multiple active flows")); }
            rows.into_iter().next().map(|row| {
                let frame: Vec<u8> = row.try_get("payload").map_err(|error| database_error(concat!("decode ", $label, " wait correlation frame"), error))?;
                let continuation = decode_continuation(&frame)?;
                if continuation.wait().is_some_and(|wait| wait.correlation_id() == correlation_id) { Ok(continuation) } else { Err(CatgaError::new(ErrorCode::Internal, concat!($label, " wait correlation index does not match its continuation frame"))) }
            }).transpose()
        }

        /// Scans no more than the caller-requested number of compact summary rows.
        pub(crate) async fn query(pool: &$pool, query: &FlowQuery) -> CatgaResult<Vec<FlowSummary>> {
            let limit = i64::try_from(query.max_scan()).map_err(|_| CatgaError::new(ErrorCode::Validation, "continuation query scan limit exceeds i64"))?;
            let created_range = query.created_at_range().map(|(start, end)| {
                Ok::<_, CatgaError>((unix_millis_and_subsec_nanos(start)?, unix_millis_and_subsec_nanos(end)?))
            }).transpose()?;
            let mut template = String::from("SELECT flow_id, flow_type, status, version, created_at_ms, created_at_subsec_ns, updated_at_ms, updated_at_subsec_ns FROM catga_flow_continuations WHERE 1 = 1");
            if query.status().is_some() { template.push_str(" AND status = ?"); }
            if query.flow_type().is_some() { template.push_str(" AND flow_type_key = ? AND flow_type = ?"); }
            if created_range.is_some() { template.push_str(" AND (created_at_ms > ? OR (created_at_ms = ? AND created_at_subsec_ns >= ?)) AND (created_at_ms < ? OR (created_at_ms = ? AND created_at_subsec_ns < ?))"); }
            template.push_str(" ORDER BY created_at_ms ASC, created_at_subsec_ns ASC, flow_key ASC LIMIT ?");
            let template = statement(&template, $postgres).0.into_owned();
            let mut statement = sqlx::query(sqlx::AssertSqlSafe(template));
            if let Some(status) = query.status() { statement = statement.bind(status_code(status)); }
            if let Some(flow_type) = query.flow_type() { let flow_type_key = flow_key(flow_type); statement = statement.bind(flow_type_key.as_slice()).bind(flow_type); }
            if let Some(((start_ms, start_subsec_ns), (end_ms, end_subsec_ns))) = created_range { statement = statement.bind(start_ms).bind(start_ms).bind(start_subsec_ns).bind(end_ms).bind(end_ms).bind(end_subsec_ns); }
            let rows = statement.bind(limit).fetch_all(pool).await.map_err(|error| database_error(concat!("query ", $label, " continuations"), error))?;
            let mut summaries = Vec::with_capacity(query.max_results());
            for row in rows {
                let id: String = row.try_get("flow_id").map_err(|error| database_error(concat!("decode ", $label, " summary identity"), error))?;
                let flow_type: String = row.try_get("flow_type").map_err(|error| database_error(concat!("decode ", $label, " summary flow type"), error))?;
                let status: i64 = row.try_get("status").map_err(|error| database_error(concat!("decode ", $label, " summary status"), error))?;
                let version: i64 = row.try_get("version").map_err(|error| database_error(concat!("decode ", $label, " summary version"), error))?;
                let created_at: i64 = row.try_get("created_at_ms").map_err(|error| database_error(concat!("decode ", $label, " summary creation time"), error))?;
                let created_at_subsec_ns: i64 = row.try_get("created_at_subsec_ns").map_err(|error| database_error(concat!("decode ", $label, " summary creation precision"), error))?; let updated_at: i64 = row.try_get("updated_at_ms").map_err(|error| database_error(concat!("decode ", $label, " summary update time"), error))?; let updated_at_subsec_ns: i64 = row.try_get("updated_at_subsec_ns").map_err(|error| database_error(concat!("decode ", $label, " summary update precision"), error))?;
                let summary = FlowSummary::new(id, flow_type, status_from_code(status)?, version, system_time_from_unix_millis_and_subsec_nanos(created_at, created_at_subsec_ns)?).with_updated_at(system_time_from_unix_millis_and_subsec_nanos(updated_at, updated_at_subsec_ns)?);
                if query.matches_summary(&summary) {
                    summaries.push(summary);
                    if summaries.len() == query.max_results() { break; }
                }
            }
            Ok(summaries)
        }

        /// Deletes only a current business-version snapshot.
        pub(crate) async fn delete(pool: &$pool, flow_id: &str, expected_version: i64) -> CatgaResult<bool> {
            for _ in 0..MAX_CAS_RETRIES { let Some(current) = load(pool, flow_id).await? else { return Ok(false); }; if current.continuation.state().version() != expected_version { return Ok(false); } let key = flow_key(flow_id); let result = sqlx::query(statement("DELETE FROM catga_flow_continuations WHERE flow_key = ? AND flow_id = ? AND revision = ?", $postgres)).bind(key.as_slice()).bind(flow_id).bind(current.revision).execute(pool).await.map_err(|error| database_error(concat!("delete ", $label, " continuation"), error))?; if result.rows_affected() == 1 { return Ok(true); } }
            Err(cas_error(concat!("delete a ", $label, " continuation")))
        }

        /// Applies a single business-version continuation update.
        pub(crate) async fn update(pool: &$pool, expected_version: i64, next: FlowContinuation) -> CatgaResult<bool> {
            if !catga_flow::FlowState::is_next_version(expected_version, next.state().version()) { return Ok(false); }
            next.validate()?;
            for _ in 0..MAX_CAS_RETRIES { let Some(current) = load(pool, next.state().id()).await? else { return Ok(false); }; if current.continuation.state().version() != expected_version { return Ok(false); } if replace(pool, &current, &next).await? { return Ok(true); } }
            Err(cas_error(concat!("update a ", $label, " continuation")))
        }

        /// Requires byte-for-byte equality of the persisted expected snapshot before replacing it.
        pub(crate) async fn claim(pool: &$pool, expected: &FlowContinuation, next: FlowContinuation) -> CatgaResult<bool> {
            if next.state().id() != expected.state().id() || !catga_flow::FlowState::is_next_version(expected.state().version(), next.state().version()) { return Ok(false); }
            next.validate()?;
            let Some(current) = load(pool, expected.state().id()).await? else { return Ok(false); };
            if current.continuation != *expected { return Ok(false); }
            replace(pool, &current, &next).await
        }

        /// Stores one child success without changing the business version.
        pub(crate) async fn record_wait_success(pool: &$pool, flow_id: &str, version: i64, child_id: &str, payload: Vec<u8>) -> CatgaResult<bool> {
            for _ in 0..MAX_CAS_RETRIES { let Some(current) = load(pool, flow_id).await? else { return Ok(false); }; if current.continuation.state().version() != version { return Ok(false); } let Some(wait) = current.continuation.wait() else { return Ok(false); }; let next_wait = wait.record_success(child_id, payload.clone()); if next_wait.completed_count() == wait.completed_count() { return Ok(true); } let next = current.continuation.clone().with_wait(next_wait); if replace(pool, &current, &next).await? { return Ok(true); } }
            Err(cas_error(concat!("record a ", $label, " wait result")))
        }

        /// Stores one child failure without changing the business version.
        pub(crate) async fn record_wait_failure(pool: &$pool, flow_id: &str, version: i64, child_id: &str, error: CatgaError) -> CatgaResult<bool> {
            for _ in 0..MAX_CAS_RETRIES { let Some(current) = load(pool, flow_id).await? else { return Ok(false); }; if current.continuation.state().version() != version { return Ok(false); } let Some(wait) = current.continuation.wait() else { return Ok(false); }; let next_wait = wait.record_failure(child_id, error.clone()); if next_wait.completed_count() == wait.completed_count() { return Ok(true); } let next = current.continuation.clone().with_wait(next_wait); if replace(pool, &current, &next).await? { return Ok(true); } }
            Err(cas_error(concat!("record a failed ", $label, " wait result")))
        }

        /// Refreshes a fenced owner heartbeat and increments only physical revision.
        pub(crate) async fn heartbeat(pool: &$pool, flow_id: &str, owner: &str, version: i64) -> CatgaResult<bool> {
            for _ in 0..MAX_CAS_RETRIES { let Some(current) = load(pool, flow_id).await? else { return Ok(false); }; if current.continuation.state().owner() != Some(owner) || current.continuation.state().version() != version { return Ok(false); } let next = current.continuation.clone().with_state(current.continuation.state().clone().heartbeated_at(SystemTime::now())); if replace(pool, &current, &next).await? { return Ok(true); } }
            Err(cas_error(concat!("heartbeat a ", $label, " continuation")))
        }

        async fn load(pool: &$pool, flow_id: &str) -> CatgaResult<Option<StoredContinuation>> {
            let key = flow_key(flow_id); let row = sqlx::query(statement("SELECT payload, revision FROM catga_flow_continuations WHERE flow_key = ? AND flow_id = ?", $postgres)).bind(key.as_slice()).bind(flow_id).fetch_optional(pool).await.map_err(|error| database_error(concat!("read ", $label, " continuation"), error))?;
            row.map(|row| { let frame: Vec<u8> = row.try_get("payload").map_err(|error| database_error(concat!("decode ", $label, " continuation frame"), error))?; let continuation = decode_continuation(&frame)?; if continuation.state().id() != flow_id { return Err(CatgaError::new(ErrorCode::Internal, concat!($label, " continuation row identity does not match its frame"))); } let revision = row.try_get("revision").map_err(|error| database_error(concat!("decode ", $label, " continuation revision"), error))?; Ok(StoredContinuation { continuation, revision }) }).transpose()
        }

        async fn replace(pool: &$pool, current: &StoredContinuation, next: &FlowContinuation) -> CatgaResult<bool> {
            let key = flow_key(next.state().id()); let flow_type_key = flow_key(next.state().flow_type()); let (created_at_ms, created_at_subsec_ns) = unix_millis_and_subsec_nanos(next.created_at())?; let (updated_at_ms, updated_at_subsec_ns) = unix_millis_and_subsec_nanos(next.updated_at())?; let result = sqlx::query(statement("UPDATE catga_flow_continuations SET flow_type = ?, flow_type_key = ?, status = ?, version = ?, created_at_ms = ?, created_at_subsec_ns = ?, updated_at_ms = ?, updated_at_subsec_ns = ?, deadline_ms = ?, wait_correlation = ?, wait_correlation_key = ?, payload = ?, revision = revision + 1, due_token = NULL, lease_until_ms = NULL WHERE flow_key = ? AND flow_id = ? AND revision = ?", $postgres))
                .bind(next.state().flow_type()).bind(flow_type_key.as_slice()).bind(status_code(next.state().status())).bind(next.state().version()).bind(created_at_ms).bind(created_at_subsec_ns).bind(updated_at_ms).bind(updated_at_subsec_ns).bind(deadline_millis(next)?).bind(wait_correlation(next)).bind(wait_correlation_key(next).map(|key| key.to_vec())).bind(encode_continuation(next)?).bind(key.as_slice()).bind(next.state().id()).bind(current.revision).execute(pool).await.map_err(|error| database_error(concat!("replace ", $label, " continuation"), error))?;
            Ok(result.rows_affected() == 1)
        }

        fn wait_correlation(continuation: &FlowContinuation) -> Option<&str> { continuation.wait().map(|wait| wait.correlation_id()) }
        fn wait_correlation_key(continuation: &FlowContinuation) -> Option<[u8; 32]> { wait_correlation(continuation).map(flow_key) }
    };
}

pub(crate) use define_server_suspended;
