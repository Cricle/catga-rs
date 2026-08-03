//! Shared server-SQL timeout leasing implementation.

macro_rules! define_server_timeout {
    ($pool:ty, $database:ty, $postgres:expr, $label:literal) => {
        use std::time::Duration;
        use catga_core::{CatgaError, CatgaResult, ErrorCode};
        use catga_core::flow::{TimedOutFlowPoll, TimedOutFlowReceipt};
        use sqlx::Row;
        use uuid::Uuid;
        use crate::{error::database_error, key::flow_key, sql_backend::{cas_error, statement, unix_millis}};

        const RECEIPT_LEASE: Duration = Duration::from_secs(30);

        /// Leases at most `poll.limit()` due rows after inspecting no more than `scan_limit`.
        pub(crate) async fn poll(pool: &$pool, poll: &TimedOutFlowPoll) -> CatgaResult<Vec<TimedOutFlowReceipt>> {
            let now = unix_millis(poll.now())?;
            let lease = i64::try_from(RECEIPT_LEASE.as_millis()).map_err(|_| CatgaError::new(ErrorCode::Internal, concat!($label, " timeout receipt lease exceeds signed milliseconds")))?;
            let lease_until = now.checked_add(lease).ok_or_else(|| CatgaError::new(ErrorCode::Validation, concat!($label, " timeout receipt deadline overflows")))?;
            let candidate_limit = poll.limit().min(poll.scan_limit());
            let scan = i64::try_from(candidate_limit).map_err(|_| CatgaError::new(ErrorCode::Validation, "timeout poll candidate limit exceeds i64"))?;
            let mut tx = pool.begin().await.map_err(|error| database_error(concat!("begin ", $label, " timeout poll"), error))?;
            let rows = sqlx::query(statement("SELECT flow_key, flow_id FROM catga_flow_continuations WHERE deadline_ms IS NOT NULL AND deadline_ms <= ? AND (due_token IS NULL OR lease_until_ms IS NULL OR lease_until_ms <= ?) ORDER BY deadline_ms ASC, flow_key ASC LIMIT ? FOR UPDATE SKIP LOCKED", $postgres))
                .bind(now).bind(now).bind(scan).fetch_all(&mut *tx).await.map_err(|error| database_error(concat!("find due ", $label, " continuations"), error))?;
            let mut candidates = Vec::with_capacity(rows.len());
            for row in rows {
                let key: Vec<u8> = row.try_get("flow_key").map_err(|error| database_error(concat!("decode due ", $label, " key"), error))?;
                let id: String = row.try_get("flow_id").map_err(|error| database_error(concat!("decode due ", $label, " identity"), error))?;
                candidates.push((key, id));
            }
            if candidates.is_empty() {
                tx.commit().await.map_err(|error| database_error(concat!("commit empty ", $label, " timeout poll"), error))?;
                return Ok(Vec::new());
            }

            let token = Uuid::new_v4().into_bytes();
            let mut update = sqlx::QueryBuilder::<$database>::new(
                "UPDATE catga_flow_continuations SET due_token = ",
            );
            update
                .push_bind(token.as_slice())
                .push(", lease_until_ms = ")
                .push_bind(lease_until)
                .push(", revision = revision + 1 WHERE deadline_ms IS NOT NULL AND deadline_ms <= ")
                .push_bind(now)
                .push(" AND (due_token IS NULL OR lease_until_ms IS NULL OR lease_until_ms <= ")
                .push_bind(now)
                .push(") AND flow_key IN (");
            {
                let mut keys = update.separated(", ");
                for (key, _) in &candidates {
                    keys.push_bind(key.as_slice());
                }
            }
            update.push(")");
            let update = update.build().persistent(false);
            let changed = update.execute(&mut *tx).await.map_err(|error| database_error(concat!("lease ", $label, " timeout receipts"), error))?;
            if usize::try_from(changed.rows_affected()).ok() != Some(candidates.len()) {
                return Err(cas_error(concat!("lease ", $label, " timeout receipts")));
            }
            tx.commit().await.map_err(|error| database_error(concat!("commit ", $label, " timeout poll"), error))?;
            Ok(candidates
                .into_iter()
                .map(|(_, id)| TimedOutFlowReceipt::new(id, token))
                .collect())
        }

        /// Acknowledges only the currently fenced receipt token.
        pub(crate) async fn acknowledge(pool: &$pool, receipt: &TimedOutFlowReceipt) -> CatgaResult<()> {
            validate_token(receipt)?; let key = flow_key(receipt.flow_id());
            sqlx::query(statement("UPDATE catga_flow_continuations SET deadline_ms = NULL, due_token = NULL, lease_until_ms = NULL, revision = revision + 1 WHERE flow_key = ? AND flow_id = ? AND due_token = ?", $postgres))
                .bind(key.as_slice()).bind(receipt.flow_id()).bind(receipt.token()).execute(pool).await.map(|_| ()).map_err(|error| database_error(concat!("acknowledge ", $label, " timeout receipt"), error))
        }

        /// Releases only the currently fenced receipt token.
        pub(crate) async fn release(pool: &$pool, receipt: &TimedOutFlowReceipt) -> CatgaResult<()> {
            validate_token(receipt)?; let key = flow_key(receipt.flow_id());
            sqlx::query(statement("UPDATE catga_flow_continuations SET due_token = NULL, lease_until_ms = NULL, revision = revision + 1 WHERE flow_key = ? AND flow_id = ? AND due_token = ?", $postgres))
                .bind(key.as_slice()).bind(receipt.flow_id()).bind(receipt.token()).execute(pool).await.map(|_| ()).map_err(|error| database_error(concat!("release ", $label, " timeout receipt"), error))
        }

        fn validate_token(receipt: &TimedOutFlowReceipt) -> CatgaResult<()> {
            if receipt.token().len() == 16 { Ok(()) } else { Err(CatgaError::new(ErrorCode::Validation, concat!($label, " timeout receipt token is invalid"))) }
        }
    };
}

pub(crate) use define_server_timeout;
