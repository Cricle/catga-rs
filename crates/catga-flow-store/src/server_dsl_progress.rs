//! Shared SQLx server-dialect operations for durable DSL step progress.

macro_rules! define_server_dsl_progress {
    ($pool:ty, $postgres:expr, $label:literal) => {
        use catga_core::{CatgaError, CatgaResult, ErrorCode};
        use catga_core::flow::DslStepProgress;
        use sqlx::Row;

        use crate::{
            dsl_progress_codec::{advances_version, decode_progress, encode_progress, validate_progress},
            error::{database_error, is_mysql_duplicate_key},
            key::flow_key,
            sql_backend::{cas_error, statement, MAX_CAS_RETRIES},
        };

        struct StoredProgress {
            progress: DslStepProgress,
            revision: i64,
        }

        /// Inserts a progress snapshot and collision-checks its raw flow identity.
        pub(crate) async fn create(
            pool: &$pool,
            progress: DslStepProgress,
        ) -> CatgaResult<bool> {
            validate_progress(&progress)?;
            let key = flow_key(progress.flow_id());
            let step_index = i64::from(progress.step_index());
            let insert = if $postgres {
                "INSERT INTO catga_dsl_step_progress (flow_key, flow_id, step_index, version, revision, payload) VALUES (?, ?, ?, ?, 0, ?) ON CONFLICT(flow_key, step_index) DO NOTHING"
            } else {
                "INSERT INTO catga_dsl_step_progress (flow_key, flow_id, step_index, version, revision, payload) VALUES (?, ?, ?, ?, 0, ?)"
            };
            let result = sqlx::query(statement(insert, $postgres))
                .bind(key.as_slice())
                .bind(progress.flow_id())
                .bind(step_index)
                .bind(progress.version())
                .bind(encode_progress(&progress)?)
                .execute(pool)
                .await;
            let created = match result {
                Ok(result) => result.rows_affected() == 1,
                Err(error) if !$postgres && is_mysql_duplicate_key(&error) => false,
                Err(error) => return Err(database_error(concat!("create ", $label, " DSL step progress"), error)),
            };
            if created {
                return Ok(true);
            }
            let row = sqlx::query(statement(
                "SELECT flow_id FROM catga_dsl_step_progress WHERE flow_key = ? AND step_index = ?",
                $postgres,
            ))
            .bind(key.as_slice())
            .bind(step_index)
            .fetch_optional(pool)
            .await
            .map_err(|error| database_error(concat!("read conflicting ", $label, " DSL step progress"), error))?
            .ok_or_else(|| CatgaError::new(ErrorCode::Transient, concat!($label, " DSL step progress disappeared after a conflicting create")))?;
            let existing: String = row
                .try_get("flow_id")
                .map_err(|error| database_error(concat!("decode ", $label, " DSL progress identity"), error))?;
            if existing == progress.flow_id() {
                Ok(false)
            } else {
                Err(CatgaError::new(ErrorCode::Internal, "SHA-256 collision between SQL DSL progress identities"))
            }
        }

        /// Loads progress for one raw flow-step identity.
        pub(crate) async fn get(pool: &$pool, flow_id: &str, step_index: u32) -> CatgaResult<Option<DslStepProgress>> {
            load(pool, flow_id, step_index)
                .await
                .map(|stored| stored.map(|stored| stored.progress))
        }

        /// Replaces exactly one logical version through bounded physical-revision CAS retries.
        pub(crate) async fn update(pool: &$pool, expected_version: i64, next: DslStepProgress) -> CatgaResult<bool> {
            validate_progress(&next)?;
            if !advances_version(expected_version, next.version()) {
                return Ok(false);
            }
            for _ in 0..MAX_CAS_RETRIES {
                let Some(current) = load(pool, next.flow_id(), next.step_index()).await? else {
                    return Ok(false);
                };
                if current.progress.version() != expected_version {
                    return Ok(false);
                }
                if replace(pool, &current, &next).await? {
                    return Ok(true);
                }
            }
            Err(cas_error(concat!("update ", $label, " DSL step progress")))
        }

        /// Deletes one progress row through bounded physical-revision CAS retries.
        pub(crate) async fn delete(pool: &$pool, flow_id: &str, step_index: u32) -> CatgaResult<bool> {
            for _ in 0..MAX_CAS_RETRIES {
                let Some(current) = load(pool, flow_id, step_index).await? else {
                    return Ok(false);
                };
                let key = flow_key(flow_id);
                let result = sqlx::query(statement(
                    "DELETE FROM catga_dsl_step_progress WHERE flow_key = ? AND flow_id = ? AND step_index = ? AND revision = ?",
                    $postgres,
                ))
                .bind(key.as_slice())
                .bind(flow_id)
                .bind(i64::from(step_index))
                .bind(current.revision)
                .execute(pool)
                .await
                .map_err(|error| database_error(concat!("delete ", $label, " DSL step progress"), error))?;
                if result.rows_affected() == 1 {
                    return Ok(true);
                }
            }
            Err(cas_error(concat!("delete ", $label, " DSL step progress")))
        }

        async fn load(pool: &$pool, flow_id: &str, step_index: u32) -> CatgaResult<Option<StoredProgress>> {
            let key = flow_key(flow_id);
            let row = sqlx::query(statement(
                "SELECT version, revision, payload FROM catga_dsl_step_progress WHERE flow_key = ? AND flow_id = ? AND step_index = ?",
                $postgres,
            ))
            .bind(key.as_slice())
            .bind(flow_id)
            .bind(i64::from(step_index))
            .fetch_optional(pool)
            .await
            .map_err(|error| database_error(concat!("read ", $label, " DSL step progress"), error))?;
            row.map(|row| {
                let version: i64 = row
                    .try_get("version")
                    .map_err(|error| database_error(concat!("decode ", $label, " DSL progress version"), error))?;
                let revision: i64 = row
                    .try_get("revision")
                    .map_err(|error| database_error(concat!("decode ", $label, " DSL progress revision"), error))?;
                let frame: Vec<u8> = row
                    .try_get("payload")
                    .map_err(|error| database_error(concat!("decode ", $label, " DSL progress frame"), error))?;
                let progress = decode_progress(&frame)?;
                if progress.flow_id() != flow_id || progress.step_index() != step_index || progress.version() != version {
                    return Err(CatgaError::new(ErrorCode::Internal, concat!($label, " DSL step-progress row does not match its frame")));
                }
                Ok(StoredProgress { progress, revision })
            })
            .transpose()
        }

        async fn replace(pool: &$pool, current: &StoredProgress, next: &DslStepProgress) -> CatgaResult<bool> {
            let key = flow_key(next.flow_id());
            let result = sqlx::query(statement(
                "UPDATE catga_dsl_step_progress SET version = ?, payload = ?, revision = revision + 1 WHERE flow_key = ? AND flow_id = ? AND step_index = ? AND revision = ?",
                $postgres,
            ))
            .bind(next.version())
            .bind(encode_progress(next)?)
            .bind(key.as_slice())
            .bind(next.flow_id())
            .bind(i64::from(next.step_index()))
            .bind(current.revision)
            .execute(pool)
            .await
            .map_err(|error| database_error(concat!("replace ", $label, " DSL step progress"), error))?;
            Ok(result.rows_affected() == 1)
        }
    };
}

pub(crate) use define_server_dsl_progress;
