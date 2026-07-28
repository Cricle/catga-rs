//! Shared SQLx server-dialect operations for durable state-machine snapshots.

macro_rules! define_server_state_machine {
    ($pool:ty, $postgres:expr, $label:literal) => {
        use catga_core::{CatgaError, CatgaResult, ErrorCode, SnapshotCodec};
        use catga_flow::StateMachineSnapshot;
        use sqlx::Row;

        use crate::{
            error::{database_error, is_mysql_duplicate_key},
            key::flow_key,
            sql_backend::{cas_error, statement, MAX_CAS_RETRIES},
            state_machine_codec::{decode, encode},
        };

        struct StoredSnapshot<S> {
            snapshot: StateMachineSnapshot<S>,
            revision: i64,
        }

        /// Inserts a state-machine snapshot and collision-checks its raw instance identity.
        pub(crate) async fn create<S, C>(
            pool: &$pool,
            snapshot: StateMachineSnapshot<S>,
            codec: &C,
        ) -> CatgaResult<bool>
        where
            C: SnapshotCodec<S>,
        {
            let key = flow_key(snapshot.instance_id());
            let insert = if $postgres {
                "INSERT INTO catga_state_machine_snapshots (instance_key, instance_id, version, revision, payload) VALUES (?, ?, ?, 0, ?) ON CONFLICT(instance_key) DO NOTHING"
            } else {
                "INSERT INTO catga_state_machine_snapshots (instance_key, instance_id, version, revision, payload) VALUES (?, ?, ?, 0, ?)"
            };
            let result = sqlx::query(statement(insert, $postgres))
                .bind(key.as_slice())
                .bind(snapshot.instance_id())
                .bind(snapshot.version())
                .bind(encode(&snapshot, codec)?)
                .execute(pool)
                .await;
            let created = match result {
                Ok(result) => result.rows_affected() == 1,
                Err(error) if !$postgres && is_mysql_duplicate_key(&error) => false,
                Err(error) => return Err(database_error(concat!("create ", $label, " state-machine snapshot"), error)),
            };
            if created {
                return Ok(true);
            }
            let row = sqlx::query(statement(
                "SELECT instance_id FROM catga_state_machine_snapshots WHERE instance_key = ?",
                $postgres,
            ))
            .bind(key.as_slice())
            .fetch_optional(pool)
            .await
            .map_err(|error| database_error(concat!("read conflicting ", $label, " state-machine snapshot"), error))?
            .ok_or_else(|| CatgaError::new(ErrorCode::Transient, concat!($label, " state-machine snapshot disappeared after a conflicting create")))?;
            let existing: String = row
                .try_get("instance_id")
                .map_err(|error| database_error(concat!("decode ", $label, " state-machine identity"), error))?;
            if existing == snapshot.instance_id() {
                Ok(false)
            } else {
                Err(CatgaError::new(ErrorCode::Internal, "SHA-256 collision between SQL state-machine identities"))
            }
        }

        /// Loads a state-machine snapshot by its raw instance identity.
        pub(crate) async fn get<S, C>(
            pool: &$pool,
            instance_id: &str,
            codec: &C,
        ) -> CatgaResult<Option<StateMachineSnapshot<S>>>
        where
            C: SnapshotCodec<S>,
        {
            load(pool, instance_id, codec)
                .await
                .map(|stored| stored.map(|stored| stored.snapshot))
        }

        /// Replaces one logical version through bounded physical-revision compare-and-set retries.
        pub(crate) async fn update<S, C>(
            pool: &$pool,
            expected_version: i64,
            next: StateMachineSnapshot<S>,
            codec: &C,
        ) -> CatgaResult<bool>
        where
            C: SnapshotCodec<S>,
        {
            if expected_version.checked_add(1) != Some(next.version()) {
                return Ok(false);
            }
            for _ in 0..MAX_CAS_RETRIES {
                let Some(current) = load(pool, next.instance_id(), codec).await? else {
                    return Ok(false);
                };
                if current.snapshot.version() != expected_version {
                    return Ok(false);
                }
                if replace(pool, &current, &next, codec).await? {
                    return Ok(true);
                }
            }
            Err(cas_error(concat!("update ", $label, " state-machine snapshot")))
        }

        async fn load<S, C>(
            pool: &$pool,
            instance_id: &str,
            codec: &C,
        ) -> CatgaResult<Option<StoredSnapshot<S>>>
        where
            C: SnapshotCodec<S>,
        {
            let key = flow_key(instance_id);
            let row = sqlx::query(statement(
                "SELECT version, revision, payload FROM catga_state_machine_snapshots WHERE instance_key = ? AND instance_id = ?",
                $postgres,
            ))
            .bind(key.as_slice())
            .bind(instance_id)
            .fetch_optional(pool)
            .await
            .map_err(|error| database_error(concat!("read ", $label, " state-machine snapshot"), error))?;
            row.map(|row| {
                let version: i64 = row
                    .try_get("version")
                    .map_err(|error| database_error(concat!("decode ", $label, " state-machine version"), error))?;
                let revision: i64 = row
                    .try_get("revision")
                    .map_err(|error| database_error(concat!("decode ", $label, " state-machine revision"), error))?;
                let frame: Vec<u8> = row
                    .try_get("payload")
                    .map_err(|error| database_error(concat!("decode ", $label, " state-machine frame"), error))?;
                let snapshot = decode(instance_id, &frame, codec)?;
                if snapshot.instance_id() != instance_id || snapshot.version() != version {
                    return Err(CatgaError::new(ErrorCode::Internal, concat!($label, " state-machine row does not match its snapshot frame")));
                }
                Ok(StoredSnapshot { snapshot, revision })
            })
            .transpose()
        }

        async fn replace<S, C>(
            pool: &$pool,
            current: &StoredSnapshot<S>,
            next: &StateMachineSnapshot<S>,
            codec: &C,
        ) -> CatgaResult<bool>
        where
            C: SnapshotCodec<S>,
        {
            let key = flow_key(next.instance_id());
            let result = sqlx::query(statement(
                "UPDATE catga_state_machine_snapshots SET version = ?, payload = ?, revision = revision + 1 WHERE instance_key = ? AND instance_id = ? AND revision = ?",
                $postgres,
            ))
            .bind(next.version())
            .bind(encode(next, codec)?)
            .bind(key.as_slice())
            .bind(next.instance_id())
            .bind(current.revision)
            .execute(pool)
            .await
            .map_err(|error| database_error(concat!("replace ", $label, " state-machine snapshot"), error))?;
            Ok(result.rows_affected() == 1)
        }
    };
}

pub(crate) use define_server_state_machine;
