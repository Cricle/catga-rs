//! Real-service cross-backend flow and transport coverage.

use std::{
    env,
    sync::OnceLock,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use catga_core::flow::{FlowState, FlowStore};
use catga_core::{CatgaError, CatgaResult, Envelope, ErrorCode, MessageMetadata, MessageTransport};
use catga_flow_store::SqlFlowStore;
use catga_nats::{NatsConfig, NatsTransport};
use catga_redis::{RedisConfig, RedisTransport};

static UNIQUE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static MSSQL_SCHEMA_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

#[tokio::test]
#[ignore = "requires real MySQL and Redis services"]
async fn mysql_flow_store_and_redis_transport_round_trip() -> CatgaResult<()> {
    let Some(mysql_url) = service_url("CATGA_MYSQL_URL")? else {
        return Ok(());
    };
    let Some(redis_url) = service_url("CATGA_REDIS_URL")? else {
        return Ok(());
    };
    let suffix = unique_suffix("mysql_redis");
    let flow_id = format!("cross-backend-flow-{suffix}");
    let store = SqlFlowStore::connect_mysql(&mysql_url).await?;
    store.migrate().await?;
    let state = FlowState::new(
        flow_id.as_str(),
        "cross-backend",
        b"mysql-redis".to_vec(),
        "e2e-node",
    );
    assert!(store.create(state.clone()).await?);
    let persisted = store.get(&flow_id).await?.ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Internal,
            "MySQL FlowStore did not retain the created flow",
        )
    })?;
    assert_eq!(persisted.id(), flow_id);
    assert_eq!(persisted.data(), b"mysql-redis");

    let transport = RedisTransport::connect(RedisConfig {
        server: redis_url.into(),
        stream: format!("catga:cross-backend:{suffix}").into(),
        group: format!("cross-backend:{suffix}").into(),
        consumer: format!("cross-backend:{suffix}").into(),
    })
    .await?;
    let id = message_id();
    let envelope = Envelope::new(
        id,
        "cross-backend.mysql.redis",
        b"mysql-redis".to_vec(),
        MessageMetadata::new(id, None),
    );
    transport.publish(envelope.clone()).await?;
    let delivery = receive_with_timeout(&transport).await?;
    assert_eq!(delivery.envelope().id(), envelope.id());
    assert_eq!(delivery.envelope().payload(), envelope.payload());
    transport.ack(delivery).await
}

#[tokio::test]
#[ignore = "requires real MySQL and NATS services"]
async fn mysql_flow_store_and_nats_transport_round_trip() -> CatgaResult<()> {
    let Some(mysql_url) = service_url("CATGA_MYSQL_URL")? else {
        return Ok(());
    };
    let Some(nats_url) = service_url("CATGA_NATS_URL")? else {
        return Ok(());
    };
    let suffix = unique_suffix("mysql_nats");
    let flow_id = format!("cross-backend-flow-{suffix}");
    let store = SqlFlowStore::connect_mysql(&mysql_url).await?;
    store.migrate().await?;
    let state = FlowState::new(
        flow_id.as_str(),
        "cross-backend",
        b"mysql-nats".to_vec(),
        "e2e-node",
    );
    assert!(store.create(state).await?);
    let persisted = store.get(&flow_id).await?.ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Internal,
            "MySQL FlowStore did not retain the created NATS-composed flow",
        )
    })?;
    assert_eq!(persisted.data(), b"mysql-nats");

    let transport = NatsTransport::connect(NatsConfig {
        server: nats_url.into(),
        stream: format!("CATGA_CROSS_BACKEND_{suffix}").into(),
        subject: format!("catga.cross.backend.{suffix}").into(),
        consumer: format!("cross_backend_{suffix}").into(),
    })
    .await?;
    let id = message_id();
    let envelope = Envelope::new(
        id,
        "cross-backend.mysql.nats",
        b"mysql-nats".to_vec(),
        MessageMetadata::new(id, None),
    );
    transport.publish(envelope.clone()).await?;
    let delivery = receive_with_timeout(&transport).await?;
    assert_eq!(delivery.envelope().id(), envelope.id());
    assert_eq!(delivery.envelope().payload(), envelope.payload());
    transport.ack(delivery).await
}

#[tokio::test]
#[ignore = "requires real PostgreSQL and NATS services"]
async fn postgres_flow_store_and_nats_transport_round_trip() -> CatgaResult<()> {
    let Some(postgres_url) = service_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let Some(nats_url) = service_url("CATGA_NATS_URL")? else {
        return Ok(());
    };
    let suffix = unique_suffix("postgres_nats");
    let flow_id = format!("cross-backend-flow-{suffix}");
    let store = SqlFlowStore::connect_postgres(&postgres_url).await?;
    store.migrate().await?;
    let state = FlowState::new(
        flow_id.as_str(),
        "cross-backend",
        b"postgres-nats".to_vec(),
        "e2e-node",
    );
    assert!(store.create(state.clone()).await?);
    let persisted = store.get(&flow_id).await?.ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Internal,
            "PostgreSQL FlowStore did not retain the created flow",
        )
    })?;
    assert_eq!(persisted.id(), flow_id);
    assert_eq!(persisted.data(), b"postgres-nats");

    let transport = NatsTransport::connect(NatsConfig {
        server: nats_url.into(),
        stream: format!("CATGA_CROSS_BACKEND_{suffix}").into(),
        subject: format!("catga.cross.backend.{suffix}").into(),
        consumer: format!("cross_backend_{suffix}").into(),
    })
    .await?;
    let id = message_id();
    let envelope = Envelope::new(
        id,
        "cross-backend.postgres.nats",
        b"postgres-nats".to_vec(),
        MessageMetadata::new(id, None),
    );
    transport.publish(envelope.clone()).await?;
    let delivery = receive_with_timeout(&transport).await?;
    assert_eq!(delivery.envelope().id(), envelope.id());
    assert_eq!(delivery.envelope().payload(), envelope.payload());
    transport.ack(delivery).await
}

#[tokio::test]
#[ignore = "requires real PostgreSQL and Redis services"]
async fn postgres_flow_store_and_redis_transport_round_trip() -> CatgaResult<()> {
    let Some(postgres_url) = service_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let Some(redis_url) = service_url("CATGA_REDIS_URL")? else {
        return Ok(());
    };
    let suffix = unique_suffix("postgres_redis");
    let flow_id = format!("cross-backend-flow-{suffix}");
    let store = SqlFlowStore::connect_postgres(&postgres_url).await?;
    store.migrate().await?;
    let state = FlowState::new(
        flow_id.as_str(),
        "cross-backend",
        b"postgres-redis".to_vec(),
        "e2e-node",
    );
    assert!(store.create(state).await?);
    let persisted = store.get(&flow_id).await?.ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Internal,
            "PostgreSQL FlowStore did not retain the created Redis-composed flow",
        )
    })?;
    assert_eq!(persisted.data(), b"postgres-redis");

    let transport = RedisTransport::connect(RedisConfig {
        server: redis_url.into(),
        stream: format!("catga:cross-backend:{suffix}").into(),
        group: format!("cross-backend:{suffix}").into(),
        consumer: format!("cross-backend:{suffix}").into(),
    })
    .await?;
    let id = message_id();
    let envelope = Envelope::new(
        id,
        "cross-backend.postgres.redis",
        b"postgres-redis".to_vec(),
        MessageMetadata::new(id, None),
    );
    transport.publish(envelope.clone()).await?;
    let delivery = receive_with_timeout(&transport).await?;
    assert_eq!(delivery.envelope().id(), envelope.id());
    assert_eq!(delivery.envelope().payload(), envelope.payload());
    transport.ack(delivery).await
}

#[tokio::test]
#[ignore = "requires real SQL Server and Redis services"]
async fn mssql_flow_store_and_redis_transport_round_trip() -> CatgaResult<()> {
    let Some(mssql_url) = service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let Some(redis_url) = service_url("CATGA_REDIS_URL")? else {
        return Ok(());
    };
    let _schema_guard = mssql_schema_lock().lock().await;
    let suffix = unique_suffix("mssql_redis");
    let flow_id = format!("cross-backend-flow-{suffix}");
    let store = SqlFlowStore::connect_mssql(&mssql_url).await?;
    store.migrate().await?;
    let state = FlowState::new(
        flow_id.as_str(),
        "cross-backend",
        b"mssql-redis".to_vec(),
        "e2e-node",
    );
    assert!(store.create(state.clone()).await?);
    let persisted = store.get(&flow_id).await?.ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Internal,
            "SQL Server FlowStore did not retain the created flow",
        )
    })?;
    assert_eq!(persisted.id(), flow_id);
    assert_eq!(persisted.data(), b"mssql-redis");

    let transport = RedisTransport::connect(RedisConfig {
        server: redis_url.into(),
        stream: format!("catga:cross-backend:{suffix}").into(),
        group: format!("cross-backend:{suffix}").into(),
        consumer: format!("cross-backend:{suffix}").into(),
    })
    .await?;
    let id = message_id();
    let envelope = Envelope::new(
        id,
        "cross-backend.mssql.redis",
        b"mssql-redis".to_vec(),
        MessageMetadata::new(id, None),
    );
    transport.publish(envelope.clone()).await?;
    let delivery = receive_with_timeout(&transport).await?;
    assert_eq!(delivery.envelope().id(), envelope.id());
    assert_eq!(delivery.envelope().payload(), envelope.payload());
    transport.ack(delivery).await
}

#[tokio::test]
#[ignore = "requires real SQL Server and NATS services"]
async fn mssql_flow_store_and_nats_transport_round_trip() -> CatgaResult<()> {
    let Some(mssql_url) = service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let Some(nats_url) = service_url("CATGA_NATS_URL")? else {
        return Ok(());
    };
    let _schema_guard = mssql_schema_lock().lock().await;
    let suffix = unique_suffix("mssql_nats");
    let flow_id = format!("cross-backend-flow-{suffix}");
    let store = SqlFlowStore::connect_mssql(&mssql_url).await?;
    store.migrate().await?;
    let state = FlowState::new(
        flow_id.as_str(),
        "cross-backend",
        b"mssql-nats".to_vec(),
        "e2e-node",
    );
    assert!(store.create(state).await?);
    let persisted = store.get(&flow_id).await?.ok_or_else(|| {
        CatgaError::new(
            ErrorCode::Internal,
            "SQL Server FlowStore did not retain the created NATS-composed flow",
        )
    })?;
    assert_eq!(persisted.data(), b"mssql-nats");

    let transport = NatsTransport::connect(NatsConfig {
        server: nats_url.into(),
        stream: format!("CATGA_CROSS_BACKEND_{suffix}").into(),
        subject: format!("catga.cross.backend.{suffix}").into(),
        consumer: format!("cross_backend_{suffix}").into(),
    })
    .await?;
    let id = message_id();
    let envelope = Envelope::new(
        id,
        "cross-backend.mssql.nats",
        b"mssql-nats".to_vec(),
        MessageMetadata::new(id, None),
    );
    transport.publish(envelope.clone()).await?;
    let delivery = receive_with_timeout(&transport).await?;
    assert_eq!(delivery.envelope().id(), envelope.id());
    assert_eq!(delivery.envelope().payload(), envelope.payload());
    transport.ack(delivery).await
}

#[tokio::test]
#[ignore = "requires a real MySQL service"]
async fn mysql_flow_store_create_batch_is_transactional() -> CatgaResult<()> {
    let Some(mysql_url) = service_url("CATGA_MYSQL_URL")? else {
        return Ok(());
    };
    let store = SqlFlowStore::connect_mysql(&mysql_url).await?;
    store.migrate().await?;
    assert_create_batch_contract(&store, &unique_suffix("mysql_batch")).await
}

#[tokio::test]
#[ignore = "requires a real PostgreSQL service"]
async fn postgres_flow_store_create_batch_is_transactional() -> CatgaResult<()> {
    let Some(postgres_url) = service_url("CATGA_POSTGRES_URL")? else {
        return Ok(());
    };
    let store = SqlFlowStore::connect_postgres(&postgres_url).await?;
    store.migrate().await?;
    assert_create_batch_contract(&store, &unique_suffix("postgres_batch")).await
}

#[tokio::test]
#[ignore = "requires a real SQL Server service"]
async fn mssql_flow_store_create_batch_is_transactional() -> CatgaResult<()> {
    let Some(mssql_url) = service_url("CATGA_MSSQL_URL")? else {
        return Ok(());
    };
    let _schema_guard = mssql_schema_lock().lock().await;
    let store = SqlFlowStore::connect_mssql(&mssql_url).await?;
    store.migrate().await?;
    assert_create_batch_contract(&store, &unique_suffix("mssql_batch")).await
}

/// Exercises the shared [`FlowStore::create_batch`] contract against one live backend.
async fn assert_create_batch_contract<S>(store: &S, tag: &str) -> CatgaResult<()>
where
    S: FlowStore + ?Sized,
{
    let states: Vec<FlowState> = (0..8)
        .map(|sequence| {
            FlowState::new(
                format!("batch-{tag}-{sequence}").as_str(),
                "cross-backend-batch",
                format!("payload-{sequence}").into_bytes(),
                "batch-node",
            )
        })
        .collect();

    let created = store.create_batch(states.clone()).await?;
    assert_eq!(created.len(), states.len());
    assert!(created.iter().all(|was_created| *was_created));

    let replayed = store.create_batch(states.clone()).await?;
    assert!(replayed.iter().all(|was_created| !*was_created));

    let extra_id = format!("batch-{tag}-extra");
    let mixed = vec![
        states[0].clone(),
        FlowState::new(
            extra_id.as_str(),
            "cross-backend-batch",
            b"extra".to_vec(),
            "batch-node",
        ),
    ];
    let mixed_created = store.create_batch(mixed).await?;
    assert_eq!(mixed_created, vec![false, true]);
    assert!(store.get(&extra_id).await?.is_some());
    Ok(())
}

fn service_url(name: &str) -> CatgaResult<Option<String>> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(Some(value)),
        Ok(_) | Err(env::VarError::NotPresent) if env::var_os("CI").is_none() => Ok(None),
        Ok(_) | Err(env::VarError::NotPresent) => Err(CatgaError::new(
            ErrorCode::Unavailable,
            format!("{name} must be configured when CI is set"),
        )),
        Err(error) => Err(CatgaError::new(
            ErrorCode::Validation,
            format!("could not read {name}: {error}"),
        )),
    }
}

fn unique_suffix(backend: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let sequence = UNIQUE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{backend}_{}_{}_{}", std::process::id(), nanos, sequence)
}

fn message_id() -> u64 {
    UNIQUE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
}

fn mssql_schema_lock() -> &'static tokio::sync::Mutex<()> {
    MSSQL_SCHEMA_LOCK.get_or_init(tokio::sync::Mutex::default)
}

/// Converts a missing broker delivery into a bounded, actionable E2E failure.
async fn receive_with_timeout(
    transport: &impl MessageTransport,
) -> CatgaResult<catga_core::Delivery> {
    tokio::time::timeout(Duration::from_secs(5), transport.receive())
        .await
        .map_err(|_| {
            CatgaError::new(
                ErrorCode::Timeout,
                "cross-backend transport delivery timed out",
            )
        })?
}
