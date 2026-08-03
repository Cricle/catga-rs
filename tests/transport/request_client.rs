//! Typed request-client transport contract helpers.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use catga_codec_memorypack::{
    MemoryPackCodec, MemoryPackRequestClient, MemoryPackRequestClientFactory,
    MemoryPackRpcResponse, MemoryPackable,
};
use catga_core::{
    CatgaResult, Envelope, EnvelopeHeaders, EnvelopeRequestClient, ErrorCode, MessageMetadata,
    MessagePriority, Request, RequestTransport, SnowflakeIdGenerator, SnowflakeLayout,
    scope_transport_context,
};

struct EchoTransport;

#[derive(MemoryPackable, catga_core::Message)]
struct LookupStock {
    sku: u64,
}

impl Request for LookupStock {
    type Response = StockLevel;
    type TypeId = catga_core::DefaultMessageTypeId;
}

#[derive(MemoryPackable, catga_core::Message)]
#[catga(version = 2, priority = high)]
struct VersionedLookupStock {
    sku: u64,
}

impl Request for VersionedLookupStock {
    type Response = StockLevel;
    type TypeId = catga_core::DefaultMessageTypeId;
}

#[derive(Debug, Clone, Eq, PartialEq, MemoryPackable)]
struct StockLevel {
    quantity: u32,
}

struct TypedTransport;

#[derive(Default)]
struct ContextCapturingTransport {
    request: Mutex<Option<Envelope>>,
}

impl ContextCapturingTransport {
    fn captured_request(&self) -> CatgaResult<Envelope> {
        self.request
            .lock()
            .map_err(|_| {
                catga_core::CatgaError::new(ErrorCode::Internal, "request capture lock poisoned")
            })?
            .clone()
            .ok_or_else(|| catga_core::CatgaError::new(ErrorCode::NotFound, "request not captured"))
    }
}

#[derive(Default)]
struct FactoryTransport {
    requests: Mutex<Vec<(Box<str>, Duration)>>,
    attempts: AtomicUsize,
}

impl FactoryTransport {
    fn requests(&self) -> CatgaResult<Vec<(Box<str>, Duration)>> {
        self.requests
            .lock()
            .map(|requests| requests.clone())
            .map_err(|_| {
                catga_core::CatgaError::new(ErrorCode::Internal, "factory test lock poisoned")
            })
    }
}

#[async_trait]
impl RequestTransport for EchoTransport {
    async fn request(
        &self,
        destination: &str,
        request: Envelope,
        _: Duration,
    ) -> CatgaResult<Envelope> {
        if destination != "inventory" {
            return Err(catga_core::CatgaError::new(
                ErrorCode::NotFound,
                "destination",
            ));
        }
        Ok(request)
    }
}

#[async_trait]
impl RequestTransport for TypedTransport {
    async fn request(
        &self,
        destination: &str,
        request: Envelope,
        _: Duration,
    ) -> CatgaResult<Envelope> {
        if destination != "inventory" {
            return Err(catga_core::CatgaError::new(
                ErrorCode::NotFound,
                "destination",
            ));
        }
        let codec = MemoryPackCodec::default();
        let lookup: LookupStock = codec.decode_value(request.payload())?;
        let payload = codec.encode_value(&MemoryPackRpcResponse::Success(StockLevel {
            quantity: lookup.sku as u32,
        }))?;
        Ok(Envelope::new(
            request.id(),
            "stock.level",
            payload,
            MessageMetadata::new(
                request.metadata().message_id(),
                request.metadata().correlation_id(),
            ),
        ))
    }
}

#[async_trait]
impl RequestTransport for ContextCapturingTransport {
    async fn request(
        &self,
        destination: &str,
        request: Envelope,
        _: Duration,
    ) -> CatgaResult<Envelope> {
        if destination != "inventory" {
            return Err(catga_core::CatgaError::new(
                ErrorCode::NotFound,
                "destination",
            ));
        }
        *self.request.lock().map_err(|_| {
            catga_core::CatgaError::new(ErrorCode::Internal, "request capture lock poisoned")
        })? = Some(request.clone());
        let codec = MemoryPackCodec::default();
        let lookup: VersionedLookupStock = codec.decode_value(request.payload())?;
        let payload = codec.encode_value(&MemoryPackRpcResponse::Success(StockLevel {
            quantity: lookup.sku as u32,
        }))?;
        Ok(Envelope::new(
            request.id(),
            "stock.level",
            payload,
            MessageMetadata::new(
                request.metadata().message_id(),
                request.metadata().correlation_id(),
            ),
        ))
    }
}

#[async_trait]
impl RequestTransport for FactoryTransport {
    async fn request(
        &self,
        destination: &str,
        request: Envelope,
        timeout: Duration,
    ) -> CatgaResult<Envelope> {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        self.requests
            .lock()
            .map_err(|_| {
                catga_core::CatgaError::new(ErrorCode::Internal, "factory test lock poisoned")
            })?
            .push((destination.into(), timeout));
        let codec = MemoryPackCodec::default();
        let lookup: LookupStock = codec.decode_value(request.payload())?;
        let payload = codec.encode_value(&MemoryPackRpcResponse::Success(StockLevel {
            quantity: lookup.sku as u32,
        }))?;
        Ok(Envelope::new(
            request.id(),
            "stock.level",
            payload,
            MessageMetadata::new(
                request.metadata().message_id(),
                request.metadata().correlation_id(),
            ),
        ))
    }
}

#[tokio::test]
async fn envelope_request_client_routes_to_its_destination_without_reply_state() {
    let client =
        EnvelopeRequestClient::new(Arc::new(EchoTransport), "inventory", Duration::from_secs(1))
            .expect("client configuration is valid");
    let request = Envelope::new(7, "stock", vec![1, 2], MessageMetadata::new(7, Some(7)));

    let response = client.request(request).await.expect("request succeeds");

    assert_eq!(response.id(), 7);
    assert_eq!(response.payload(), [1, 2]);
}

#[tokio::test]
async fn memorypack_typed_request_client_uses_any_envelope_request_transport() {
    let generator = Arc::new(
        SnowflakeIdGenerator::new(1, SnowflakeLayout::default())
            .expect("test Snowflake configuration is valid"),
    );
    let client = MemoryPackRequestClient::new(
        Arc::new(TypedTransport),
        "inventory",
        Duration::from_secs(1),
        generator,
    )
    .expect("client configuration is valid");
    let cloned = client.clone();

    let response = client
        .request(&LookupStock { sku: 24 }, Duration::from_secs(1))
        .await
        .expect("typed request succeeds");

    assert_eq!(response, StockLevel { quantity: 24 });
    assert_eq!(cloned.destination(), "inventory");
}

#[tokio::test]
async fn memorypack_request_client_propagates_scoped_version_headers_and_priority() {
    let transport = Arc::new(ContextCapturingTransport::default());
    let generator = Arc::new(
        SnowflakeIdGenerator::new(1, SnowflakeLayout::default())
            .expect("test Snowflake configuration is valid"),
    );
    let client = MemoryPackRequestClient::new(
        Arc::clone(&transport),
        "inventory",
        Duration::from_secs(1),
        generator,
    )
    .expect("client configuration is valid");
    let inbound = Envelope::new(
        91,
        "orders.received",
        vec![],
        MessageMetadata::new(91, Some(61)).with_priority(MessagePriority::Critical),
    )
    .with_headers(EnvelopeHeaders::try_new([("tenant", "blue")]).expect("valid inbound headers"));

    let response = scope_transport_context(
        &inbound,
        client.request(&VersionedLookupStock { sku: 12 }, Duration::from_secs(1)),
    )
    .await
    .expect("scoped request succeeds");
    let captured = transport.captured_request().expect("request was captured");

    assert_eq!(response, StockLevel { quantity: 12 });
    assert_eq!(captured.schema_version(), 2);
    assert_eq!(captured.metadata().priority(), MessagePriority::Critical);
    assert_eq!(captured.metadata().correlation_id(), Some(61));
    assert_eq!(captured.header("tenant"), Some("blue"));
}

#[tokio::test]
async fn memorypack_request_client_factory_uses_type_default_and_explicit_client_policies()
-> CatgaResult<()> {
    let transport = Arc::new(FactoryTransport::default());
    let generator = Arc::new(SnowflakeIdGenerator::new(1, SnowflakeLayout::default())?);
    let factory = MemoryPackRequestClientFactory::new(
        Arc::clone(&transport),
        Duration::from_millis(17),
        generator,
    )?;

    let default_client = factory.create::<LookupStock>()?;
    assert_eq!(
        default_client
            .request_default(&LookupStock { sku: 11 })
            .await?,
        StockLevel { quantity: 11 }
    );

    let explicit_client =
        factory.create_to_with_timeout::<LookupStock>("inventory", Duration::from_millis(5))?;
    assert_eq!(
        explicit_client
            .request_default(&LookupStock { sku: 12 })
            .await?,
        StockLevel { quantity: 12 }
    );

    assert!(matches!(
        factory.create_to_with_timeout::<LookupStock>("invalid", Duration::ZERO),
        Err(error) if error.code() == ErrorCode::Validation
    ));
    assert_eq!(transport.attempts.load(Ordering::Relaxed), 2);
    assert_eq!(
        transport.requests()?,
        vec![
            (
                std::any::type_name::<LookupStock>().into(),
                Duration::from_millis(17)
            ),
            ("inventory".into(), Duration::from_millis(5)),
        ]
    );
    Ok(())
}
