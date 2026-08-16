//! Mutual-TLS (mTLS) peer authentication for HTTP servers.
//!
//! This module provides mTLS peer authentication for HTTP servers. The server
//! requires every client to present a certificate chaining to the configured client CA root
//! (WebPKI), records the verified chain as a request extension, and
//! [`mtls_peer_identity_middleware`] turns the leaf certificate into a stable peer identity.
//!
//! A connection without a valid client certificate fails the TLS handshake, so no request reaches
//! the application. When the acceptor is instead built from a custom
//! [`rustls::ServerConfig`] that tolerates missing client certificates, the middleware simply
//! inserts no identity and the application can decide how to handle unauthenticated requests.
//!
//! # Wiring
//!
//! ```no_run
//! use std::{net::TcpListener, path::Path};
//!
//! use axum::Router;
//! use catga_axum::{
//!     MtlsAcceptor, mtls_peer_identity_middleware, serve_mtls,
//! };
//!
//! # async fn wiring() -> catga_core::CatgaResult<()> {
//! let app = Router::new()
//!     .layer(axum::middleware::from_fn(mtls_peer_identity_middleware));
//! let acceptor = MtlsAcceptor::from_pem_files(
//!     Path::new("node-1.pem"),
//!     Path::new("node-1-key.pem"),
//!     Path::new("cluster-ca.pem"),
//! )?;
//! let listener = TcpListener::bind("0.0.0.0:9700").expect("bind listener");
//! serve_mtls(listener, acceptor, app).await
//! # }
//! ```
//!
//! The PEM-loading builders perform blocking file I/O and are intended for startup paths.

use std::{
    io,
    net::TcpListener as StdTcpListener,
    path::Path,
    sync::Arc,
    task::{Context, Poll},
};

use axum::{Router, extract::Request as AxumRequest, middleware::Next, response::Response};
use axum_server::accept::Accept;
use catga_core::{CatgaError, CatgaResult, ErrorCode};
use futures::future::BoxFuture;
use http::Request;
use rustls::pki_types::{CertificateDer, pem::PemObject};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;
use tower_service::Service;
use x509_parser::prelude::{FromDer, GeneralName, X509Certificate};

// ---------------------------------------------------------------------------
// Server configuration
// ---------------------------------------------------------------------------

/// Builds a rustls server configuration that requires verified client certificates.
///
/// `server_cert_chain` is the PEM certificate chain presented to peers (leaf first),
/// `server_key` its PEM private key (PKCS#8, PKCS#1, or SEC1), and `client_ca_root` the PEM
/// bundle of CA certificates that client certificates must chain to. Client verification uses
/// WebPKI against those roots; ALPN advertises HTTP/2 and HTTP/1.1.
///
/// The ring crypto provider is pinned explicitly: workspace feature unification can enable both
/// ring and aws-lc-rs on rustls, which makes the implicit-provider builders panic.
///
/// # Errors
///
/// Returns [`ErrorCode::Validation`] when a PEM file is missing or malformed, the chain or key
/// does not parse, or the client verifier cannot be built from the supplied roots.
pub fn mtls_server_tls_config(
    server_cert_chain: &Path,
    server_key: &Path,
    client_ca_root: &Path,
) -> CatgaResult<ServerConfig> {
    let chain = load_cert_chain(server_cert_chain)?;
    let key = load_private_key(server_key)?;
    let mut roots = RootCertStore::empty();
    for ca in load_cert_chain(client_ca_root)? {
        roots.add(ca).map_err(|error| {
            tls_config_error(format!(
                "client CA root {} is not a usable trust anchor: {error}",
                client_ca_root.display()
            ))
        })?;
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
        .build()
        .map_err(|error| {
            tls_config_error(format!("failed to build client cert verifier: {error}"))
        })?;
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|error| tls_config_error(format!("failed to configure TLS versions: {error}")))?
        .with_client_cert_verifier(verifier)
        .with_single_cert(chain, key)
        .map_err(|error| {
            tls_config_error(format!("invalid server certificate chain or key: {error}"))
        })?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(config)
}

/// The verified TLS client-certificate chain of the current connection, leaf first.
///
/// [`MtlsAcceptor`] inserts one instance into every request's extensions after a successful
/// handshake. The chain has already passed WebPKI verification against the configured client CA
/// roots; consumers must treat it as the authenticated identity source and never fall back to
/// request headers.
#[derive(Clone, Debug)]
pub struct TlsPeerCertificates {
    chain: Arc<[CertificateDer<'static>]>,
}

impl TlsPeerCertificates {
    /// Records a verified chain (leaf first) as a request-extension value.
    ///
    /// Custom acceptors can use this to stay compatible with
    /// [`mtls_peer_identity_middleware`]; [`MtlsAcceptor`] already constructs it.
    #[must_use]
    pub fn new(chain: Vec<CertificateDer<'static>>) -> Self {
        Self {
            chain: chain.into(),
        }
    }

    /// Returns the verified chain, leaf certificate first.
    #[must_use]
    pub fn chain(&self) -> &[CertificateDer<'static>] {
        &self.chain
    }

    /// Returns the verified leaf (client) certificate, if the peer presented any.
    #[must_use]
    pub fn leaf(&self) -> Option<&CertificateDer<'static>> {
        self.chain.first()
    }
}

/// An [`axum_server`] acceptor that performs mutual TLS and records the verified client chain.
///
/// Every accepted connection completes a rustls handshake that requires a valid client
/// certificate; the verified chain is then exposed to handlers as a [`TlsPeerCertificates`]
/// request extension. Handshake failures never reach the HTTP layer.
///
/// Use [`serve_mtls`] for the common case, or plug this acceptor into
/// [`axum_server::Server::acceptor`] when the deployment needs graceful shutdown or other
/// server options.
#[derive(Clone)]
pub struct MtlsAcceptor {
    acceptor: TlsAcceptor,
}

impl MtlsAcceptor {
    /// Wraps a fully configured rustls server configuration.
    ///
    /// The configuration should require client certificates (see
    /// [`mtls_server_tls_config`]); a configuration that allows anonymous clients still works,
    /// but requests then carry no [`TlsPeerCertificates`] extension.
    #[must_use]
    pub fn new(config: Arc<ServerConfig>) -> Self {
        Self {
            acceptor: TlsAcceptor::from(config),
        }
    }

    /// Builds an acceptor from PEM paths; see [`mtls_server_tls_config`] for semantics.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorCode::Validation`] on the same conditions as
    /// [`mtls_server_tls_config`].
    pub fn from_pem_files(
        server_cert_chain: &Path,
        server_key: &Path,
        client_ca_root: &Path,
    ) -> CatgaResult<Self> {
        let config = mtls_server_tls_config(server_cert_chain, server_key, client_ca_root)?;
        Ok(Self::new(Arc::new(config)))
    }
}

impl<S> Accept<TcpStream, S> for MtlsAcceptor
where
    S: Send + 'static,
{
    type Stream = TlsStream<TcpStream>;
    type Service = PeerCertificateService<S>;
    type Future = BoxFuture<'static, io::Result<(Self::Stream, Self::Service)>>;

    fn accept(&self, stream: TcpStream, service: S) -> Self::Future {
        let acceptor = self.acceptor.clone();
        Box::pin(async move {
            let stream = acceptor.accept(stream).await?;
            let certificates = stream
                .get_ref()
                .1
                .peer_certificates()
                .map(|chain| TlsPeerCertificates::new(chain.to_vec()));
            Ok((
                stream,
                PeerCertificateService {
                    inner: service,
                    certificates,
                },
            ))
        })
    }
}

/// Serves `app` on a pre-bound TCP listener with mutual TLS until the server stops.
///
/// This is the minimal mTLS serving path: no graceful shutdown, no config reload. Deployments
/// that need those should drive [`axum_server::Server`] directly with [`MtlsAcceptor`], which
/// exposes a `Handle` for shutdown. The listener is already bound, so the local address (and
/// therefore the port, useful with port 0 in tests) is known before this future is spawned.
///
/// # Errors
///
/// Returns [`ErrorCode::Unavailable`] when the listener cannot be converted for async use or
/// the server loop fails.
pub async fn serve_mtls(
    listener: StdTcpListener,
    acceptor: MtlsAcceptor,
    app: Router,
) -> CatgaResult<()> {
    listener.set_nonblocking(true).map_err(|error| {
        CatgaError::new(
            ErrorCode::Unavailable,
            format!("failed to configure mTLS listener: {error}"),
        )
    })?;
    axum_server::from_tcp(listener)
        .map_err(|error| {
            CatgaError::new(
                ErrorCode::Unavailable,
                format!("failed to prepare mTLS listener: {error}"),
            )
        })?
        .acceptor(acceptor)
        .serve(app.into_make_service())
        .await
        .map_err(|error| {
            CatgaError::new(
                ErrorCode::Unavailable,
                format!("mTLS server failed: {error}"),
            )
        })
}

/// Per-connection tower service that stamps every request with the verified client chain.
///
/// This is the service produced by [`MtlsAcceptor`]; it inserts the connection's
/// [`TlsPeerCertificates`] into each request's extensions before delegating to the inner
/// service. It is public so the [`Accept`] implementation has no private types in its
/// interface, but most deployments never name it directly.
#[derive(Clone)]
pub struct PeerCertificateService<S> {
    inner: S,
    certificates: Option<TlsPeerCertificates>,
}

impl<S, B> Service<Request<B>> for PeerCertificateService<S>
where
    S: Service<Request<B>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request<B>) -> Self::Future {
        if let Some(certificates) = &self.certificates {
            request.extensions_mut().insert(certificates.clone());
        }
        self.inner.call(request)
    }
}

// ---------------------------------------------------------------------------
// Peer identity extraction
// ---------------------------------------------------------------------------

/// A stable peer identity derived from a verified TLS client certificate.
///
/// This type wraps a string identity (typically a SPIFFE URI, DNS name, or CN)
/// that is extracted from a verified X.509 client certificate.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PeerIdentity(String);

impl PeerIdentity {
    /// Creates a new peer identity from a string value.
    ///
    /// Returns an error if the value is empty.
    pub fn new(value: impl Into<String>) -> Result<Self, &'static str> {
        let value = value.into();
        if value.is_empty() {
            Err("peer identity cannot be empty")
        } else {
            Ok(Self(value))
        }
    }

    /// Returns the identity string value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Extracts a stable peer identity from a verified X.509 leaf certificate (DER).
///
/// The first SAN URI entry wins, which covers SPIFFE-style workload identities such as
/// `spiffe://cluster/node-2`. When no SAN URI is present, the first DNS SAN is used, then the
/// subject CN. Returns `None` when the certificate does not parse or carries none of these
/// names (or the name is empty after trimming).
///
/// Callers must only pass certificates that the TLS stack has already verified; this function
/// performs no signature or chain validation itself.
#[must_use]
pub fn peer_identity_from_certificate(certificate: &[u8]) -> Option<PeerIdentity> {
    let (_, certificate) = X509Certificate::from_der(certificate).ok()?;
    let sans = certificate.subject_alternative_name().ok().flatten();
    let san_name = |want_uri: bool| {
        sans.as_ref().and_then(|san| {
            san.value
                .general_names
                .iter()
                .find_map(|name| match (name, want_uri) {
                    (GeneralName::URI(uri), true) => Some(*uri),
                    (GeneralName::DNSName(dns), false) => Some(*dns),
                    _ => None,
                })
        })
    };
    let identity = san_name(true).or_else(|| san_name(false)).or_else(|| {
        certificate
            .subject()
            .iter_common_name()
            .find_map(|attribute| attribute.attr_value().as_str().ok())
    })?;
    PeerIdentity::new(identity).ok()
}

/// Axum middleware that derives [`PeerIdentity`] from the verified client certificate.
///
/// Apply with `axum::middleware::from_fn(mtls_peer_identity_middleware)` on the router.
/// The identity comes from the verified [`TlsPeerCertificates`] extension (SAN URI,
/// then DNS SAN, then subject CN via [`peer_identity_from_certificate`]) and is inserted
/// as the request extension. A connection without a verified client certificate gets no
/// extension.
pub async fn mtls_peer_identity_middleware(mut request: AxumRequest, next: Next) -> Response {
    let identity = request
        .extensions()
        .get::<TlsPeerCertificates>()
        .and_then(TlsPeerCertificates::leaf)
        .and_then(|leaf| peer_identity_from_certificate(leaf.as_ref()));
    if let Some(identity) = identity {
        request.extensions_mut().insert(identity);
    }
    next.run(request).await
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// Builds a [`reqwest::Client`] that authenticates with a client certificate over rustls.
///
/// `client_cert_chain` and `client_key` are the PEM identity this node presents (leaf first);
/// `server_ca_root` is the PEM bundle of CA certificates trusted to sign peer server
/// certificates. Server verification uses pure WebPKI against exactly those roots.
pub fn mtls_reqwest_client(
    client_cert_chain: &Path,
    client_key: &Path,
    server_ca_root: &Path,
) -> CatgaResult<reqwest::Client> {
    let mut identity_pem = read_pem_file(client_cert_chain, "client certificate chain")?;
    identity_pem.extend_from_slice(&read_pem_file(client_key, "client private key")?);
    let identity = reqwest::Identity::from_pem(&identity_pem)
        .map_err(|e| tls_config_error(format!("failed to parse client identity: {}", e)))?;
    let ca_pem = read_pem_file(server_ca_root, "server CA root")?;
    let roots = reqwest::Certificate::from_pem_bundle(&ca_pem)
        .map_err(|e| tls_config_error(format!("failed to parse server CA root: {}", e)))?;
    reqwest::Client::builder()
        .use_rustls_tls()
        .identity(identity)
        .tls_certs_only(roots)
        .build()
        .map_err(|e| tls_config_error(format!("failed to build mTLS HTTP client: {}", e)))
}

// ---------------------------------------------------------------------------
// Development certificate helpers
// ---------------------------------------------------------------------------

/// An in-memory certificate authority for tests, demos, and local development.
pub struct DevCertificateAuthority {
    cert_pem: String,
    issuer_params: rcgen::CertificateParams,
    key_pair: rcgen::KeyPair,
}

impl DevCertificateAuthority {
    /// Generates a new self-signed CA with an ECDSA P-256 key.
    pub fn generate() -> CatgaResult<Self> {
        let mut issuer_params = rcgen::CertificateParams::default();
        issuer_params.distinguished_name = rcgen::DistinguishedName::new();
        issuer_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "catga-dev-ca");
        issuer_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        issuer_params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
        ];
        let key_pair = rcgen::KeyPair::generate().map_err(dev_pki_error)?;
        let cert = issuer_params
            .self_signed(&key_pair)
            .map_err(dev_pki_error)?;
        Ok(Self {
            cert_pem: cert.pem(),
            issuer_params,
            key_pair,
        })
    }

    /// Returns the PEM-encoded CA certificate.
    pub fn cert_pem(&self) -> &str {
        &self.cert_pem
    }

    /// Issues a node identity signed by this CA.
    pub fn issue_node_identity(
        &self,
        trust_domain: &str,
        node_name: &str,
    ) -> CatgaResult<DevNodeIdentity> {
        let uri = format!("spiffe://{}/{}", trust_domain, node_name);
        let uri =
            rcgen::string::Ia5String::try_from(uri).map_err(|e| tls_config_error(e.to_string()))?;
        let mut params =
            rcgen::CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
                .map_err(dev_pki_error)?;
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, node_name);
        params.subject_alt_names.push(rcgen::SanType::URI(uri));
        params.key_usages = vec![rcgen::KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let key_pair = rcgen::KeyPair::generate().map_err(dev_pki_error)?;
        let issuer = rcgen::Issuer::from_params(&self.issuer_params, &self.key_pair);
        let cert = params
            .signed_by(&key_pair, &issuer)
            .map_err(dev_pki_error)?;
        Ok(DevNodeIdentity {
            certificate_chain_pem: cert.pem(),
            private_key_pem: key_pair.serialize_pem(),
        })
    }
}

/// A PEM-encoded node certificate and private key issued by a DevCertificateAuthority.
pub struct DevNodeIdentity {
    certificate_chain_pem: String,
    private_key_pem: String,
}

impl DevNodeIdentity {
    /// Returns the PEM-encoded certificate chain.
    pub fn certificate_chain_pem(&self) -> &str {
        &self.certificate_chain_pem
    }
    /// Returns the PEM-encoded private key.
    pub fn private_key_pem(&self) -> &str {
        &self.private_key_pem
    }
}

// ---------------------------------------------------------------------------
// PEM loading and error mapping
// ---------------------------------------------------------------------------

fn load_cert_chain(path: &Path) -> CatgaResult<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let pem = read_pem_file(path, "certificate chain")?;
    rustls::pki_types::CertificateDer::pem_slice_iter(&pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| tls_config_error(format!("failed to parse certificate chain: {}", e)))
}

fn load_private_key(path: &Path) -> CatgaResult<rustls::pki_types::PrivateKeyDer<'static>> {
    let pem = read_pem_file(path, "private key")?;
    rustls::pki_types::PrivateKeyDer::from_pem_slice(&pem)
        .map_err(|e| tls_config_error(format!("failed to parse private key: {}", e)))
}

fn read_pem_file(path: &Path, kind: &str) -> CatgaResult<Vec<u8>> {
    std::fs::read(path)
        .map_err(|e| tls_config_error(format!("failed to read {} PEM file: {}", kind, e)))
}

fn tls_config_error(message: String) -> CatgaError {
    CatgaError::new(ErrorCode::Validation, message)
}

fn dev_pki_error(error: rcgen::Error) -> CatgaError {
    CatgaError::new(
        ErrorCode::Internal,
        format!("failed to generate development certificate: {}", error),
    )
}
