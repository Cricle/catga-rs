//! Contract tests for the mTLS helpers: development CA issuance, peer-identity
//! extraction precedence, PEM error mapping, and a full mutual-TLS round trip
//! with authenticated peer-identity extraction.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use axum::{
    Router,
    extract::Extension,
    middleware,
    routing::{get, post},
};
use catga_axum::{
    DevCertificateAuthority, MtlsAcceptor, PeerIdentity, TlsPeerCertificates,
    mtls_peer_identity_middleware, mtls_reqwest_client, mtls_server_tls_config,
    peer_identity_from_certificate, serve_mtls,
};
use catga_core::ErrorCode;
use http::StatusCode;
use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair};
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;

static NEXT_TEMP_DIR: AtomicU64 = AtomicU64::new(0);

/// A unique temporary directory that removes its contents on drop.
struct TempPemDir(PathBuf);

impl TempPemDir {
    fn new() -> Self {
        let id = NEXT_TEMP_DIR.fetch_add(1, Ordering::SeqCst);
        let path = std::env::temp_dir().join(format!("catga-axum-tls-{}-{id}", std::process::id()));
        std::fs::create_dir_all(&path).expect("temp dir must be created");
        Self(path)
    }

    fn write(&self, name: &str, contents: &str) -> PathBuf {
        let path = self.0.join(name);
        std::fs::write(&path, contents).expect("PEM file must be written");
        path
    }

    fn missing(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TempPemDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn first_cert_der(pem: &str) -> CertificateDer<'static> {
    CertificateDer::pem_slice_iter(pem.as_bytes())
        .next()
        .expect("a certificate PEM block")
        .expect("certificate PEM must parse")
}

#[test]
fn dev_ca_issues_node_identities_with_spiffe_san_uris() {
    let ca = DevCertificateAuthority::generate().expect("CA must generate");
    assert!(ca.cert_pem().contains("BEGIN CERTIFICATE"));

    let identity = ca
        .issue_node_identity("cluster", "node-2")
        .expect("identity must issue");
    assert!(
        identity
            .certificate_chain_pem()
            .contains("BEGIN CERTIFICATE")
    );
    assert!(identity.private_key_pem().contains("BEGIN PRIVATE KEY"));

    let der = first_cert_der(identity.certificate_chain_pem());
    let peer = peer_identity_from_certificate(der.as_ref()).expect("identity must extract");
    assert_eq!(peer.as_str(), "spiffe://cluster/node-2");
}

#[test]
fn dev_ca_rejects_non_ascii_trust_domains() {
    let ca = DevCertificateAuthority::generate().expect("CA must generate");
    let error = ca
        .issue_node_identity("clüster", "node-2")
        .map(|_| ())
        .expect_err("non-ASCII input must fail");
    assert_eq!(error.code(), ErrorCode::Validation);
}

#[test]
fn peer_identity_falls_back_to_dns_san_then_common_name() {
    // DNS SAN wins when no SAN URI is present.
    let key_pair = KeyPair::generate().expect("key generates");
    let mut params =
        CertificateParams::new(vec!["node.example".to_string()]).expect("params build");
    params.distinguished_name = DistinguishedName::new();
    params
        .distinguished_name
        .push(DnType::CommonName, "cn-fallback");
    let cert = params.self_signed(&key_pair).expect("cert signs");
    let peer = peer_identity_from_certificate(cert.der().as_ref()).expect("identity extracts");
    assert_eq!(peer.as_str(), "node.example");

    // The subject CN is the last resort.
    let key_pair = KeyPair::generate().expect("key generates");
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("params build");
    params.distinguished_name = DistinguishedName::new();
    params
        .distinguished_name
        .push(DnType::CommonName, "legacy-node");
    let cert = params.self_signed(&key_pair).expect("cert signs");
    let peer = peer_identity_from_certificate(cert.der().as_ref()).expect("identity extracts");
    assert_eq!(peer.as_str(), "legacy-node");

    // No usable name at all yields no identity (rcgen injects a default CN
    // unless the distinguished name is explicitly emptied).
    let key_pair = KeyPair::generate().expect("key generates");
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("params build");
    params.distinguished_name = DistinguishedName::new();
    let cert = params.self_signed(&key_pair).expect("cert signs");
    let extracted = peer_identity_from_certificate(cert.der().as_ref());
    assert!(extracted.is_none(), "unexpected identity: {extracted:?}");

    // Garbage input is rejected, never panics.
    assert!(peer_identity_from_certificate(b"not a certificate").is_none());
}

#[test]
fn tls_peer_certificates_exposes_leaf_first_chains() {
    let ca = DevCertificateAuthority::generate().expect("CA must generate");
    let identity = ca
        .issue_node_identity("cluster", "node-1")
        .expect("identity must issue");
    let leaf = first_cert_der(identity.certificate_chain_pem());
    let root = first_cert_der(ca.cert_pem());

    let chain = TlsPeerCertificates::new(vec![leaf.clone(), root]);
    assert_eq!(chain.chain().len(), 2);
    assert_eq!(chain.leaf(), Some(&leaf));

    let empty = TlsPeerCertificates::new(Vec::new());
    assert!(empty.leaf().is_none());
    assert!(empty.chain().is_empty());

    let cloned = chain.clone();
    assert_eq!(cloned.chain().len(), 2);
    let _debug = format!("{cloned:?}");
}

#[test]
fn tls_config_builders_map_missing_and_malformed_pem_files() {
    let dir = TempPemDir::new();

    // Missing files are validation errors on both builders.
    for result in [
        mtls_server_tls_config(
            Path::new("missing-cert.pem"),
            Path::new("missing-key.pem"),
            Path::new("missing-ca.pem"),
        )
        .map(|_| ()),
        mtls_reqwest_client(
            Path::new("missing-cert.pem"),
            Path::new("missing-key.pem"),
            Path::new("missing-ca.pem"),
        )
        .map(|_| ()),
    ] {
        let error = result.expect_err("missing PEM files must fail");
        assert_eq!(error.code(), ErrorCode::Validation);
    }
    let error = mtls_server_tls_config(
        &dir.missing("no-cert.pem"),
        &dir.missing("no-key.pem"),
        &dir.missing("no-ca.pem"),
    )
    .map(|_| ())
    .expect_err("missing files must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    // A chain file without any certificate block is rejected.
    let garbage = dir.write("garbage.pem", "this is not PEM content");
    let error = mtls_server_tls_config(&garbage, &garbage, &garbage)
        .map(|_| ())
        .expect_err("garbage PEM must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    // A syntactically valid certificate in the key position is rejected.
    let ca = DevCertificateAuthority::generate().expect("CA must generate");
    let identity = ca
        .issue_node_identity("cluster", "node-1")
        .expect("identity must issue");
    let cert_path = dir.write("node-1.pem", identity.certificate_chain_pem());
    let key_path = dir.write("node-1-key.pem", identity.private_key_pem());
    let ca_path = dir.write("ca.pem", ca.cert_pem());
    let error = mtls_server_tls_config(&cert_path, &cert_path, &ca_path)
        .map(|_| ())
        .expect_err("a certificate is not a private key");
    assert_eq!(error.code(), ErrorCode::Validation);

    // The client identity must parse as a certificate-plus-key bundle.
    let error = mtls_reqwest_client(&garbage, &garbage, &ca_path)
        .map(|_| ())
        .expect_err("garbage identity must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    // A certificate chain that does not match the private key is rejected.
    let other = ca
        .issue_node_identity("cluster", "node-2")
        .expect("identity must issue");
    let other_key_path = dir.write("node-2-key.pem", other.private_key_pem());
    let error = mtls_server_tls_config(&cert_path, &other_key_path, &ca_path)
        .map(|_| ())
        .expect_err("a mismatched key must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    // A PEM block whose payload is not a certificate fails chain parsing.
    let malformed_block = dir.write(
        "malformed.pem",
        "-----BEGIN CERTIFICATE-----\nAAAA####\n-----END CERTIFICATE-----\n",
    );
    let error = mtls_server_tls_config(&malformed_block, &key_path, &ca_path)
        .map(|_| ())
        .expect_err("a malformed certificate block must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    // The client trust bundle must contain parseable certificates.
    let error = mtls_reqwest_client(&cert_path, &key_path, &malformed_block)
        .map(|_| ())
        .expect_err("an unparseable CA bundle must fail");
    assert_eq!(error.code(), ErrorCode::Validation);

    // Happy paths build.
    mtls_server_tls_config(&cert_path, &key_path, &ca_path).expect("valid server config");
    mtls_reqwest_client(&cert_path, &key_path, &ca_path).expect("valid client");
    MtlsAcceptor::from_pem_files(&cert_path, &key_path, &ca_path).expect("acceptor builds");
}

#[tokio::test]
async fn mtls_round_trip_extracts_the_authenticated_peer_identity() {
    let dir = TempPemDir::new();
    let ca = DevCertificateAuthority::generate().expect("CA must generate");
    let server_identity = ca
        .issue_node_identity("cluster", "node-1")
        .expect("server identity must issue");
    let client_identity = ca
        .issue_node_identity("cluster", "node-2")
        .expect("client identity must issue");

    let server_cert = dir.write("server.pem", server_identity.certificate_chain_pem());
    let server_key = dir.write("server-key.pem", server_identity.private_key_pem());
    let client_cert = dir.write("client.pem", client_identity.certificate_chain_pem());
    let client_key = dir.write("client-key.pem", client_identity.private_key_pem());
    let ca_path = dir.write("ca.pem", ca.cert_pem());

    let app = Router::new()
        .route(
            "/identity",
            post(|identity: Option<Extension<PeerIdentity>>| async move {
                match identity {
                    Some(Extension(identity)) => (StatusCode::OK, identity.as_str().to_string()),
                    None => (StatusCode::UNAUTHORIZED, String::new()),
                }
            }),
        )
        .layer(middleware::from_fn(mtls_peer_identity_middleware));

    let acceptor =
        MtlsAcceptor::from_pem_files(&server_cert, &server_key, &ca_path).expect("acceptor builds");
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("listener binds");
    let port = listener.local_addr().expect("local address").port();
    let server = tokio::spawn(serve_mtls(listener, acceptor, app));

    let client = mtls_reqwest_client(&client_cert, &client_key, &ca_path).expect("client builds");
    let url = format!("https://127.0.0.1:{port}/identity");

    let mut response = None;
    for attempt in 0..50 {
        match client.post(&url).send().await {
            Ok(ok) => {
                response = Some(ok);
                break;
            }
            Err(_) if attempt < 49 => {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await
            }
            Err(error) => panic!("mTLS request must eventually succeed: {error}"),
        }
    }
    let response = response.expect("response received");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.text().await.expect("identity body"),
        "spiffe://cluster/node-2",
        "the verified certificate identity must reach the handler"
    );

    // A client without a certificate never completes the handshake.
    let anonymous = reqwest::Client::new();
    let result = anonymous.post(&url).send().await;
    assert!(result.is_err(), "anonymous clients fail the handshake");

    server.abort();
}

#[tokio::test]
async fn mtls_middleware_passes_requests_without_verified_certificates() {
    // Without the acceptor's extension the middleware inserts no identity and
    // the request still reaches the handler.
    let app = Router::new()
        .route("/probe", get(|| async { StatusCode::OK }))
        .layer(middleware::from_fn(mtls_peer_identity_middleware));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener binds");
    let address = listener.local_addr().expect("local address");
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("server runs");
    });

    let response = reqwest::Client::new()
        .get(format!("http://{address}/probe"))
        .send()
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), StatusCode::OK);

    server.abort();
}
