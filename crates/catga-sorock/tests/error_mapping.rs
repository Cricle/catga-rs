//! Exhaustive tests for the public error-mapping functions in
//! `catga_sorock::error`: every documented gRPC code category, transport
//! failures, redb storage failures, and sorock's opaque anyhow errors.

use std::{net::Ipv4Addr, time::Duration};

use catga_core::ErrorCode;
use catga_sorock::error::{
    anyhow_to_catga, database_to_catga, status_to_catga, transport_to_catga,
};

#[test]
fn status_mapping_covers_every_documented_code() {
    let cases = [
        (tonic::Code::Unavailable, ErrorCode::Unavailable),
        (tonic::Code::DeadlineExceeded, ErrorCode::Timeout),
        (tonic::Code::Cancelled, ErrorCode::Cancelled),
        (tonic::Code::NotFound, ErrorCode::NotFound),
        (tonic::Code::AlreadyExists, ErrorCode::Conflict),
        (tonic::Code::FailedPrecondition, ErrorCode::Conflict),
        (tonic::Code::InvalidArgument, ErrorCode::Validation),
        (tonic::Code::OutOfRange, ErrorCode::Validation),
        (tonic::Code::Unauthenticated, ErrorCode::Unauthorized),
        (tonic::Code::PermissionDenied, ErrorCode::Forbidden),
        (tonic::Code::Unknown, ErrorCode::Transient),
        (tonic::Code::Aborted, ErrorCode::Transient),
        (tonic::Code::ResourceExhausted, ErrorCode::Transient),
        (tonic::Code::Internal, ErrorCode::Internal),
        (tonic::Code::DataLoss, ErrorCode::Internal),
        (tonic::Code::Unimplemented, ErrorCode::Internal),
    ];

    for (grpc_code, expected) in cases {
        let status = tonic::Status::new(grpc_code, "boom");
        let error = status_to_catga(&status);
        assert_eq!(
            error.code(),
            expected,
            "grpc code {grpc_code:?} must map to {expected:?}"
        );
        assert_eq!(error.message(), "sorock gRPC request failed: boom");
        assert_eq!(
            error.details(),
            Some(format!("grpc code: {grpc_code:?}").as_str()),
            "the grpc code must be preserved in the details"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transport_errors_map_to_transport_failed() {
    // Bind and immediately drop a listener to obtain a port nothing serves.
    let listener =
        std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("ephemeral bind must succeed");
    let port = listener
        .local_addr()
        .expect("the bound address must be readable")
        .port();
    drop(listener);

    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .expect("a loopback endpoint URI must parse");
    let transport_error = tokio::time::timeout(Duration::from_secs(10), endpoint.connect())
        .await
        .expect("connecting to a closed port must terminate")
        .expect_err("connecting to a closed port must fail");

    let error = transport_to_catga(&transport_error);
    assert_eq!(error.code(), ErrorCode::TransportFailed);
    assert!(
        error.message().starts_with("sorock transport failed: "),
        "unexpected message: {}",
        error.message()
    );
}

#[test]
fn database_errors_map_to_persistence_failed() {
    // A path inside a directory that does not exist cannot be created.
    let missing_dir =
        std::env::temp_dir().join(format!("catga-sorock-missing-{}", std::process::id()));
    assert!(
        !missing_dir.exists(),
        "test precondition: {missing_dir:?} must not exist"
    );
    let db_error = redb::Database::create(missing_dir.join("raft.redb"))
        .expect_err("creating a database in a missing directory must fail");

    let error = database_to_catga(&db_error);
    assert_eq!(error.code(), ErrorCode::PersistenceFailed);
    assert!(
        error.message().starts_with("sorock redb storage failed: "),
        "unexpected message: {}",
        error.message()
    );
}

#[test]
fn anyhow_mapping_matches_known_sorock_failures() {
    let leader = anyhow_to_catga(&anyhow::anyhow!("leader is unknown"));
    assert_eq!(leader.code(), ErrorCode::Unavailable);
    assert_eq!(leader.message(), "sorock node failed: leader is unknown");

    let missing = anyhow_to_catga(&anyhow::anyhow!("peer (node_id=x) not found"));
    assert_eq!(missing.code(), ErrorCode::NotFound);
    assert_eq!(
        missing.message(),
        "sorock node failed: peer (node_id=x) not found"
    );

    let other = anyhow_to_catga(&anyhow::anyhow!("something unexpected"));
    assert_eq!(other.code(), ErrorCode::Internal);
    assert_eq!(other.message(), "sorock node failed: something unexpected");
}
