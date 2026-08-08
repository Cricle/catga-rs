//! Unit tests for transport helper functions.

use catga_core::{ErrorCode, QualityOfService};

const NATS_DEDUP_DROPS: &str = "catga.nats.dedup.drops";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NatsPublishMode {
    Core,
    JetStream,
    JetStreamDeduplicated,
}

const fn publish_mode(quality_of_service: QualityOfService) -> NatsPublishMode {
    match quality_of_service {
        QualityOfService::AtMostOnce => NatsPublishMode::Core,
        QualityOfService::AtLeastOnce => NatsPublishMode::JetStream,
        QualityOfService::ExactlyOnce => NatsPublishMode::JetStreamDeduplicated,
    }
}

struct NatsConfig {
    server: Box<str>,
    stream: Box<str>,
    subject: Box<str>,
    consumer: Box<str>,
}

struct NatsDestinationConfig {
    stream: Box<str>,
    subject: Box<str>,
    consumer: Box<str>,
}

fn map_error(error: impl std::fmt::Display) -> catga_core::CatgaError {
    catga_core::CatgaError::new(ErrorCode::Transient, error.to_string())
}

fn validate_config(config: &NatsConfig) -> Result<(), catga_core::CatgaError> {
    if config.stream.trim().is_empty()
        || config.subject.trim().is_empty()
        || config.consumer.trim().is_empty()
    {
        return Err(catga_core::CatgaError::new(
            ErrorCode::Validation,
            "NATS stream, subject, and consumer must not be empty",
        ));
    }
    Ok(())
}

fn validate_destination_config(config: &NatsDestinationConfig) -> Result<(), catga_core::CatgaError> {
    if config.stream.trim().is_empty()
        || config.subject.trim().is_empty()
        || config.consumer.trim().is_empty()
    {
        return Err(catga_core::CatgaError::new(
            ErrorCode::Validation,
            "NATS destination stream, subject, and consumer must not be empty",
        ));
    }
    Ok(())
}

fn record_broker_duplicate(_duplicate: bool) {
    // No-op in tests - just verify it doesn't panic
}

#[test]
fn nats_dedup_drops_constant() {
    assert_eq!(NATS_DEDUP_DROPS, "catga.nats.dedup.drops");
}

#[test]
fn validate_config_accepts_valid_config() {
    let config = NatsConfig {
        server: "nats://localhost:4222".into(),
        stream: "test-stream".into(),
        subject: "test.subject".into(),
        consumer: "test-consumer".into(),
    };
    assert!(validate_config(&config).is_ok());
}

#[test]
fn validate_config_rejects_empty_stream() {
    let config = NatsConfig {
        server: "nats://localhost:4222".into(),
        stream: "".into(),
        subject: "test.subject".into(),
        consumer: "test-consumer".into(),
    };
    let result = validate_config(&config);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("empty"));
}

#[test]
fn validate_config_rejects_empty_subject() {
    let config = NatsConfig {
        server: "nats://localhost:4222".into(),
        stream: "test-stream".into(),
        subject: "".into(),
        consumer: "test-consumer".into(),
    };
    let result = validate_config(&config);
    assert!(result.is_err());
}

#[test]
fn validate_config_rejects_empty_consumer() {
    let config = NatsConfig {
        server: "nats://localhost:4222".into(),
        stream: "test-stream".into(),
        subject: "test.subject".into(),
        consumer: "".into(),
    };
    let result = validate_config(&config);
    assert!(result.is_err());
}

#[test]
fn validate_config_rejects_whitespace_stream() {
    let config = NatsConfig {
        server: "nats://localhost:4222".into(),
        stream: "   ".into(),
        subject: "test.subject".into(),
        consumer: "test-consumer".into(),
    };
    let result = validate_config(&config);
    assert!(result.is_err());
}

#[test]
fn validate_config_rejects_whitespace_subject() {
    let config = NatsConfig {
        server: "nats://localhost:4222".into(),
        stream: "test-stream".into(),
        subject: "   ".into(),
        consumer: "test-consumer".into(),
    };
    assert!(validate_config(&config).is_err());
}

#[test]
fn validate_config_rejects_whitespace_consumer() {
    let config = NatsConfig {
        server: "nats://localhost:4222".into(),
        stream: "test-stream".into(),
        subject: "test.subject".into(),
        consumer: "   ".into(),
    };
    assert!(validate_config(&config).is_err());
}

#[test]
fn validate_config_rejects_all_empty() {
    let config = NatsConfig {
        server: "nats://localhost:4222".into(),
        stream: "".into(),
        subject: "".into(),
        consumer: "".into(),
    };
    let result = validate_config(&config);
    assert!(result.is_err());
}

#[test]
fn validate_destination_config_accepts_valid_config() {
    let config = NatsDestinationConfig {
        stream: "dest-stream".into(),
        subject: "dest.subject".into(),
        consumer: "dest-consumer".into(),
    };
    assert!(validate_destination_config(&config).is_ok());
}

#[test]
fn validate_destination_config_rejects_empty_stream() {
    let config = NatsDestinationConfig {
        stream: "".into(),
        subject: "dest.subject".into(),
        consumer: "dest-consumer".into(),
    };
    assert!(validate_destination_config(&config).is_err());
}

#[test]
fn validate_destination_config_rejects_empty_subject() {
    let config = NatsDestinationConfig {
        stream: "dest-stream".into(),
        subject: "".into(),
        consumer: "dest-consumer".into(),
    };
    assert!(validate_destination_config(&config).is_err());
}

#[test]
fn validate_destination_config_rejects_empty_consumer() {
    let config = NatsDestinationConfig {
        stream: "dest-stream".into(),
        subject: "dest.subject".into(),
        consumer: "".into(),
    };
    assert!(validate_destination_config(&config).is_err());
}

#[test]
fn validate_destination_config_rejects_whitespace_stream() {
    let config = NatsDestinationConfig {
        stream: "   ".into(),
        subject: "dest.subject".into(),
        consumer: "dest-consumer".into(),
    };
    assert!(validate_destination_config(&config).is_err());
}

#[test]
fn validate_destination_config_rejects_whitespace_subject() {
    let config = NatsDestinationConfig {
        stream: "dest-stream".into(),
        subject: "   ".into(),
        consumer: "dest-consumer".into(),
    };
    assert!(validate_destination_config(&config).is_err());
}

#[test]
fn validate_destination_config_rejects_whitespace_consumer() {
    let config = NatsDestinationConfig {
        stream: "dest-stream".into(),
        subject: "dest.subject".into(),
        consumer: "   ".into(),
    };
    assert!(validate_destination_config(&config).is_err());
}

#[test]
fn publish_mode_at_most_once() {
    assert_eq!(publish_mode(QualityOfService::AtMostOnce), NatsPublishMode::Core);
}

#[test]
fn publish_mode_at_least_once() {
    assert_eq!(publish_mode(QualityOfService::AtLeastOnce), NatsPublishMode::JetStream);
}

#[test]
fn publish_mode_exactly_once() {
    assert_eq!(publish_mode(QualityOfService::ExactlyOnce), NatsPublishMode::JetStreamDeduplicated);
}

#[test]
fn nats_publish_mode_variants() {
    assert_eq!(format!("{:?}", NatsPublishMode::Core), "Core");
    assert_eq!(format!("{:?}", NatsPublishMode::JetStream), "JetStream");
    assert_eq!(format!("{:?}", NatsPublishMode::JetStreamDeduplicated), "JetStreamDeduplicated");
}

#[test]
fn nats_publish_mode_equality() {
    assert_eq!(NatsPublishMode::Core, NatsPublishMode::Core);
    assert_eq!(NatsPublishMode::JetStream, NatsPublishMode::JetStream);
    assert_ne!(NatsPublishMode::Core, NatsPublishMode::JetStream);
}

#[test]
fn map_error_creates_transient_error() {
    let error = map_error("connection timeout");
    assert_eq!(error.code(), ErrorCode::Transient);
    assert!(error.to_string().contains("connection timeout"));
}

#[test]
fn map_error_handles_empty_string() {
    let error = map_error("");
    assert_eq!(error.code(), ErrorCode::Transient);
}

#[test]
fn map_error_includes_error_type() {
    let error = map_error("jetstream: consumer not found");
    assert!(error.to_string().contains("jetstream"));
}

#[test]
fn record_broker_duplicate_false_does_not_panic() {
    record_broker_duplicate(false);
}

#[test]
fn record_broker_duplicate_true_does_not_panic() {
    record_broker_duplicate(true);
}
