use super::*;

#[test]
fn validate_config_accepts_valid_config() {
    let config = NatsPublisherConfig {
        server: "nats://localhost:4222".into(),
        stream: "test-stream".into(),
        subject: "test.subject".into(),
    };
    assert!(validate_config(&config).is_ok());
}

#[test]
fn validate_config_rejects_empty_server() {
    let config = NatsPublisherConfig {
        server: "".into(),
        stream: "test-stream".into(),
        subject: "test.subject".into(),
    };
    let result = validate_config(&config);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("NATS server"));
}

#[test]
fn validate_config_rejects_whitespace_server() {
    let config = NatsPublisherConfig {
        server: "   ".into(),
        stream: "test-stream".into(),
        subject: "test.subject".into(),
    };
    assert!(validate_config(&config).is_err());
}

#[test]
fn validate_config_rejects_empty_stream() {
    let config = NatsPublisherConfig {
        server: "nats://localhost:4222".into(),
        stream: "".into(),
        subject: "test.subject".into(),
    };
    let result = validate_config(&config);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("NATS stream"));
}

#[test]
fn validate_config_rejects_empty_subject() {
    let config = NatsPublisherConfig {
        server: "nats://localhost:4222".into(),
        stream: "test-stream".into(),
        subject: "".into(),
    };
    let result = validate_config(&config);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("NATS subject"));
}
