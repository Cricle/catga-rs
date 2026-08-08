//! Unit tests for MailboxClient helper functions.

use catga_core::codec::memorypack::MemoryPackCodec;
use catga_core::{Envelope, MessageMetadata, MessagePriority};

use catga_robustmq::{MailboxConfig, MailboxClient};

fn encode_envelope<C: catga_core::EnvelopeCodec>(codec: &C, envelope: &Envelope) -> catga_core::CatgaResult<Vec<u8>> {
    codec.encode(envelope)
}

fn decode_envelope<C: catga_core::EnvelopeCodec>(codec: &C, bytes: &[u8]) -> catga_core::CatgaResult<Envelope> {
    codec.decode(bytes)
}

#[test]
fn test_mailbox_config_creation() {
    let config = MailboxConfig {
        server: "nats://127.0.0.1:4222".into(),
        ttl_seconds: 60,
        public: false,
        name: "order-replies".into(),
        description: "private request replies".into(),
    };
    assert_eq!(config.server.as_ref(), "nats://127.0.0.1:4222");
    assert_eq!(config.ttl_seconds, 60);
    assert!(!config.public);
    assert_eq!(config.name.as_ref(), "order-replies");
    assert_eq!(config.description.as_ref(), "private request replies");
}

#[test]
fn test_mailbox_config_public() {
    let config = MailboxConfig {
        server: "nats://127.0.0.1:4222".into(),
        ttl_seconds: 300,
        public: true,
        name: "public-service".into(),
        description: "public mailbox".into(),
    };
    assert!(config.public);
}

#[test]
fn test_mailbox_config_clone() {
    let config = MailboxConfig {
        server: "nats://127.0.0.1:4222".into(),
        ttl_seconds: 60,
        public: false,
        name: "test".into(),
        description: "test desc".into(),
    };
    let cloned = config.clone();
    assert_eq!(config, cloned);
}

#[test]
fn test_mailbox_config_debug() {
    let config = MailboxConfig {
        server: "nats://127.0.0.1:4222".into(),
        ttl_seconds: 60,
        public: false,
        name: "test".into(),
        description: "test desc".into(),
    };
    let debug_str = format!("{:?}", config);
    assert!(debug_str.contains("MailboxConfig"));
    assert!(debug_str.contains("4222"));
}

#[test]
fn test_mailbox_config_eq() {
    let config1 = MailboxConfig {
        server: "nats://127.0.0.1:4222".into(),
        ttl_seconds: 60,
        public: false,
        name: "test".into(),
        description: "test desc".into(),
    };
    let config2 = MailboxConfig {
        server: "nats://127.0.0.1:4222".into(),
        ttl_seconds: 60,
        public: false,
        name: "test".into(),
        description: "test desc".into(),
    };
    let config3 = MailboxConfig {
        server: "nats://127.0.0.1:4223".into(),
        ttl_seconds: 60,
        public: false,
        name: "test".into(),
        description: "test desc".into(),
    };
    assert_eq!(config1, config2);
    assert_ne!(config1, config3);
}

#[test]
fn test_encode_decode_envelope_roundtrip() {
    let codec = MemoryPackCodec::default();
    let metadata = MessageMetadata::new(1, None).with_priority(MessagePriority::High);
    let original = Envelope::new(42, "test.message", vec![1, 2, 3], metadata);

    let encoded = encode_envelope(&codec, &original).expect("encode should succeed");
    assert!(!encoded.is_empty());

    let decoded = decode_envelope(&codec, &encoded).expect("decode should succeed");

    assert_eq!(decoded.id(), original.id());
    assert_eq!(decoded.message_type(), original.message_type());
    assert_eq!(decoded.payload(), original.payload());
    assert_eq!(decoded.schema_version(), original.schema_version());
}

#[test]
fn test_encode_envelope_with_priority() {
    let codec = MemoryPackCodec::default();

    for priority in [
        MessagePriority::Critical,
        MessagePriority::High,
        MessagePriority::Normal,
        MessagePriority::Low,
    ] {
        let metadata = MessageMetadata::new(1, None).with_priority(priority);
        let envelope = Envelope::new(1, "test", vec![], metadata);
        let encoded = encode_envelope(&codec, &envelope).expect("encode should succeed");
        assert!(
            !encoded.is_empty(),
            "encoding {:?} should produce non-empty bytes",
            priority
        );
    }
}

#[test]
fn test_encode_decode_envelope_with_empty_payload() {
    let codec = MemoryPackCodec::default();
    let metadata = MessageMetadata::new(1, None);
    let original = Envelope::new(1, "empty.message", vec![], metadata);

    let encoded = encode_envelope(&codec, &original).expect("encode should succeed");
    let decoded = decode_envelope(&codec, &encoded).expect("decode should succeed");

    assert!(decoded.payload().is_empty());
    assert_eq!(decoded.message_type(), "empty.message");
}

#[test]
fn test_encode_decode_envelope_with_large_payload() {
    let codec = MemoryPackCodec::default();
    let payload: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
    let metadata = MessageMetadata::new(1, None);
    let original = Envelope::new(1, "large.message", payload.clone(), metadata);

    let encoded = encode_envelope(&codec, &original).expect("encode should succeed");
    let decoded = decode_envelope(&codec, &encoded).expect("decode should succeed");

    assert_eq!(decoded.payload(), &payload);
}

#[test]
fn test_decode_invalid_bytes() {
    let codec = MemoryPackCodec::default();
    let invalid_bytes = vec![0xFF, 0xFE, 0xFD];
    let result = decode_envelope(&codec, &invalid_bytes);
    assert!(result.is_err(), "decoding invalid bytes should fail");
}

#[test]
fn test_mailbox_config_with_various_ttls() {
    let test_cases = [1u64, 60, 3600, u64::MAX];
    for ttl in test_cases {
        let config = MailboxConfig {
            server: "nats://127.0.0.1:4222".into(),
            ttl_seconds: ttl,
            public: false,
            name: "test".into(),
            description: "".into(),
        };
        assert_eq!(config.ttl_seconds, ttl);
    }
}

#[test]
fn test_mailbox_config_with_different_servers() {
    let servers = [
        "nats://127.0.0.1:4222",
        "nats://192.168.1.1:4222",
        "nats://example.com:4222",
    ];
    for server in servers {
        let config = MailboxConfig {
            server: server.into(),
            ttl_seconds: 60,
            public: false,
            name: "test".into(),
            description: "".into(),
        };
        assert_eq!(config.server.as_ref(), server);
    }
}

#[test]
fn test_mailbox_config_name_variations() {
    let names = [
        "",
        "simple",
        "with-dashes",
        "with_underscores",
        "With.UPPERCASE",
    ];
    for name in names {
        let config = MailboxConfig {
            server: "nats://127.0.0.1:4222".into(),
            ttl_seconds: 60,
            public: false,
            name: name.into(),
            description: "".into(),
        };
        assert_eq!(config.name.as_ref(), name);
    }
}

#[test]
fn test_mailbox_config_description_variations() {
    let descriptions = [
        "",
        "short",
        "A longer description with spaces",
        "特殊字符!@#$%",
    ];
    for desc in descriptions {
        let config = MailboxConfig {
            server: "nats://127.0.0.1:4222".into(),
            ttl_seconds: 60,
            public: false,
            name: "test".into(),
            description: desc.into(),
        };
        assert_eq!(config.description.as_ref(), desc);
    }
}

#[test]
fn test_encode_decode_envelope_with_metadata() {
    let codec = MemoryPackCodec::default();

    let test_cases = vec![
        MessageMetadata::new(1, None).with_priority(MessagePriority::Critical),
        MessageMetadata::new(2, Some(12345)).with_priority(MessagePriority::Normal),
        MessageMetadata::new(3, Some(0)),
    ];

    for metadata in test_cases {
        let original = Envelope::new(1, "test.message", vec![1, 2, 3], metadata);
        let encoded = encode_envelope(&codec, &original).expect("encode should succeed");
        let decoded = decode_envelope(&codec, &encoded).expect("decode should succeed");

        assert_eq!(decoded.schema_version(), original.schema_version());
    }
}

#[test]
fn test_encode_decode_envelope_with_reply_to() {
    let codec = MemoryPackCodec::default();
    let metadata = MessageMetadata::new(1, None);
    let original =
        Envelope::new(1, "request", vec![1, 2, 3], metadata).with_reply_to("reply-mailbox-123");

    let encoded = encode_envelope(&codec, &original).expect("encode should succeed");
    let decoded = decode_envelope(&codec, &encoded).expect("decode should succeed");

    assert!(decoded.reply_to().is_some());
    assert_eq!(
        decoded.reply_to().expect("reply_to should be present"),
        "reply-mailbox-123"
    );
}

#[test]
fn test_encode_decode_special_characters_in_message_type() {
    let codec = MemoryPackCodec::default();
    let message_types = vec![
        "simple",
        "with.dots",
        "with_underscores",
        "With-UPPERCASE",
        "numbers123",
        "CamelCase",
    ];

    for msg_type in message_types {
        let metadata = MessageMetadata::new(1, None);
        let original = Envelope::new(1, msg_type, vec![], metadata);
        let encoded = encode_envelope(&codec, &original).expect("encode should succeed");
        let decoded = decode_envelope(&codec, &encoded).expect("decode should succeed");
        assert_eq!(decoded.message_type(), msg_type);
    }
}

#[test]
fn test_encode_decode_binary_payload() {
    let codec = MemoryPackCodec::default();

    let test_cases = vec![
        vec![] as Vec<u8>,
        vec![0x00],
        vec![0xFF],
        vec![0x00, 0xFF, 0x7F, 0x80],
        (0..256).map(|i| i as u8).collect(),
        vec![b'a'; 100],
    ];

    for payload in test_cases {
        let metadata = MessageMetadata::new(1, None);
        let original = Envelope::new(1, "binary", payload.clone(), metadata);
        let encoded = encode_envelope(&codec, &original).expect("encode should succeed");
        let decoded = decode_envelope(&codec, &encoded).expect("decode should succeed");
        assert_eq!(decoded.payload(), &payload);
    }
}

#[test]
fn test_mailbox_config_all_public_values() {
    let config_public = MailboxConfig {
        server: "nats://127.0.0.1:4222".into(),
        ttl_seconds: 60,
        public: true,
        name: "public".into(),
        description: "desc".into(),
    };
    let config_private = MailboxConfig {
        server: "nats://127.0.0.1:4222".into(),
        ttl_seconds: 60,
        public: false,
        name: "private".into(),
        description: "desc".into(),
    };
    assert!(config_public.public);
    assert!(!config_private.public);
}

#[test]
fn test_encode_deploy_envelope_id_uniqueness() {
    let codec = MemoryPackCodec::default();
    let metadata = MessageMetadata::new(1, None);

    let mut decoded_ids = Vec::new();
    for id in [1u64, 2, 100, u64::MAX] {
        let original = Envelope::new(id, "test", vec![], metadata);
        let encoded = encode_envelope(&codec, &original).expect("encode should succeed");
        let decoded = decode_envelope(&codec, &encoded).expect("decode should succeed");
        decoded_ids.push(decoded.id());
    }

    let mut sorted_ids = decoded_ids.clone();
    sorted_ids.sort();
    sorted_ids.dedup();
    assert_eq!(sorted_ids.len(), decoded_ids.len());
}
