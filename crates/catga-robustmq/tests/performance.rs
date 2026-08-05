//! RobustMQ transport performance tests.
//!
//! These tests require an external RobustMQ server and are ignored by default.
//! Run with: `cargo test -p catga-robustmq --test performance -- --ignored`

use std::time::Instant;

use catga_core::{
    Envelope, EnvelopeCodec, ErrorCode, MessageMetadata, QualityOfService,
    codec::memorypack::MemoryPackCodec,
};

/// Test envelope encoding/decoding throughput.
#[tokio::test]
#[ignore = "requires RobustMQ server"]
async fn envelope_encoding_decoding_throughput() -> Result<(), Box<dyn std::error::Error>> {
    const COUNT: usize = 100_000;
    let codec = MemoryPackCodec::default();

    // Create a test envelope
    let metadata =
        MessageMetadata::new(1, None).with_quality_of_service(QualityOfService::AtLeastOnce);
    let original = Envelope::new(1, "test.message", vec![1, 2, 3, 4, 5, 6, 7, 8], metadata);

    // Benchmark encoding
    let encode_start = Instant::now();
    let mut encoded = Vec::with_capacity(COUNT * 64);
    for _ in 0..COUNT {
        encoded.push(codec.encode(&original)?);
    }
    let encode_elapsed = encode_start.elapsed();
    let encode_ops = COUNT as f64 / encode_elapsed.as_secs_f64();

    // Benchmark decoding
    let decode_start = Instant::now();
    for bytes in &encoded {
        let _: Envelope = codec.decode(bytes)?;
    }
    let decode_elapsed = decode_start.elapsed();
    let decode_ops = COUNT as f64 / decode_elapsed.as_secs_f64();

    println!(
        "Envelope encode: {:.0} ops/s ({:?} total)",
        encode_ops, encode_elapsed
    );
    println!(
        "Envelope decode: {:.0} ops/s ({:?} total)",
        decode_ops, decode_elapsed
    );
    Ok(())
}

/// Test client creation and configuration overhead.
#[tokio::test]
#[ignore = "requires RobustMQ server"]
async fn client_connection_establishment() -> Result<(), Box<dyn std::error::Error>> {
    use catga_robustmq::{MailboxClient, MailboxConfig};

    let config = MailboxConfig {
        server: "nats://127.0.0.1:4222".into(),
        ttl_seconds: 60,
        public: false,
        name: "perf_test".into(),
        description: "performance test mailbox".into(),
    };

    // Measure connection time
    let start = Instant::now();
    let client = MailboxClient::connect(&config.server).await?;
    let connect_elapsed = start.elapsed();

    println!(
        "RobustMQ client connection establishment: {:?}",
        connect_elapsed
    );

    // Create a test mailbox
    let mailbox_start = Instant::now();
    let _mailbox = client.create(&config).await?;
    let mailbox_elapsed = mailbox_start.elapsed();

    println!("RobustMQ mailbox creation: {:?}", mailbox_elapsed);

    // Measure send/receive round-trip
    const COUNT: usize = 10_000;
    let metadata = MessageMetadata::new(1, None);
    let envelope = Envelope::new(1, "test", vec![1, 2, 3], metadata);

    let (mailbox_id, _) = {
        let m = client
            .create(&MailboxConfig {
                server: config.server.clone(),
                ttl_seconds: 60,
                public: true,
                name: "perf_test_replies".into(),
                description: "".into(),
            })
            .await?;
        (m.mail_id.clone(), m)
    };

    let send_start = Instant::now();
    for i in 0..COUNT {
        let mut e = envelope.clone();
        let meta = MessageMetadata::new(i as u64, None);
        e = e.with_metadata(meta);
        client
            .send_envelope(&mailbox_id, &e, catga_robustmq::MailboxPriority::Normal)
            .await?;
    }
    let send_elapsed = send_start.elapsed();
    let send_ops = COUNT as f64 / send_elapsed.as_secs_f64();

    println!(
        "RobustMQ send throughput: {:.0} ops/s ({:?} total)",
        send_ops, send_elapsed
    );

    Ok(())
}

/// Test error handling scenarios.
#[tokio::test]
#[ignore = "requires RobustMQ server"]
async fn error_handling_under_failure() -> Result<(), Box<dyn std::error::Error>> {
    use catga_robustmq::MailboxClient;

    // Test with invalid server (should fail gracefully)
    let result = MailboxClient::connect("nats://127.0.0.1:9999").await;
    match result {
        Ok(_) => panic!("Connection to invalid server should fail"),
        Err(error) => {
            assert_eq!(
                error.code(),
                ErrorCode::Transient,
                "Invalid server should return Transient error"
            );
            println!("Error handling test passed: {:?}", error);
        }
    }

    Ok(())
}

/// Test batch operations performance.
#[tokio::test]
#[ignore = "requires RobustMQ server"]
async fn batch_send_throughput() -> Result<(), Box<dyn std::error::Error>> {
    use catga_robustmq::{MailboxClient, MailboxConfig, MailboxPriority};

    let client = MailboxClient::connect("nats://127.0.0.1:4222").await?;

    // Create a test mailbox
    let config = MailboxConfig {
        server: "nats://127.0.0.1:4222".into(),
        ttl_seconds: 60,
        public: true,
        name: "batch_test".into(),
        description: "batch performance test".into(),
    };
    let mailbox = client.create(&config).await?;
    let mailbox_id = mailbox.mail_id.clone();

    // Test different batch sizes
    for batch_size in [1, 10, 100, 1000] {
        let mut envelopes = Vec::with_capacity(batch_size);
        for i in 0..batch_size {
            let metadata = MessageMetadata::new(i as u64, None);
            let env = Envelope::new(i as u64, "batch.message", vec![1, 2, 3], metadata);
            envelopes.push(env);
        }

        let start = Instant::now();
        for _ in 0..100 {
            for env in &envelopes {
                client
                    .send_envelope(&mailbox_id, env, MailboxPriority::Normal)
                    .await?;
            }
        }
        let elapsed = start.elapsed();
        let ops = (batch_size * 100) as f64 / elapsed.as_secs_f64();

        println!("Batch size {}: {:.0} total ops/s", batch_size, ops);
    }

    Ok(())
}

/// Test envelope serialization sizes.
#[tokio::test]
#[ignore = "requires RobustMQ server"]
async fn envelope_serialization_overhead() -> Result<(), Box<dyn std::error::Error>> {
    let codec = MemoryPackCodec::default();

    // Test various payload sizes
    for payload_size in [16, 64, 256, 1024, 4096] {
        let payload: Vec<u8> = (0..payload_size).map(|i| i as u8).collect();
        let metadata = MessageMetadata::new(1, None);
        let envelope = Envelope::new(1, "test.message", payload, metadata);

        let encoded = codec.encode(&envelope)?;
        let decoded: Envelope = codec.decode(&encoded)?;

        assert_eq!(envelope.id(), decoded.id());
        assert_eq!(envelope.message_type(), decoded.message_type());

        let ratio = encoded.len() as f64 / payload_size as f64;
        println!(
            "Payload {} bytes -> {} wire bytes (ratio: {:.2}x)",
            payload_size,
            encoded.len(),
            ratio
        );
    }

    Ok(())
}

/// Test priority queue operations.
#[tokio::test]
#[ignore = "requires RobustMQ server"]
async fn priority_queue_operations() -> Result<(), Box<dyn std::error::Error>> {
    use catga_robustmq::{MailboxClient, MailboxConfig, MailboxPriority};

    let client = MailboxClient::connect("nats://127.0.0.1:4222").await?;

    // Create mailboxes for different priorities
    let priorities = vec![
        MailboxPriority::Critical,
        MailboxPriority::High,
        MailboxPriority::Normal,
        MailboxPriority::Low,
    ];

    for priority in priorities {
        let config = MailboxConfig {
            server: "nats://127.0.0.1:4222".into(),
            ttl_seconds: 60,
            public: false,
            name: format!("priority_test_{:?}", priority).into(),
            description: format!("priority {:?} test", priority).into(),
        };

        let start = Instant::now();
        let _mailbox = client.create(&config).await?;

        // Send test message with priority
        let metadata = MessageMetadata::new(1, None);
        let envelope = Envelope::new(1, "priority.test", vec![1, 2, 3], metadata);
        client
            .send_envelope(&config.name, &envelope, priority)
            .await?;

        let elapsed = start.elapsed();
        println!("Priority {:?}: mailbox + send in {:?}", priority, elapsed);
    }

    Ok(())
}
