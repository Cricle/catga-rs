//! Redis transport performance tests.
//!
//! These tests require an external Redis server and are ignored by default.
//! Run with: `cargo test -p catga-redis --test performance -- --ignored`

use std::time::{Duration, Instant};

use catga_core::{
    codec::memorypack::MemoryPackCodec,
    Envelope, EnvelopeCodec, ErrorCode, MessageMetadata, MessageTransport, QualityOfService,
    Stoppable,
};

/// Test envelope encoding/decoding throughput.
#[tokio::test]
#[ignore = "requires Redis server"]
async fn envelope_encoding_decoding_throughput() -> Result<(), Box<dyn std::error::Error>> {
    const COUNT: usize = 100_000;
    let codec = MemoryPackCodec::default();

    // Create a test envelope
    let metadata = MessageMetadata::new(1, None)
        .with_quality_of_service(QualityOfService::AtLeastOnce);
    let original =
        Envelope::new(1, "test.message", vec![1, 2, 3, 4, 5, 6, 7, 8], metadata);

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
#[ignore = "requires Redis server"]
async fn connection_establishment() -> Result<(), Box<dyn std::error::Error>> {
    use catga_redis::{RedisConfig, RedisTransport};

    let config = RedisConfig {
        server: "redis://127.0.0.1/".into(),
        stream: "perf_test".into(),
        group: "perf_group".into(),
        consumer: "perf_consumer".into(),
    };

    // Measure connection time
    let start = Instant::now();
    let transport = RedisTransport::connect(config).await?;
    let connect_elapsed = start.elapsed();

    println!("Redis connection establishment: {:?}", connect_elapsed);

    // Verify transport is usable
    assert!(transport.is_accepting());

    // Measure publish round-trip
    const COUNT: usize = 10_000;
    let metadata = MessageMetadata::new(1, None);
    let envelope = Envelope::new(1, "test", vec![1, 2, 3], metadata);

    let pub_start = Instant::now();
    for i in 0..COUNT {
        let mut e = envelope.clone();
        // Vary the message ID to avoid deduplication
        let meta = MessageMetadata::new(i as u64, None);
        e = e.with_metadata(meta);
        MessageTransport::publish(&transport, e).await?;
    }
    let pub_elapsed = pub_start.elapsed();
    let publish_ops = COUNT as f64 / pub_elapsed.as_secs_f64();

    println!(
        "Redis publish throughput: {:.0} ops/s ({:?} total)",
        publish_ops, pub_elapsed
    );

    // Measure receive round-trip
    let recv_start = Instant::now();
    let mut recv_count = 0;
    let deadline = Instant::now() + Duration::from_secs(10);
    while recv_count < COUNT && Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(100), MessageTransport::receive(&transport)).await {
            Ok(Ok(delivery)) => {
                recv_count += 1;
                delivery.acknowledge().await?;
            }
            Ok(Err(e)) if e.code() == ErrorCode::Unavailable => {
                continue;
            }
            Err(_) => break,
            _ => {}
        }
    }
    let recv_elapsed = recv_start.elapsed();
    let receive_ops = if recv_count > 0 {
        recv_count as f64 / recv_elapsed.as_secs_f64()
    } else {
        0.0
    };

    println!(
        "Redis receive throughput: {:.0} ops/s ({} received in {:?})",
        receive_ops, recv_count, recv_elapsed
    );

    Ok(())
}

/// Test error handling scenarios.
#[tokio::test]
#[ignore = "requires Redis server"]
async fn error_handling_under_failure() -> Result<(), Box<dyn std::error::Error>> {
    use catga_redis::{RedisConfig, RedisTransport};

    // Test with invalid server (should fail gracefully)
    let config = RedisConfig {
        server: "redis://127.0.0.1:9999".into(),
        stream: "test".into(),
        group: "test_group".into(),
        consumer: "test_consumer".into(),
    };

    let result = RedisTransport::connect(config).await;
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
#[ignore = "requires Redis server"]
async fn batch_publish_throughput() -> Result<(), Box<dyn std::error::Error>> {
    use catga_redis::{RedisConfig, RedisTransport};

    let config = RedisConfig {
        server: "redis://127.0.0.1/".into(),
        stream: "batch_test".into(),
        group: "batch_group".into(),
        consumer: "batch_consumer".into(),
    };

    let transport = RedisTransport::connect(config).await?;

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
                MessageTransport::publish(&transport, env.clone()).await?;
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
#[ignore = "requires Redis server"]
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
