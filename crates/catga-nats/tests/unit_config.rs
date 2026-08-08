use super::*;

#[test]
fn receive_and_consumer_options_keep_safe_defaults_and_validate_overrides() {
    let receive = NatsReceiveOptions::default();
    assert_eq!(
        receive.pull_batch_size().get(),
        DEFAULT_NATS_PULL_BATCH_SIZE
    );
    assert_eq!(
        receive
            .with_pull_batch_size(128)
            .expect("positive batch size")
            .pull_batch_size()
            .get(),
        128
    );
    assert_eq!(
        receive
            .with_pull_batch_size(0)
            .expect_err("zero batch size rejected")
            .code(),
        ErrorCode::Validation
    );

    let durable = NatsConsumerOptions::durable();
    assert_eq!(durable.mode(), NatsConsumerMode::Durable);
    assert_eq!(durable.inactive_threshold(), None);
    let ephemeral =
        NatsConsumerOptions::ephemeral().with_inactive_threshold(Duration::from_secs(30));
    assert_eq!(ephemeral.mode(), NatsConsumerMode::Ephemeral);
    assert_eq!(
        ephemeral.inactive_threshold(),
        Some(Duration::from_secs(30))
    );
}

#[test]
fn transport_options_combine_receive_and_lifecycle_configuration() {
    let receive = NatsReceiveOptions::default()
        .with_pull_batch_size(4)
        .expect("positive batch size");
    let consumer = NatsConsumerOptions::ephemeral();
    let options = NatsTransportOptions::default()
        .with_receive(receive)
        .with_consumer(consumer);
    assert_eq!(options.receive(), receive);
    assert_eq!(options.consumer(), consumer);
    assert_eq!(
        NatsTransportOptions::default(),
        NatsTransportOptions::default()
    );
}

#[test]
fn consumer_mode_default_is_durable() {
    let mode: NatsConsumerMode = Default::default();
    assert_eq!(mode, NatsConsumerMode::Durable);
}

#[test]
fn consumer_options_with_inactive_threshold() {
    let options =
        NatsConsumerOptions::durable().with_inactive_threshold(Duration::from_secs(300));
    assert_eq!(options.inactive_threshold(), Some(Duration::from_secs(300)));
    let options2 = options.with_inactive_threshold(Duration::from_secs(600));
    assert_eq!(
        options2.inactive_threshold(),
        Some(Duration::from_secs(600))
    );
}

#[test]
fn transport_options_default_values() {
    let options = NatsTransportOptions::default();
    assert_eq!(options.receive(), NatsReceiveOptions::default());
    assert_eq!(options.consumer(), NatsConsumerOptions::default());
}

#[test]
fn nats_config_equality_and_debug() {
    let config1 = NatsConfig {
        server: "nats://localhost:4222".into(),
        stream: "test".into(),
        subject: "events".into(),
        consumer: "worker".into(),
    };
    let config2 = NatsConfig {
        server: "nats://localhost:4222".into(),
        stream: "test".into(),
        subject: "events".into(),
        consumer: "worker".into(),
    };
    let config3 = NatsConfig {
        server: "nats://remote:4222".into(),
        stream: "test".into(),
        subject: "events".into(),
        consumer: "worker".into(),
    };
    assert_eq!(config1, config2);
    assert_ne!(config1, config3);

    let debug = format!("{:?}", config1);
    assert!(debug.contains("NatsConfig"));
    assert!(debug.contains("test"));
}

#[test]
fn nats_publisher_config_equality() {
    let config1 = NatsPublisherConfig {
        server: "nats://localhost".into(),
        stream: "orders".into(),
        subject: "order.created".into(),
    };
    let config2 = NatsPublisherConfig {
        server: "nats://localhost".into(),
        stream: "orders".into(),
        subject: "order.created".into(),
    };
    assert_eq!(config1, config2);
    assert_eq!(config1.server.as_ref(), "nats://localhost");
    assert_eq!(config1.stream.as_ref(), "orders");
    assert_eq!(config1.subject.as_ref(), "order.created");
}

#[test]
fn nats_pubsub_config_equality() {
    let config1 = NatsPubSubConfig {
        server: "nats://localhost".into(),
        subject: "chat.room1".into(),
    };
    let config2 = NatsPubSubConfig {
        server: "nats://localhost".into(),
        subject: "chat.room1".into(),
    };
    assert_eq!(config1, config2);
    assert_ne!(
        config1,
        NatsPubSubConfig {
            server: "nats://remote".into(),
            subject: "chat.room1".into(),
        }
    );
}

#[test]
fn nats_destination_config_equality() {
    let dest1 = NatsDestinationConfig {
        stream: "orders".into(),
        subject: "orders.processed".into(),
        consumer: "processor".into(),
    };
    let dest2 = NatsDestinationConfig {
        stream: "orders".into(),
        subject: "orders.processed".into(),
        consumer: "processor".into(),
    };
    assert_eq!(dest1, dest2);
    assert_eq!(dest1.stream.as_ref(), "orders");
    assert_eq!(dest1.subject.as_ref(), "orders.processed");
    assert_eq!(dest1.consumer.as_ref(), "processor");
}

#[test]
fn receive_options_with_batch_size_1() {
    let receive = NatsReceiveOptions::default()
        .with_pull_batch_size(1)
        .expect("batch size 1 should be valid");
    assert_eq!(receive.pull_batch_size().get(), 1);
}

#[test]
fn receive_options_with_large_batch_size() {
    let receive = NatsReceiveOptions::default()
        .with_pull_batch_size(10_000)
        .expect("large batch size should be valid");
    assert_eq!(receive.pull_batch_size().get(), 10_000);
}

#[test]
fn receive_options_rejects_size_overflow() {
    let result = NatsReceiveOptions::default().with_pull_batch_size(0);
    assert!(result.is_err());

    let result = NatsReceiveOptions::default().with_pull_batch_size(1);
    assert!(result.is_ok());
}

#[test]
fn consumer_options_clone_independence() {
    let options1 =
        NatsConsumerOptions::durable().with_inactive_threshold(Duration::from_secs(60));
    let options2 = options1;
    let options3 = options1.with_inactive_threshold(Duration::from_secs(120));
    assert_eq!(options1.inactive_threshold(), options2.inactive_threshold());
    assert_ne!(options1.inactive_threshold(), options3.inactive_threshold());
}

#[test]
fn transport_options_clone_independence() {
    let options1 = NatsTransportOptions::default();
    let options2 = options1;
    assert_eq!(options1, options2);
}

#[test]
fn nats_config_clone() {
    let config1 = NatsConfig {
        server: "nats://localhost".into(),
        stream: "test".into(),
        subject: "events".into(),
        consumer: "worker".into(),
    };
    let config2 = config1.clone();
    assert_eq!(config1, config2);
    assert_eq!(config1.server, config2.server);
}

#[test]
fn config_structs_impl_debug() {
    let configs: Vec<String> = vec![
        format!("{:?}", NatsReceiveOptions::default()),
        format!("{:?}", NatsConsumerOptions::default()),
        format!("{:?}", NatsTransportOptions::default()),
        format!(
            "{:?}",
            NatsConfig {
                server: "localhost".into(),
                stream: "test".into(),
                subject: "t".into(),
                consumer: "c".into(),
            }
        ),
    ];
    for debug_str in configs {
        assert!(!debug_str.is_empty());
    }
}
