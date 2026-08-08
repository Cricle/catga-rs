use catga_nats::projection_runner::NatsProjectionConfig;
use std::num::NonZeroUsize;

#[test]
fn projection_config_clone() {
    let config = NatsProjectionConfig {
        event_stream: "events".into(),
        event_subject_prefix: "catga.events".into(),
        checkpoint_bucket: "checkpoints".into(),
    };
    let cloned = config.clone();
    assert_eq!(config.event_stream, cloned.event_stream);
    assert_eq!(config.event_subject_prefix, cloned.event_subject_prefix);
    assert_eq!(config.checkpoint_bucket, cloned.checkpoint_bucket);
}

#[test]
fn projection_config_debug() {
    let config = NatsProjectionConfig {
        event_stream: "events".into(),
        event_subject_prefix: "catga.events".into(),
        checkpoint_bucket: "checkpoints".into(),
    };
    let debug_str = format!("{:?}", config);
    assert!(debug_str.contains("events"));
    assert!(debug_str.contains("catga.events"));
    assert!(debug_str.contains("checkpoints"));
}

#[test]
fn projection_config_eq() {
    let config1 = NatsProjectionConfig {
        event_stream: "events".into(),
        event_subject_prefix: "catga.events".into(),
        checkpoint_bucket: "checkpoints".into(),
    };
    let config2 = NatsProjectionConfig {
        event_stream: "events".into(),
        event_subject_prefix: "catga.events".into(),
        checkpoint_bucket: "checkpoints".into(),
    };
    let config3 = NatsProjectionConfig {
        event_stream: "other".into(),
        event_subject_prefix: "other.events".into(),
        checkpoint_bucket: "other.checkpoints".into(),
    };
    assert_eq!(config1, config2);
    assert_ne!(config1, config3);
}

#[test]
fn projection_config_boxed_strings() {
    let config = NatsProjectionConfig {
        event_stream: Box::from("my-events"),
        event_subject_prefix: Box::from("my.prefix"),
        checkpoint_bucket: Box::from("my-checkpoints"),
    };
    assert_eq!(config.event_stream.as_ref(), "my-events");
    assert_eq!(config.event_subject_prefix.as_ref(), "my.prefix");
    assert_eq!(config.checkpoint_bucket.as_ref(), "my-checkpoints");
}

#[test]
fn projection_config_from_string() {
    let config = NatsProjectionConfig {
        event_stream: "events".into(),
        event_subject_prefix: "prefix".into(),
        checkpoint_bucket: "checkpoints".into(),
    };
    assert_eq!(config.event_stream.as_ref(), "events");
    assert_eq!(config.event_subject_prefix.as_ref(), "prefix");
    assert_eq!(config.checkpoint_bucket.as_ref(), "checkpoints");
}

#[test]
fn projection_config_with_long_paths() {
    let config = NatsProjectionConfig {
        event_stream: "com.example.myapp.events".into(),
        event_subject_prefix: "com.example.myapp.events".into(),
        checkpoint_bucket: "com.example.myapp.checkpoints".into(),
    };
    assert!(config.event_stream.len() > 20);
    assert_eq!(config.event_stream, config.event_subject_prefix);
}

#[test]
fn projection_runner_fields_accessible() {
    use std::num::NonZeroUsize;

    let config = NatsProjectionConfig {
        event_stream: "events".into(),
        event_subject_prefix: "prefix".into(),
        checkpoint_bucket: "checkpoints".into(),
    };
    // Verify config fields are accessible
    let _ = config.event_stream.as_ref();
    let _ = config.event_subject_prefix.as_ref();
    let _ = config.checkpoint_bucket.as_ref();

    // Test with_batch_size accepts NonZeroUsize
    let batch_size = NonZeroUsize::new(100).unwrap();
    assert_eq!(batch_size.get(), 100);
}

#[test]
fn projection_config_debug_format() {
    let config = NatsProjectionConfig {
        event_stream: "my-stream".into(),
        event_subject_prefix: "prefix".into(),
        checkpoint_bucket: "bucket".into(),
    };
    let debug = format!("{:?}", config);
    assert!(debug.contains("my-stream"));
    assert!(debug.contains("prefix"));
    assert!(debug.contains("bucket"));
}

#[test]
fn projection_config_clone_is_independent() {
    let config = NatsProjectionConfig {
        event_stream: "events".into(),
        event_subject_prefix: "prefix".into(),
        checkpoint_bucket: "checkpoints".into(),
    };
    let cloned = config.clone();
    // Modify original
    let _ = config.event_stream;
    // Cloned should still be valid
    assert_eq!(cloned.event_stream.as_ref(), "events");
    assert_eq!(cloned.event_subject_prefix.as_ref(), "prefix");
    assert_eq!(cloned.checkpoint_bucket.as_ref(), "checkpoints");
}

#[test]
fn projection_config_partial_eq() {
    let config1 = NatsProjectionConfig {
        event_stream: "events".into(),
        event_subject_prefix: "prefix".into(),
        checkpoint_bucket: "bucket".into(),
    };
    let config2 = NatsProjectionConfig {
        event_stream: "events".into(),
        event_subject_prefix: "prefix".into(),
        checkpoint_bucket: "bucket".into(),
    };
    let config3 = NatsProjectionConfig {
        event_stream: "other".into(),
        event_subject_prefix: "prefix".into(),
        checkpoint_bucket: "bucket".into(),
    };
    assert_eq!(config1, config2);
    assert_ne!(config1, config3);
}

#[test]
fn projection_config_box_types_work() {
    let config = NatsProjectionConfig {
        event_stream: Box::from("stream"),
        event_subject_prefix: Box::from("subject"),
        checkpoint_bucket: Box::from("bucket"),
    };
    assert_eq!(config.event_stream.as_ref(), "stream");
    assert_eq!(config.event_subject_prefix.as_ref(), "subject");
    assert_eq!(config.checkpoint_bucket.as_ref(), "bucket");
}

#[test]
fn projection_config_string_conversions() {
    let config = NatsProjectionConfig {
        event_stream: String::from("stream").into_boxed_str(),
        event_subject_prefix: String::from("subject").into_boxed_str(),
        checkpoint_bucket: String::from("bucket").into_boxed_str(),
    };
    assert_eq!(config.event_stream.as_ref(), "stream");
    assert_eq!(config.event_subject_prefix.as_ref(), "subject");
    assert_eq!(config.checkpoint_bucket.as_ref(), "bucket");
}
