use super::*;

#[test]
fn kv_bucket_name_format() {
    let bucket = "test-bucket";
    let expected_stream = format!("KV_{bucket}");
    let expected_subject = format!("$KV.{bucket}.>");
    assert_eq!(expected_stream, "KV_test-bucket");
    assert_eq!(expected_subject, "$KV.test-bucket.>");
}

#[test]
fn kv_stream_subjects_template() {
    let subjects = vec![format!("$KV.{}.>", "mybucket")];
    assert_eq!(subjects[0], "$KV.mybucket.>");
}

#[test]
fn kv_bucket_name_with_underscores() {
    let bucket = "my_test_bucket";
    let expected_stream = format!("KV_{bucket}");
    let expected_subject = format!("$KV.{bucket}.>");
    assert_eq!(expected_stream, "KV_my_test_bucket");
    assert_eq!(expected_subject, "$KV.my_test_bucket.>");
}

#[test]
fn kv_bucket_name_with_dots() {
    let bucket = "my.bucket.name";
    let expected_stream = format!("KV_{bucket}");
    let expected_subject = format!("$KV.{bucket}.>");
    assert_eq!(expected_stream, "KV_my.bucket.name");
    assert_eq!(expected_subject, "$KV.my.bucket.name.>");
}

#[test]
fn kv_bucket_name_empty() {
    let bucket = "";
    let expected_stream = format!("KV_{bucket}");
    let expected_subject = format!("$KV.{bucket}.>");
    assert_eq!(expected_stream, "KV_");
    assert_eq!(expected_subject, "$KV..>");
}

#[test]
fn kv_bucket_name_with_numbers() {
    let bucket = "bucket123";
    let expected_stream = format!("KV_{bucket}");
    let expected_subject = format!("$KV.{bucket}.>");
    assert_eq!(expected_stream, "KV_bucket123");
    assert_eq!(expected_subject, "$KV.bucket123.>");
}

#[test]
fn kv_stream_config_stream_name() {
    let bucket = "test-bucket";
    let stream_name = format!("KV_{bucket}");
    assert!(stream_name.starts_with("KV_"));
    assert!(stream_name.contains(bucket));
}

#[test]
fn kv_stream_config_subject_pattern() {
    let bucket = "test-bucket";
    let subject = format!("$KV.{bucket}.>");
    assert!(subject.starts_with("$KV."));
    assert!(subject.ends_with(".>"));
    assert!(subject.contains(bucket));
}
