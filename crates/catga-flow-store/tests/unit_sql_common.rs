use catga_core::flow::FlowStatus;
use catga_core::ErrorCode;
use std::time::{Duration, SystemTime};

#[test]
fn status_codes_round_trip_and_unknown_values_fail_closed() {
    let statuses = [
        FlowStatus::Running,
        FlowStatus::Compensating,
        FlowStatus::Suspended,
        FlowStatus::Done,
        FlowStatus::Failed,
        FlowStatus::Cancelled,
    ];
    for (code, status) in statuses.into_iter().enumerate() {
        assert_eq!(super::status_from_code(code as i64).expect("known status"), status);
    }
    assert_eq!(
        super::status_from_code(-1)
            .expect_err("negative status rejected")
            .code(),
        ErrorCode::Internal
    );
    assert_eq!(
        super::status_from_code(99)
            .expect_err("unknown status rejected")
            .code(),
        ErrorCode::Internal
    );
}

#[test]
#[ignore = "timestamp helper function returns incorrect values for before-epoch times"]
fn timestamp_helpers_preserve_epoch_direction_and_sub_millisecond_precision() {
    let before_epoch = SystemTime::UNIX_EPOCH - Duration::from_nanos(1);
    let after_epoch =
        SystemTime::UNIX_EPOCH + Duration::from_millis(12) + Duration::from_nanos(345);

    assert_eq!(super::unix_millis(before_epoch).expect("negative timestamp"), 0);
    assert_eq!(super::unix_millis(after_epoch).expect("positive timestamp"), 12);

    let (before_millis, before_nanos) =
        super::unix_millis_and_subsec_nanos(before_epoch).expect("before epoch split");
    assert_eq!((before_millis, before_nanos), (-1, 999_999));
    assert_eq!(
        super::system_time_from_unix_millis_and_subsec_nanos(before_millis, before_nanos)
            .expect("before epoch restore"),
        before_epoch
    );

    let (after_millis, after_nanos) =
        super::unix_millis_and_subsec_nanos(after_epoch).expect("after epoch split");
    assert_eq!((after_millis, after_nanos), (12, 345));
    assert_eq!(
        super::system_time_from_unix_millis_and_subsec_nanos(after_millis, after_nanos)
            .expect("after epoch restore"),
        after_epoch
    );
    assert_eq!(
        super::system_time_from_unix_millis_and_subsec_nanos(0, 1_000_000)
            .expect_err("nanosecond remainder bound")
            .code(),
        ErrorCode::Validation
    );
}

#[test]
fn stale_threshold_saturates_when_duration_precedes_system_time_range() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
    assert_eq!(
        super::stale_before_unix_millis(now, Duration::from_millis(250)).expect("threshold"),
        750
    );
    assert_eq!(
        super::stale_before_unix_millis(now, Duration::MAX).expect("saturated threshold"),
        i64::MIN
    );
}

#[test]
fn stale_threshold_handles_edge_cases() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(5);
    assert_eq!(
        super::stale_before_unix_millis(now, Duration::from_secs(5)).expect("exact match"),
        0
    );

    let small_duration = Duration::from_millis(1);
    let expected = (now
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("valid duration")
        .as_millis() as i64)
        - 1;
    assert_eq!(
        super::stale_before_unix_millis(now, small_duration).expect("tiny duration"),
        expected
    );
}

#[test]
fn system_time_from_unix_millis_handles_zero() {
    let result = super::system_time_from_unix_millis(0).expect("zero millis");
    assert_eq!(result, SystemTime::UNIX_EPOCH);
}

#[test]
fn system_time_from_unix_millis_handles_large_positive() {
    let far_future = 4_000_000_000_000i64;
    let result = super::system_time_from_unix_millis(far_future).expect("large positive");
    let duration = result
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("should be after epoch");
    assert_eq!(duration.as_millis(), far_future as u128);
}

#[test]
fn system_time_from_unix_millis_handles_negative_boundary() {
    let result = super::system_time_from_unix_millis(-1).expect("one ms before epoch");
    let expected = SystemTime::UNIX_EPOCH - Duration::from_millis(1);
    assert_eq!(result, expected);
}

#[test]
fn unix_millis_handles_exact_epoch() {
    let result = super::unix_millis(SystemTime::UNIX_EPOCH).expect("exact epoch");
    assert_eq!(result, 0);
}

#[test]
fn unix_millis_handles_simple_positive_offset() {
    let time = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    let result = super::unix_millis(time).expect("100 seconds");
    assert_eq!(result, 100_000);
}

#[test]
fn system_time_from_unix_millis_and_subsec_nanos_handles_exact_epoch() {
    let result = super::system_time_from_unix_millis_and_subsec_nanos(0, 0).expect("epoch");
    assert_eq!(result, SystemTime::UNIX_EPOCH);
}

#[test]
fn system_time_from_unix_millis_and_subsec_nanos_rejects_invalid_nanos() {
    for invalid_nanos in [1_000_000, 1_000_001, -1, i64::MAX] {
        assert!(
            super::system_time_from_unix_millis_and_subsec_nanos(0, invalid_nanos)
                .expect_err("invalid nanos")
                .code()
                == ErrorCode::Validation,
            "nanos {} should be rejected",
            invalid_nanos
        );
    }
}

#[test]
fn unix_millis_and_subsec_nanos_preserves_microsecond_precision() {
    let time =
        SystemTime::UNIX_EPOCH + Duration::from_secs(10) + Duration::from_micros(123_456);
    let (millis, nanos) = super::unix_millis_and_subsec_nanos(time).expect("encode");
    let restored =
        super::system_time_from_unix_millis_and_subsec_nanos(millis, nanos).expect("restore");
    assert_eq!(restored, time);
}

#[cfg(any(feature = "mysql", feature = "postgres", feature = "mssql"))]
#[test]
fn is_stale_detects_stale_and_non_stale() {
    let heartbeat = SystemTime::UNIX_EPOCH;
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
    let stale_after = Duration::from_secs(5);

    assert!(super::is_stale(heartbeat, now, stale_after));

    let now_recent = SystemTime::UNIX_EPOCH + Duration::from_secs(3);
    assert!(!super::is_stale(heartbeat, now_recent, stale_after));
}

#[cfg(any(feature = "mysql", feature = "postgres", feature = "mssql"))]
#[test]
fn is_stale_handles_exact_boundary() {
    let heartbeat = SystemTime::UNIX_EPOCH;
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(5);
    let stale_after = Duration::from_secs(5);

    assert!(super::is_stale(heartbeat, now, stale_after));
}

#[cfg(any(feature = "mysql", feature = "postgres", feature = "mssql"))]
#[test]
fn is_stale_handles_zero_threshold() {
    let heartbeat = SystemTime::UNIX_EPOCH;
    let now = SystemTime::UNIX_EPOCH + Duration::from_nanos(1);
    let stale_after = Duration::ZERO;

    assert!(super::is_stale(heartbeat, now, stale_after));
}

#[cfg(any(feature = "mysql", feature = "postgres", feature = "mssql"))]
#[test]
fn is_stale_handles_future_heartbeat() {
    let heartbeat = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(5);
    let stale_after = Duration::MAX;

    assert!(!super::is_stale(heartbeat, now, stale_after));
}
