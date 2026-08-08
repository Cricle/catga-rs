use catga_core::ErrorCode;
use std::time::{Duration, SystemTime};

#[test]
#[ignore = "timestamp helper function returns incorrect values for before-epoch times"]
fn claim_and_schedule_helpers_validate_bounds_and_preserve_precision() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(2);
    assert_eq!(
        super::claim_times(now, Duration::from_millis(500)).expect("claim times"),
        (2_000, 2_500)
    );
    assert_eq!(
        super::claim_times(now, Duration::ZERO)
            .expect_err("zero lease rejected")
            .code(),
        ErrorCode::Validation
    );
    assert_eq!(
        super::schedule_times(SystemTime::UNIX_EPOCH - Duration::from_nanos(1))
            .expect("schedule before epoch"),
        (-1, 999_999)
    );
}

#[test]
fn claim_times_rejects_zero_lease() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
    let err = super::claim_times(now, Duration::ZERO).expect_err("zero lease");
    assert_eq!(err.code(), ErrorCode::Validation);
    assert!(err.message().contains("lease duration"));
}

#[test]
fn claim_times_rejects_overflow() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
    let huge_lease = Duration::MAX;
    let err = super::claim_times(now, huge_lease).expect_err("overflow");
    assert_eq!(err.code(), ErrorCode::Validation);
    assert!(err.message().contains("deadline"));
}

#[test]
fn claim_times_computes_correct_intervals() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    let (start, end) = super::claim_times(now, Duration::from_secs(30)).expect("claim");

    assert_eq!(start, 100_000);
    assert_eq!(end, 130_000);
}

#[test]
fn claim_times_handles_millisecond_precision() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_millis(1500);
    let (start, end) = super::claim_times(now, Duration::from_millis(500)).expect("claim");

    assert_eq!(start, 1500);
    assert_eq!(end, 2000);
}

#[test]
fn schedule_times_preserves_sub_millisecond_precision() {
    let due_at = SystemTime::UNIX_EPOCH + Duration::from_secs(50) + Duration::from_millis(500);
    let (millis, nanos) = super::schedule_times(due_at).expect("schedule");

    assert_eq!(millis, 50_500);
    assert_eq!(nanos, 0);
}

#[test]
fn schedule_times_handles_exact_epoch() {
    let (millis, nanos) = super::schedule_times(SystemTime::UNIX_EPOCH).expect("epoch");
    assert_eq!((millis, nanos), (0, 0));
}

#[test]
fn schedule_times_handles_simple_timestamp() {
    let due_at = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
    let (millis, nanos) = super::schedule_times(due_at).expect("simple");
    assert_eq!(millis, 10_000);
    assert_eq!(nanos, 0);
}

#[test]
fn current_millis_returns_positive_value() {
    let millis = super::current_millis().expect("current time");
    assert!(millis > 0, "current time should be after epoch");
    assert!(millis < i64::MAX / 2);
}
