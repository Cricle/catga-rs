use catga_core::{ErrorCode, TaskSchedule};

#[test]
fn cron_schedule_preserves_the_adapter_owned_expression() {
    let schedule = TaskSchedule::cron("0 * * * * *").expect("a nonempty cron expression");

    assert_eq!(schedule.as_cron(), "0 * * * * *");
}

#[test]
fn cron_schedule_rejects_an_empty_expression() {
    let error = TaskSchedule::cron(" ").expect_err("an empty cron expression is invalid");

    assert_eq!(error.code(), ErrorCode::Validation);
}
