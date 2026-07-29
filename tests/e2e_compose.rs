//! Static contracts for the Docker E2E service topology.

const COMPOSE: &str = include_str!("../testing/docker/compose.yaml");
const E2E_SCENARIOS: &str = include_str!("../testing/e2e-scenarios.json");

#[test]
fn nats_jetstream_persists_storage_in_its_mounted_volume() {
    assert!(COMPOSE.contains("command: [\"-js\", \"-sd\", \"/data\", \"-m\", \"8222\"]"));
    assert!(COMPOSE.contains("- nats-data:/data"));
}

#[test]
fn cron_scheduler_scenario_uses_the_declared_integration_test_target() {
    let scenarios = serde_json::from_str::<serde_json::Value>(E2E_SCENARIOS)
        .expect("E2E scenario matrix must remain valid JSON");
    let cron_scheduler = scenarios["scenarios"]
        .as_array()
        .expect("E2E scenario matrix must contain scenarios")
        .iter()
        .find(|scenario| scenario["id"] == "scheduler-cron-lifecycle")
        .expect("E2E scenario matrix must include the cron scheduler scenario");
    assert_eq!(
        cron_scheduler["target"], "cron_runtime",
        "the cron scheduler E2E scenario must invoke the cron_runtime integration target"
    );
}
