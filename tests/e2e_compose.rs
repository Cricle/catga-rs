//! Static contracts for the Docker E2E service topology.

const COMPOSE: &str = include_str!("../testing/docker/compose.yaml");

#[test]
fn nats_jetstream_persists_storage_in_its_mounted_volume() {
    assert!(COMPOSE.contains("command: [\"-js\", \"-sd\", \"/data\", \"-m\", \"8222\"]"));
    assert!(COMPOSE.contains("- nats-data:/data"));
}
