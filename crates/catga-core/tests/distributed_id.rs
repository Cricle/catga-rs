//! Distributed identifier uniqueness contract tests.

use catga_core::{DistributedIdGenerator, SnowflakeIdGenerator, SnowflakeLayout};

#[test]
fn fill_produces_unique_ids_for_the_configured_worker() {
    let generator = SnowflakeIdGenerator::new(7, SnowflakeLayout::default()).unwrap();
    let mut ids = [0_u64; 8];

    generator.fill(&mut ids).unwrap();

    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(ids.iter().all(|id| generator.parse(*id).worker_id() == 7));
}

#[test]
fn layouts_reject_invalid_bit_budgets_and_ranges() {
    assert!(SnowflakeLayout::new(44, 8, 10, 0).is_err());
    assert!(SnowflakeLayout::new(29, 20, 14, 0).is_err());
    assert!(SnowflakeLayout::new(50, 21, 0, 0).is_err());
    assert!(SnowflakeLayout::new(50, 0, 21, 0).is_err());

    let layout = SnowflakeLayout::new(43, 10, 10, 0).expect("valid layout");
    assert_eq!(layout.max_worker_id(), 1023);
    assert_eq!(layout.max_sequence(), 1023);
    assert!(SnowflakeIdGenerator::new(1024, layout).is_err());
}

#[test]
fn parser_extracts_worker_id_and_sequence_from_generated_id() {
    let layout = SnowflakeLayout::new(43, 10, 10, 0).expect("valid layout");
    let generator = SnowflakeIdGenerator::new(7, layout).expect("valid generator");

    let id = generator.next_id().expect("generate id");
    let metadata = generator.parse(id);
    assert_eq!(metadata.worker_id(), 7);
    assert!(metadata.sequence() <= 1023);
}

#[test]
fn future_epoch_cannot_generate_ids_before_its_clock_starts() {
    let layout = SnowflakeLayout::new(43, 10, 10, u64::MAX).expect("valid layout");
    let generator = SnowflakeIdGenerator::new(0, layout).expect("valid generator");
    // Try to generate an ID - this should fail because the clock is before epoch
    let result = generator.next_id();
    assert!(result.is_err());
}
