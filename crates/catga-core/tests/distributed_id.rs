use catga_core::{CatgaResult, DistributedIdGenerator, SnowflakeIdGenerator, SnowflakeLayout};

#[test]
fn fill_produces_unique_ids_for_the_configured_worker() -> CatgaResult<()> {
    let generator = SnowflakeIdGenerator::new(7, SnowflakeLayout::default())?;
    let mut ids = [0_u64; 8];

    generator.fill(&mut ids)?;

    assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(ids.iter().all(|id| generator.parse(*id).worker_id() == 7));
    Ok(())
}
